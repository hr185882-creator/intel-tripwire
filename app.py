import os
import re
import json
import time
import logging
import threading
from datetime import datetime, timezone
from collections import defaultdict, deque

import requests
import feedparser
from flask import Flask, jsonify, request, render_template
from flask_cors import CORS

app = Flask(__name__, template_folder="templates")
CORS(app)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("intel")

# ── Config ─────────────────────────────────────────────────────────────
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
UA = {"User-Agent": "IntelTripwire/1.0"}

WHALE_THRESHOLD = float(os.getenv("WHALE_THRESHOLD", "5000"))
VELOCITY_WINDOW = int(os.getenv("VELOCITY_WINDOW", "3600"))
VELOCITY_MIN_MOVE = float(os.getenv("VELOCITY_MIN_MOVE", "0.08"))
BRIEF_MIN_SCORE = float(os.getenv("BRIEF_MIN_SCORE", "0.20"))

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# Seed feeds (you can expand)
CATEGORIES_SEED = {
    "politics": {
        "label": "Politics",
        "icon": "⚖",
        "color": "#cc0000",
        "feeds": [
            "https://apnews.com/rss",
            "https://feeds.bbci.co.uk/news/rss.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
        ],
    },
    "geopolitics": {
        "label": "Geopolitics",
        "icon": "◌",
        "color": "#17a589",
        "feeds": [
            "https://feeds.bbci.co.uk/news/world/rss.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
            "https://apnews.com/rss",
        ],
    },
    "crypto": {
        "label": "Crypto",
        "icon": "₿",
        "color": "#e67e22",
        "feeds": [
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://decrypt.co/feed",
        ],
    },
    "ai": {
        "label": "AI & Tech",
        "icon": "◈",
        "color": "#2980b9",
        "feeds": [
            "https://www.theverge.com/rss/index.xml",
            "https://feeds.arstechnica.com/arstechnica/index",
        ],
    },
    "finance": {
        "label": "Finance & Macro",
        "icon": "◈",
        "color": "#8e44ad",
        "feeds": [
            "https://finance.yahoo.com/news/rssindex",
            "https://www.marketwatch.com/rss/topstories",
        ],
    },
    "sports": {
        "label": "Sports",
        "icon": "◉",
        "color": "#27ae60",
        "feeds": [
            "https://www.espn.com/espn/rss/news",
            "https://feeds.bbci.co.uk/sport/rss.xml",
        ],
    },
}

FALLBACK_FEEDS = [
    "https://apnews.com/rss",
    "https://feeds.bbci.co.uk/news/rss.xml",
]

TAG_MAP = {
    "politics": "politics",
    "us-politics": "politics",
    "world": "geopolitics",
    "geopolitics": "geopolitics",
    "crypto": "crypto",
    "bitcoin": "crypto",
    "ai": "ai",
    "artificial-intelligence": "ai",
    "finance": "finance",
    "economics": "finance",
    "sports": "sports",
    "nfl": "sports",
    "nba": "sports",
}

STOPWORDS = {
    "will","the","a","an","be","is","are","was","were","has","have","had","do","does","did","to","of","in","on","at","by","for",
    "with","from","or","and","but","if","not","it","its","this","that","which","who","what","when","where","how","any","all",
    "more","than","up","out","over","under","into","there","their","they","he","she","we","you","my","our","your","his","her",
    "get","go","come","make","take","see","know","can","could","would","should","may","might","must","shall","new","also",
    "just","still","first","last","next","one","two","no","yes","so","as","some","other","such","about","end","said","says",
    "after","before","report","news","percent","million","billion",
}

state = {
    "categories": {},
    "feeds_by_category": {},
    "markets": defaultdict(list),
    "news": defaultdict(list),
    "signals": [],
    "movers": [],
    "price_snapshots": defaultdict(deque),
    "whale_trades": deque(maxlen=200),
    "seen_trade_ts": {},
    "briefs": {},
    "errors": deque(maxlen=200),
    "total_markets": 0,
    "total_news": 0,
    "last_market_scan": None,
    "last_news_scan": None,
    "last_velocity_scan": None,
    "last_whale_scan": None,
}

_lock = threading.Lock()
_threads_started = False


def extract_keywords(text: str) -> set:
    words = re.findall(r"[A-Za-z][A-Za-z'\-]{2,}", (text or "").lower())
    out = set()
    for w in words:
        w = w.strip("'-")
        if len(w) > 2 and w not in STOPWORDS:
            out.add(w)
    return out


def signal_score(market_kw: set, article_kw: set) -> float:
    if not market_kw or not article_kw:
        return 0.0
    inter = market_kw & article_kw
    if not inter:
        return 0.0
    union = market_kw | article_kw
    base = len(inter) / len(union)
    bonus = sum(0.025 for w in inter if len(w) > 6)
    return round(min(1.0, base + bonus), 4)


def gamma_get(path: str, params: dict | None = None):
    r = requests.get(f"{GAMMA_BASE}{path}", params=params or {}, headers=UA, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch_all_tags(limit=200):
    try:
        return gamma_get("/tags", {"limit": limit})
    except Exception as e:
        log.error(f"Tags fetch error: {e}")
        return []


def fetch_events_for_tag(tag_id: int, limit=25):
    try:
        return gamma_get("/events", {
            "tag_id": tag_id,
            "active": "true",
            "closed": "false",
            "limit": limit,
            "order": "volume",
            "ascending": "false",
        })
    except Exception as e:
        log.warning(f"Events fetch error (tag {tag_id}): {e}")
        return []


def parse_resolution_criteria(description: str) -> dict:
    if not description:
        return {"criteria_text": "", "trigger_keywords": [], "requires_official": False, "requires_date": False}

    desc = re.sub(r"<[^>]+>", "", description).strip()
    criteria_match = re.search(
        r"(?:resolv\w+|resolution\s*:?|criteria\s*:?|resolve\s+yes\s+if)[^\n]{10,}",
        desc,
        re.IGNORECASE,
    )
    criteria_text = (criteria_match.group(0).strip() if criteria_match else desc[:300])
    trigger_kw = extract_keywords(criteria_text)

    requires_official = bool(re.search(
        r"\b(official|announces?|confirms?|signs?|executive order|declares?|enacted|passed|certified|published|federal register)\b",
        criteria_text,
        re.IGNORECASE,
    ))
    requires_date = bool(re.search(r"\b(before|by|prior to|deadline|end of)\b", criteria_text, re.IGNORECASE))

    return {
        "criteria_text": criteria_text[:400],
        "trigger_keywords": sorted(trigger_kw),
        "requires_official": requires_official,
        "requires_date": requires_date,
    }


def resolution_weighted_score(market_kw: set, article_kw: set, resolution: dict) -> float:
    base = signal_score(market_kw, article_kw)
    if base == 0 or not resolution:
        return base

    trigger_kw = set(resolution.get("trigger_keywords", []))
    if not trigger_kw:
        return base

    trigger_hit = len(trigger_kw & article_kw) / max(len(trigger_kw), 1)
    bonus = trigger_hit * 0.15

    if resolution.get("requires_official"):
        official_words = {"announces","confirmed","signed","enacted","official","certified","declared"}
        if article_kw & official_words:
            bonus += 0.05

    return round(min(1.0, base + bonus), 4)


def normalize_event(event: dict, category: str) -> dict:
    markets = event.get("markets", [])
    yes_price = 0.0
    token_id = None

    if markets:
        m = markets[0]
        try:
            prices = json.loads(m.get("outcomePrices", "[0,1]"))
            yes_price = float(prices[0])
        except Exception:
            pass
        try:
            tokens = m.get("clobTokenIds", [])
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            if tokens:
                token_id = tokens[0]
        except Exception:
            pass

    title = event.get("title") or event.get("slug", "")
    description = event.get("description") or event.get("rules") or ""
    keywords = extract_keywords(title)
    resolution = parse_resolution_criteria(description)

    return {
        "id": event.get("id"),
        "slug": event.get("slug", ""),
        "title": title,
        "description": description[:600],
        "resolution": resolution,
        "yes_price": round(yes_price, 4),
        "no_price": round(1 - yes_price, 4),
        "volume": float(event.get("volume") or 0),
        "liquidity": float(event.get("liquidity") or 0),
        "token_id": token_id,
        "category": category,
        "end_date": event.get("endDate"),
        "tags": [t.get("label") for t in event.get("tags", [])],
        "url": f"https://polymarket.com/event/{event.get('slug', '')}",
        "keywords": sorted(keywords),
    }


def parse_feed(url: str, category: str, max_items=15) -> list:
    articles = []
    try:
        feed = feedparser.parse(url)
        source_name = feed.feed.get("title", url.split("/")[2])
        for entry in feed.entries[:max_items]:
            title = (entry.get("title", "") or "").strip()
            if not title:
                continue
            summary = entry.get("summary", "") or entry.get("description", "") or ""
            summary = re.sub(r"<[^>]+>", "", summary)[:400].strip()
            link = entry.get("link", "")

            published = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
                except Exception:
                    published = None

            articles.append({
                "title": title,
                "summary": summary,
                "url": link,
                "source": source_name,
                "category": category,
                "published": published or datetime.now(timezone.utc).isoformat(),
                "keywords": sorted(extract_keywords(title + " " + summary)),
                "age_mins": 0,
            })
    except Exception as e:
        log.warning(f"Feed error ({url}): {e}")
    return articles


def articles_with_age(articles: list) -> list:
    now = datetime.now(timezone.utc)
    res = []
    for a in articles:
        try:
            pub = datetime.fromisoformat(a["published"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            age = int((now - pub).total_seconds() / 60)
        except Exception:
            age = 9999
        res.append({**a, "age_mins": age})
    return res


def run_signal_matching() -> list:
    with _lock:
        all_markets = [m for ms in state["markets"].values() for m in ms]
        all_news = [a for ns in state["news"].values() for a in ns]

    signals = []
    seen = set()

    for market in all_markets:
        m_kw = set(market.get("keywords", []))
        resolution = market.get("resolution", {})

        for article in all_news:
            a_kw = set(article.get("keywords", []))
            score = resolution_weighted_score(m_kw, a_kw, resolution)
            if score < 0.07:
                continue

            key = (market["id"], article["url"][:120])
            if key in seen:
                continue
            seen.add(key)

            inter = sorted(set(market.get("keywords", [])) & set(article.get("keywords", [])))

            signals.append({
                "id": f"sig_{market['id']}_{abs(hash(article['url'])) % 99999:05d}",
                "score": score,
                "market_id": market["id"],
                "market_title": market["title"],
                "market_url": market["url"],
                "yes_price": market["yes_price"],
                "resolution_criteria": resolution.get("criteria_text", ""),
                "requires_official": resolution.get("requires_official", False),
                "article_title": article["title"],
                "article_url": article["url"],
                "article_source": article["source"],
                "article_published": article["published"],
                "article_age_mins": article.get("age_mins", 9999),
                "category": market["category"],
                "matched_at": datetime.now(timezone.utc).isoformat(),
                "matched_keywords": inter[:8],
            })

    signals.sort(key=lambda x: (x["score"], -x.get("article_age_mins", 9999)), reverse=True)
    return signals[:300]


def market_scanner_thread():
    while True:
        try:
            log.info("Scanning Polymarket tags + markets...")
            tags = fetch_all_tags(limit=200)

            categories = {}
            feeds_by_cat = {}
            markets_by_cat = defaultdict(list)
            total = 0

            for k, v in CATEGORIES_SEED.items():
                categories[k] = {
                    "label": v["label"],
                    "icon": v["icon"],
                    "color": v["color"],
                    "market_count": 0,
                    "tags": [],
                    "auto": False,
                }
                feeds_by_cat[k] = list(v.get("feeds", []))

            for tag in tags:
                slug = (tag.get("slug") or "").lower()
                label = tag.get("label", slug)
                tag_id = tag.get("id")

                category = TAG_MAP.get(slug, slug)

                if category not in categories:
                    categories[category] = {
                        "label": label,
                        "icon": "◌",
                        "color": "#888888",
                        "market_count": 0,
                        "tags": [],
                        "auto": True,
                    }
                    feeds_by_cat[category] = FALLBACK_FEEDS[:]

                categories[category]["tags"].append(label)

                events = fetch_events_for_tag(tag_id, limit=25)
                for event in events:
                    markets_by_cat[category].append(normalize_event(event, category))
                    total += 1

                time.sleep(0.25)

            for cat in list(markets_by_cat.keys()):
                seen_ids, deduped = set(), []
                for m in sorted(markets_by_cat[cat], key=lambda x: x["volume"], reverse=True):
                    if m["id"] not in seen_ids:
                        seen_ids.add(m["id"])
                        deduped.append(m)
                markets_by_cat[cat] = deduped
                categories[cat]["market_count"] = len(deduped)

            with _lock:
                state["categories"] = categories
                state["feeds_by_category"] = feeds_by_cat
                state["markets"] = markets_by_cat
                state["total_markets"] = total
                state["last_market_scan"] = datetime.now(timezone.utc).isoformat()

            signals = run_signal_matching()
            with _lock:
                state["signals"] = signals

            log.info(f"Market scan done: {total} markets / {len(categories)} categories | signals: {len(signals)}")

        except Exception as e:
            log.error(f"Market scanner: {e}")
            with _lock:
                state["errors"].append(f"market_scan|{e}")

        time.sleep(300)


def news_fetcher_thread():
    while True:
        try:
            log.info("Fetching news feeds...")
            with _lock:
                feeds_snapshot = dict(state["feeds_by_category"])  # prevents mutation issues

            news_by_cat = defaultdict(list)
            total = 0

            for category, feeds in feeds_snapshot.items():
                feeds = feeds or FALLBACK_FEEDS
                for feed_url in feeds:
                    articles = parse_feed(feed_url, category)
                    news_by_cat[category].extend(articles)
                    total += len(articles)
                    time.sleep(0.4)

                seen_urls, deduped = set(), []
                for a in news_by_cat[category]:
                    if a["url"] and a["url"] not in seen_urls:
                        seen_urls.add(a["url"])
                        deduped.append(a)

                deduped = articles_with_age(deduped)
                deduped.sort(key=lambda x: x["age_mins"])
                news_by_cat[category] = deduped[:60]

            with _lock:
                state["news"] = news_by_cat
                state["total_news"] = total
                state["last_news_scan"] = datetime.now(timezone.utc).isoformat()

            signals = run_signal_matching()
            with _lock:
                state["signals"] = signals

        except Exception as e:
            log.error(f"News fetcher: {e}")
            with _lock:
                state["errors"].append(f"news_fetch|{e}")

        time.sleep(600)


def price_velocity_thread():
    SNAP_INTERVAL = 300
    while True:
        try:
            with _lock:
                all_markets = [m for ms in state["markets"].values() for m in ms]

            now = time.time()
            movers = []

            for market in all_markets:
                token_id = market.get("token_id")
                price = market.get("yes_price", 0)
                if not token_id or not price:
                    continue

                with _lock:
                    snaps = state["price_snapshots"][token_id]
                    snaps.append((now, price))
                    while snaps and snaps[0][0] < now - VELOCITY_WINDOW * 2:
                        snaps.popleft()
                    snap_list = list(snaps)

                window_snaps = [(ts, p) for ts, p in snap_list if ts >= now - VELOCITY_WINDOW]
                if len(window_snaps) < 2:
                    continue

                old = window_snaps[0][1]
                new = window_snaps[-1][1]
                if old == 0:
                    continue

                change = (new - old) / old
                change_abs = abs(change)
                if change_abs >= VELOCITY_MIN_MOVE:
                    movers.append({
                        "market_id": market["id"],
                        "market_title": market["title"],
                        "market_url": market["url"],
                        "category": market["category"],
                        "yes_price": new,
                        "price_1h_ago": old,
                        "change_pct": round(change * 100, 2),
                        "change_abs": round(change_abs * 100, 2),
                        "direction": "UP" if change > 0 else "DOWN",
                        "volume": market.get("volume", 0),
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    })

            movers.sort(key=lambda x: x["change_abs"], reverse=True)
            with _lock:
                state["movers"] = movers[:50]
                state["last_velocity_scan"] = datetime.now(timezone.utc).isoformat()

        except Exception as e:
            log.error(f"Velocity tracker: {e}")
            with _lock:
                state["errors"].append(f"velocity|{e}")

        time.sleep(SNAP_INTERVAL)


def whale_watcher_thread():
    POLL_INTERVAL = 90
    while True:
        try:
            r = requests.get(f"{CLOB_BASE}/trades", params={"limit": 500}, headers=UA, timeout=20)
            if r.status_code != 200:
                time.sleep(POLL_INTERVAL)
                continue

            trades_data = r.json()
            trades = trades_data if isinstance(trades_data, list) else trades_data.get("data", [])

            with _lock:
                all_markets = [m for ms in state["markets"].values() for m in ms]
                seen_trade_ts = state["seen_trade_ts"]

            token_to_market = {m["token_id"]: m for m in all_markets if m.get("token_id")}
            now = time.time()

            cutoff = now - 24 * 3600
            if len(seen_trade_ts) > 150000:
                seen_trade_ts.clear()
            else:
                for tid, ts in list(seen_trade_ts.items()):
                    if ts < cutoff:
                        del seen_trade_ts[tid]

            new_whales = []

            for trade in trades:
                trade_id = trade.get("id") or trade.get("tradeId") or ""
                if not trade_id:
                    continue
                if trade_id in seen_trade_ts:
                    continue

                try:
                    size = float(trade.get("size", 0))
                    price = float(trade.get("price", 0))
                    usd = size * price
                except Exception:
                    continue

                seen_trade_ts[trade_id] = now
                if usd < WHALE_THRESHOLD:
                    continue

                token_id = trade.get("asset_id") or trade.get("tokenId") or ""
                market = token_to_market.get(token_id, {})

                whale = {
                    "trade_id": trade_id,
                    "token_id": token_id,
                    "market_id": market.get("id"),
                    "market_title": market.get("title", "Unknown market"),
                    "market_url": market.get("url", ""),
                    "category": market.get("category", ""),
                    "side": (trade.get("side", "") or "").upper(),
                    "price": round(price, 4),
                    "size": round(size, 2),
                    "usd_size": round(usd, 2),
                    "outcome": trade.get("outcome", ""),
                    "trader": ((trade.get("maker_address") or trade.get("makerAddress") or "")[:10] + "..."),
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    "tier": "MEGA" if usd >= 50000 else ("LARGE" if usd >= 20000 else "WHALE"),
                }
                new_whales.append(whale)

            if new_whales:
                new_whales.sort(key=lambda x: x["usd_size"], reverse=True)
                with _lock:
                    for w in new_whales:
                        state["whale_trades"].appendleft(w)
                    state["last_whale_scan"] = datetime.now(timezone.utc).isoformat()

        except Exception as e:
            log.error(f"Whale watcher: {e}")
            with _lock:
                state["errors"].append(f"whales|{e}")

        time.sleep(POLL_INTERVAL)


def start_background_threads():
    global _threads_started
    if _threads_started:
        return
    _threads_started = True

    threads = [
        threading.Thread(target=market_scanner_thread, daemon=True, name="market-scanner"),
        threading.Thread(target=news_fetcher_thread, daemon=True, name="news-fetcher"),
        threading.Thread(target=price_velocity_thread, daemon=True, name="velocity-tracker"),
        threading.Thread(target=whale_watcher_thread, daemon=True, name="whale-watcher"),
    ]

    for t in threads:
        t.start()
        log.info(f"Thread started: {t.name}")


@app.get("/")
def dashboard():
    return render_template("dashboard.html")


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()})


@app.get("/status")
def status():
    with _lock:
        return jsonify({
            "status": "live",
            "total_markets": state["total_markets"],
            "total_news": state["total_news"],
            "total_signals": len(state["signals"]),
            "total_movers": len(state["movers"]),
            "total_whales": len(state["whale_trades"]),
            "total_briefs": len(state["briefs"]),
            "categories_count": len(state["categories"]),
            "last_market_scan": state["last_market_scan"],
            "last_news_scan": state["last_news_scan"],
            "last_velocity_scan": state["last_velocity_scan"],
            "last_whale_scan": state["last_whale_scan"],
            "api_key_set": bool(ANTHROPIC_API_KEY),
            "errors": list(state["errors"])[-5:],
            "server_time": datetime.now(timezone.utc).isoformat(),
        })


@app.get("/categories")
def get_categories():
    with _lock:
        return jsonify({"categories": state["categories"]})


@app.get("/markets")
def get_markets():
    category = request.args.get("category", "")
    limit = int(request.args.get("limit", 30))
    min_vol = float(request.args.get("min_volume", 0))

    with _lock:
        if category:
            data = list(state["markets"].get(category, []))
        else:
            data = [m for ms in state["markets"].values() for m in ms]
            data.sort(key=lambda x: x["volume"], reverse=True)
        if min_vol:
            data = [m for m in data if m["volume"] >= min_vol]
        data = data[:limit]

    return jsonify({"markets": data, "count": len(data)})


@app.get("/news")
def get_news():
    category = request.args.get("category", "")
    limit = int(request.args.get("limit", 25))

    with _lock:
        if category:
            data = list(state["news"].get(category, []))[:limit]
        else:
            data = [a for ns in state["news"].values() for a in ns]
            data.sort(key=lambda x: x.get("age_mins", 9999))
            data = data[:limit]

    return jsonify({"news": data, "count": len(data)})


@app.get("/signals")
def get_signals():
    category = request.args.get("category", "")
    min_score = float(request.args.get("min_score", 0.0))
    limit = int(request.args.get("limit", 50))

    with _lock:
        data = list(state["signals"])

    if category:
        data = [s for s in data if s.get("category") == category]
    if min_score:
        data = [s for s in data if s.get("score", 0) >= min_score]

    return jsonify({"signals": data[:limit], "count": len(data)})


@app.get("/market/<market_id>/signals")
def market_signals(market_id: str):
    with _lock:
        matched = [s for s in state["signals"] if str(s.get("market_id")) == str(market_id)]
    matched.sort(key=lambda x: x["score"], reverse=True)
    return jsonify({"signals": matched[:15], "count": len(matched)})


@app.get("/movers")
def get_movers():
    direction = request.args.get("direction", "")
    limit = int(request.args.get("limit", 30))

    with _lock:
        data = list(state["movers"])
        last = state.get("last_velocity_scan")

    if direction:
        data = [m for m in data if m.get("direction") == direction.upper()]

    return jsonify({
        "movers": data[:limit],
        "count": len(data),
        "window": f"{VELOCITY_WINDOW // 60}m",
        "threshold": f"{VELOCITY_MIN_MOVE * 100:.0f}%",
        "last_scan": last,
    })


@app.get("/whales")
def get_whales():
    limit = int(request.args.get("limit", 50))
    category = request.args.get("category", "")
    tier = request.args.get("tier", "")

    with _lock:
        data = list(state["whale_trades"])
        last = state.get("last_whale_scan")

    if category:
        data = [w for w in data if w.get("category") == category]
    if tier:
        data = [w for w in data if w.get("tier") == tier.upper()]

    return jsonify({
        "whales": data[:limit],
        "count": len(data),
        "threshold": f"${WHALE_THRESHOLD:,.0f}",
        "last_scan": last,
    })


@app.get("/briefs")
def get_briefs():
    # Stub endpoint for UI; AI briefs are optional.
    with _lock:
        briefs = list(state["briefs"].values())
    return jsonify({
        "briefs": briefs[:20],
        "count": len(briefs),
        "threshold": BRIEF_MIN_SCORE,
        "api_ready": bool(ANTHROPIC_API_KEY),
    })


@app.get("/search")
def search():
    q = (request.args.get("q", "") or "").lower().strip()
    limit = int(request.args.get("limit", 20))
    if not q:
        return jsonify({"markets": [], "news": [], "query": q})

    terms = set(q.split())
    with _lock:
        all_markets = [m for ms in state["markets"].values() for m in ms]
        all_news = [a for ns in state["news"].values() for a in ns]

    mkt = [m for m in all_markets if any(t in (m.get("title", "").lower()) for t in terms)][:limit]
    nws = [a for a in all_news if any(t in (a.get("title", "").lower()) for t in terms)][:limit]

    return jsonify({"markets": mkt, "news": nws, "query": q})


if __name__ == "__main__":
    start_background_threads()
    port = int(os.environ.get("PORT", "5001"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

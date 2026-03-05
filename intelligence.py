"""
Intel Tripwire — Intelligence Server
─────────────────────────────────────────────────────────────────
Aggregates Polymarket market data across ALL categories and maps
live news feeds to open markets via keyword signal matching.

Three layers:
  1. Market Scanner   — polls Gamma API, fetches all tags + markets
  2. News Aggregator  — parses RSS feeds per category, normalizes
  3. Signal Mapper    — scores relevance between news and markets

Install:
    pip install flask flask-cors requests feedparser

Run:
    python intelligence_server.py

Then open: intelligence_dashboard.html in your browser
API base:  http://localhost:5001
─────────────────────────────────────────────────────────────────
"""

import re
import os
import json
import time
import logging
import threading
import requests
import feedparser
from datetime import datetime, timezone
from collections import defaultdict, deque
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__, static_folder=".", static_url_path="/static")
CORS(app)

@app.route("/")
def serve_dashboard():
    """Serve the dashboard HTML at root so same-origin API calls work."""
    import os
    dashboard = os.path.join(os.path.dirname(os.path.abspath(__file__)), "intelligence_dashboard.html")
    if os.path.exists(dashboard):
        with open(dashboard) as f:
            return f.read(), 200, {"Content-Type": "text/html"}
    return "<h1>Intel Tripwire running. Dashboard HTML not found.</h1>", 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("intel")

# ── Config ─────────────────────────────────────────────────────────────────────
CLOB_BASE          = "https://clob.polymarket.com"
CLOB_HEADERS       = {"User-Agent": "IntelTripwire/1.0"}
WHALE_THRESHOLD    = 5000    # USD — trades above this are flagged
VELOCITY_WINDOW    = 3600    # seconds — look-back for price change detection
VELOCITY_MIN_MOVE  = 0.08    # 8% move triggers a mover alert
BRIEF_MIN_SCORE    = 0.20    # signal score threshold to trigger auto-brief
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

# ── Alert Config ───────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")   # @BotFather token
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID",   "")   # your chat/channel ID
ALERT_WHALE_TIER    = {"MEGA", "LARGE"}   # tiers that trigger alerts
ALERT_MOVER_PCT     = 15.0                # % move threshold for mover alerts
ALERT_SIGNAL_SCORE  = 0.25               # score threshold for signal alerts
ALERT_COOLDOWN      = 1800               # seconds between repeat alerts for same market

# ── Portfolio / Trades Log ─────────────────────────────────────────────────────
TRADES_LOG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "trades.jsonl"
)

# ── Polymarket API ─────────────────────────────────────────────────────────────
GAMMA_BASE    = "https://gamma-api.polymarket.com"
GAMMA_HEADERS = {"User-Agent": "IntelTripwire/1.0"}

# ── Category Config ────────────────────────────────────────────────────────────
CATEGORIES = {
    "politics": {
        "label": "Politics", "icon": "⚖", "color": "#cc0000",
        "feeds": [
            "https://rss.politico.com/politics-news.xml",
            "https://thehill.com/feed/",
            "https://www.realclearpolitics.com/index.xml",
            "https://apnews.com/rss",
            "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
        ],
    },
    "elections": {
        "label": "Elections", "icon": "◉", "color": "#d63031",
        "feeds": [
            "https://rss.politico.com/politics-news.xml",
            "https://www.realclearpolitics.com/index.xml",
            "https://apnews.com/rss",
            "https://thehill.com/homenews/campaign/feed/",
        ],
    },
    "trump": {
        "label": "Trump", "icon": "★", "color": "#c0392b",
        "feeds": [
            "https://rss.politico.com/politics-news.xml",
            "https://thehill.com/feed/",
            "https://apnews.com/rss",
            "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
        ],
    },
    "crypto": {
        "label": "Crypto", "icon": "₿", "color": "#e67e22",
        "feeds": [
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://cointelegraph.com/rss",
            "https://decrypt.co/feed",
            "https://theblock.co/rss.xml",
            "https://bitcoinmagazine.com/.rss/full/",
        ],
    },
    "ai": {
        "label": "AI & Tech", "icon": "◈", "color": "#2980b9",
        "feeds": [
            "https://venturebeat.com/feed/",
            "https://www.theverge.com/rss/index.xml",
            "https://techcrunch.com/feed/",
            "https://www.wired.com/feed/rss",
            "https://feeds.arstechnica.com/arstechnica/index",
        ],
    },
    "spacex": {
        "label": "SpaceX & Space", "icon": "◎", "color": "#34495e",
        "feeds": [
            "https://www.nasaspaceflight.com/feed/",
            "https://spaceflightnow.com/feed/",
            "https://www.space.com/feeds/all",
        ],
    },
    "sports": {
        "label": "Sports", "icon": "◉", "color": "#27ae60",
        "feeds": [
            "https://www.espn.com/espn/rss/news",
            "https://feeds.bbci.co.uk/sport/rss.xml",
            "https://www.cbssports.com/rss/headlines/",
        ],
    },
    "finance": {
        "label": "Finance & Macro", "icon": "◈", "color": "#8e44ad",
        "feeds": [
            "https://www.marketwatch.com/rss/topstories",
            "https://finance.yahoo.com/news/rssindex",
            "https://feeds.reuters.com/reuters/businessNews",
        ],
    },
    "fed": {
        "label": "Fed & Rates", "icon": "◇", "color": "#6c3483",
        "feeds": [
            "https://www.marketwatch.com/rss/topstories",
            "https://feeds.reuters.com/reuters/businessNews",
            "https://finance.yahoo.com/news/rssindex",
        ],
    },
    "equities": {
        "label": "Equities & IPOs", "icon": "◫", "color": "#1a5276",
        "feeds": [
            "https://www.marketwatch.com/rss/topstories",
            "https://finance.yahoo.com/news/rssindex",
            "https://feeds.reuters.com/reuters/businessNews",
            "https://techcrunch.com/feed/",
        ],
    },
    "commodities": {
        "label": "Oil & Commodities", "icon": "◬", "color": "#784212",
        "feeds": [
            "https://feeds.reuters.com/reuters/businessNews",
            "https://oilprice.com/rss/main",
            "https://finance.yahoo.com/news/rssindex",
        ],
    },
    "tariffs": {
        "label": "Tariffs & Trade", "icon": "◰", "color": "#a04000",
        "feeds": [
            "https://apnews.com/rss",
            "https://feeds.reuters.com/reuters/businessNews",
            "https://feeds.washingtonpost.com/rss/business",
            "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        ],
    },
    "geopolitics": {
        "label": "Geopolitics", "icon": "◌", "color": "#17a589",
        "feeds": [
            "https://feeds.bbci.co.uk/news/world/rss.xml",
            "https://www.aljazeera.com/xml/rss/all.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
            "https://feeds.reuters.com/reuters/worldNews",
            "https://foreignpolicy.com/feed/",
        ],
    },
    "ukraine": {
        "label": "Ukraine & Russia", "icon": "◌", "color": "#1f618d",
        "feeds": [
            "https://feeds.bbci.co.uk/news/world/europe/rss.xml",
            "https://www.aljazeera.com/xml/rss/all.xml",
            "https://feeds.reuters.com/reuters/worldNews",
        ],
    },
    "middle_east": {
        "label": "Middle East", "icon": "◌", "color": "#1a5276",
        "feeds": [
            "https://www.aljazeera.com/xml/rss/all.xml",
            "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml",
            "https://feeds.reuters.com/reuters/worldNews",
        ],
    },
    "china": {
        "label": "China", "icon": "◌", "color": "#922b21",
        "feeds": [
            "https://feeds.bbci.co.uk/news/world/asia/rss.xml",
            "https://feeds.reuters.com/reuters/worldNews",
            "https://apnews.com/rss",
        ],
    },
    "science": {
        "label": "Science", "icon": "⬡", "color": "#d35400",
        "feeds": [
            "https://www.sciencedaily.com/rss/all.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",
            "https://feeds.arstechnica.com/arstechnica/science",
        ],
    },
    "climate": {
        "label": "Climate & Weather", "icon": "◍", "color": "#1e8449",
        "feeds": [
            "https://www.theguardian.com/environment/climate-crisis/rss",
            "https://insideclimatenews.org/feed/",
            "https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml",
        ],
    },
    "entertainment": {
        "label": "Entertainment", "icon": "◆", "color": "#884ea0",
        "feeds": [
            "https://deadline.com/feed/",
            "https://variety.com/feed/",
            "https://www.hollywoodreporter.com/feed/",
            "https://pitchfork.com/rss/news/",
        ],
    },
    "awards": {
        "label": "Awards & Oscars", "icon": "◆", "color": "#d4ac0d",
        "feeds": [
            "https://deadline.com/feed/",
            "https://variety.com/feed/",
            "https://www.hollywoodreporter.com/feed/",
        ],
    },
}

# ── Polymarket Tag → Category Mapping ─────────────────────────────────────────
TAG_MAP = {
    # Politics
    "politics": "politics", "us-politics": "politics",
    "congress": "politics", "president": "politics",
    "senate": "politics", "house": "politics", "supreme-court": "politics",
    "democrat": "politics", "republican": "politics", "government": "politics",
    "epstein": "politics", "gov-shutdown": "politics",
    # Trump
    "trump": "trump", "trump-tariffs": "trump", "trump-admin": "trump",
    "doge": "trump", "musk": "trump", "tweet-markets": "trump",
    # Elections
    "elections": "elections", "2024-elections": "elections",
    "2026-elections": "elections", "global-elections": "elections",
    "midterms": "elections", "primaries": "elections",
    "texas-senate": "elections", "nepal-election": "elections",
    # Crypto
    "crypto": "crypto", "bitcoin": "crypto", "ethereum": "crypto",
    "defi": "crypto", "nft": "crypto", "altcoins": "crypto", "web3": "crypto",
    "solana": "crypto", "blockchain": "crypto", "xrp": "crypto",
    "crypto-prices": "crypto", "stablecoins": "crypto", "btc": "crypto",
    # AI / Tech
    "ai": "ai", "artificial-intelligence": "ai", "tech": "ai",
    "technology": "ai", "openai": "ai", "llm": "ai", "cybersecurity": "ai",
    "chatgpt": "ai", "anthropic": "ai", "google": "ai", "microsoft": "ai",
    # SpaceX / Space
    "spacex": "spacex", "space": "spacex", "nasa": "spacex",
    "starship": "spacex", "rocket": "spacex",
    # Sports
    "sports": "sports", "nfl": "sports", "nba": "sports", "mlb": "sports",
    "nhl": "sports", "soccer": "sports", "tennis": "sports", "golf": "sports",
    "mma": "sports", "boxing": "sports", "ncaa": "sports", "esports": "sports",
    "formula-1": "sports", "cricket": "sports", "olympics": "sports",
    "epl": "sports", "ufc": "sports", "super-bowl": "sports",
    # Finance & Macro
    "economics": "finance", "finance": "finance", "macro": "finance",
    "recession": "finance", "gdp": "finance", "inflation": "finance",
    # Fed / Rates
    "fed": "fed", "interest-rates": "fed", "fomc": "fed",
    "federal-reserve": "fed", "rate-cuts": "fed",
    # Equities / IPOs
    "stocks": "equities", "equities": "equities", "ipos": "equities",
    "earnings": "equities", "derivatives": "equities", "s&p500": "equities",
    "nasdaq": "equities", "dow": "equities",
    # Commodities / Oil
    "oil": "commodities", "commodities": "commodities", "energy": "commodities",
    "gold": "commodities", "natural-gas": "commodities",
    # Tariffs / Trade
    "tariffs": "tariffs", "trade": "tariffs", "trade-war": "tariffs",
    "sanctions": "tariffs", "wto": "tariffs",
    # Geopolitics (general)
    "world": "geopolitics", "geopolitics": "geopolitics", "war": "geopolitics",
    "europe": "geopolitics", "nato": "geopolitics", "un": "geopolitics",
    "venezuela": "geopolitics", "mexico": "geopolitics",
    "mexico-cartel-war": "geopolitics", "reza-pahlavi": "geopolitics",
    # Ukraine / Russia
    "ukraine": "ukraine", "russia": "ukraine", "zelensky": "ukraine",
    "putin": "ukraine", "nato-ukraine": "ukraine",
    # Middle East
    "middle-east": "middle_east", "israel": "middle_east",
    "iran": "middle_east", "lebanon": "middle_east",
    "hamas": "middle_east", "hezbollah": "middle_east",
    "gaza": "middle_east", "palestine": "middle_east",
    # China
    "china": "china", "taiwan": "china", "xi-jinping": "china",
    "prc": "china", "hong-kong": "china",
    # Science / Health
    "science": "science", "health": "science", "medicine": "science",
    "physics": "science", "biology": "science", "pandemic": "science",
    # Climate / Weather
    "climate": "climate", "environment": "climate",
    "daily-temperature": "climate", "weather": "climate",
    "renewable-energy": "climate",
    # Entertainment
    "entertainment": "entertainment", "movies": "entertainment",
    "music": "entertainment", "tv": "entertainment",
    "celebrity": "entertainment",
    # Awards
    "awards": "awards", "oscars": "awards", "grammys": "awards",
    "emmys": "awards", "golden-globes": "awards",
}

# Stopwords for keyword extraction
STOPWORDS = {
    "will", "the", "a", "an", "be", "is", "are", "was", "were", "has", "have",
    "had", "do", "does", "did", "to", "of", "in", "on", "at", "by", "for",
    "with", "from", "or", "and", "but", "if", "not", "it", "its", "this",
    "that", "which", "who", "what", "when", "where", "how", "any", "all",
    "more", "than", "up", "out", "over", "under", "into", "there", "their",
    "they", "he", "she", "we", "you", "my", "our", "your", "his", "her",
    "get", "go", "come", "make", "take", "see", "know", "can", "could",
    "would", "should", "may", "might", "must", "shall", "new", "also",
    "just", "still", "first", "last", "next", "one", "two", "no", "yes",
    "so", "as", "some", "other", "such", "about", "end", "2024", "2025",
    "2026", "year", "day", "days", "week", "month", "time", "said", "says",
    "after", "before", "report", "news", "percent", "million", "billion",
}

# ── State ──────────────────────────────────────────────────────────────────────
state = {
    "categories":       {},
    "markets":          defaultdict(list),
    "news":             defaultdict(list),
    "signals":          [],
    "all_tags":         [],
    "last_market_scan": None,
    "last_news_scan":   None,
    "total_markets":    0,
    "total_news":       0,
    "errors":           [],
    # ── New: Layer 1 — price velocity
    "price_snapshots":  defaultdict(deque),  # token_id -> deque of (ts, price)
    "movers":           [],                  # markets with big moves in last hour
    "last_velocity_scan": None,
    # ── New: Layer 2 — whale detection
    "whale_trades":     deque(maxlen=200),   # most recent large trades
    "seen_trade_ids":   set(),
    "last_whale_scan":  None,
    # ── New: Layer 3 — auto-briefs
    "briefs":           {},                  # signal_id -> brief dict
    "brief_queue":      deque(maxlen=50),    # signal_ids pending brief generation
    # ── Alert engine
    "alert_cooldowns":  {},                  # market_id+type -> last_alert_ts
    "alerts_sent":      deque(maxlen=100),   # log of sent alerts
    # ── Portfolio overlay
    "positions":        [],                  # active positions from trades.jsonl
    "portfolio_pnl":    0.0,
    "last_portfolio_scan": None,
}
_lock = threading.Lock()


# ── Keyword Utilities ──────────────────────────────────────────────────────────
def extract_keywords(text: str) -> set:
    words = re.findall(r"[A-Za-z][A-Za-z''\-]{2,}", text.lower())
    return {w.strip("'-") for w in words
            if w.strip("'-") not in STOPWORDS and len(w.strip("'-")) > 2}


def signal_score(market_kw: set, article_kw: set) -> float:
    """
    Jaccard similarity with a bonus for longer (more specific) matching terms.
    Returns [0.0, 1.0].
    """
    if not market_kw or not article_kw:
        return 0.0
    intersection = market_kw & article_kw
    if not intersection:
        return 0.0
    union  = market_kw | article_kw
    base   = len(intersection) / len(union)
    bonus  = sum(0.025 for w in intersection if len(w) > 6)
    return round(min(1.0, base + bonus), 4)


# ── Gamma API ──────────────────────────────────────────────────────────────────
def gamma_get(path: str, params: dict = None) -> any:
    r = requests.get(
        f"{GAMMA_BASE}{path}",
        params=params or {},
        headers=GAMMA_HEADERS,
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def fetch_all_tags() -> list:
    try:
        return gamma_get("/tags", {"limit": 200})
    except Exception as e:
        log.error(f"Tags fetch error: {e}")
        return []


def fetch_events_for_tag(tag_id: int, limit: int = 25) -> list:
    try:
        return gamma_get("/events", {
            "tag_id":    tag_id,
            "active":    "true",
            "closed":    "false",
            "limit":     limit,
            "order":     "volume",
            "ascending": "false",
        })
    except Exception as e:
        log.warning(f"Events fetch error (tag {tag_id}): {e}")
        return []


def parse_resolution_criteria(description: str) -> dict:
    """
    Extracts resolution triggers from a market description.
    Returns: {criteria_text, trigger_keywords, requires_official, requires_date}
    """
    if not description:
        return {"criteria_text": "", "trigger_keywords": [], "requires_official": False, "requires_date": False}

    desc = re.sub(r"<[^>]+>", "", description).strip()

    # Extract the resolution paragraph — usually after "resolves" or "Resolution:"
    criteria_match = re.search(
        r"(?:resolv\w+|resolution\s*:?|criteria\s*:?|resolve\s+yes\s+if)[^\n]{10,}",
        desc, re.IGNORECASE
    )
    criteria_text = criteria_match.group(0).strip() if criteria_match else desc[:300]

    # Pull specific trigger keywords from resolution text — these get bonus weight
    trigger_kw = extract_keywords(criteria_text)

    # Flags for resolution type
    requires_official = bool(re.search(
        r"\b(official|announces?|confirms?|signs?|executive order|declares?|"
        r"enacted|passed|certified|published|federal register)\b",
        criteria_text, re.IGNORECASE
    ))
    requires_date = bool(re.search(
        r"\b(before|by|prior to|on or before|deadline|end of)\b",
        criteria_text, re.IGNORECASE
    ))

    return {
        "criteria_text":    criteria_text[:400],
        "trigger_keywords": list(trigger_kw),
        "requires_official": requires_official,
        "requires_date":    requires_date,
    }


def resolution_weighted_score(
    market_kw: set,
    article_kw: set,
    resolution: dict,
) -> float:
    """
    Jaccard base score + bonus if article keywords match resolution trigger words.
    A news article matching the *specific resolution condition* scores higher
    than one merely mentioning the same topic.
    """
    base  = signal_score(market_kw, article_kw)
    if base == 0 or not resolution:
        return base

    trigger_kw = set(resolution.get("trigger_keywords", []))
    if not trigger_kw:
        return base

    # How much of the resolution criteria does this article address?
    trigger_hit = len(trigger_kw & article_kw) / max(len(trigger_kw), 1)
    bonus       = trigger_hit * 0.15  # up to +15% for matching resolution language

    # Extra bonus if article contains "official" language and market requires it
    if resolution.get("requires_official"):
        official_words = {"announces", "confirmed", "signed", "enacted",
                          "official", "certified", "declared"}
        if article_kw & official_words:
            bonus += 0.05

    return round(min(1.0, base + bonus), 4)


def normalize_event(event: dict, category: str) -> dict:
    """Flatten a Gamma event into a clean market record."""
    markets   = event.get("markets", [])
    yes_price = 0.0
    token_id  = None

    if markets:
        m = markets[0]
        try:
            prices    = json.loads(m.get("outcomePrices", "[0,1]"))
            yes_price = float(prices[0])
        except Exception:
            pass
        try:
            tokens   = m.get("clobTokenIds", [])
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            if tokens:
                token_id = tokens[0]
        except Exception:
            pass

    title       = event.get("title") or event.get("slug", "")
    description = event.get("description") or event.get("rules", "") or ""
    keywords    = extract_keywords(title)
    resolution  = parse_resolution_criteria(description)

    return {
        "id":          event.get("id"),
        "slug":        event.get("slug", ""),
        "title":       title,
        "description": description[:600],
        "resolution":  resolution,
        "yes_price":   round(yes_price, 4),
        "no_price":    round(1 - yes_price, 4),
        "volume":      float(event.get("volume")    or 0),
        "liquidity":   float(event.get("liquidity") or 0),
        "token_id":    token_id,
        "category":    category,
        "end_date":    event.get("endDate"),
        "tags":        [t.get("label") for t in event.get("tags", [])],
        "url":         f"https://polymarket.com/event/{event.get('slug', '')}",
        "keywords":    list(keywords),
    }


# ── RSS Aggregator ─────────────────────────────────────────────────────────────
# RSS feeds block server-side bots by User-Agent.
# Fetch raw content via requests with browser headers first,
# then parse with feedparser from the string — bypasses most blocks.
RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

def parse_feed(url: str, category: str, max_items: int = 15) -> list:
    articles = []
    try:
        # Fetch with browser UA — most RSS blocks are UA-based
        resp = requests.get(url, headers=RSS_HEADERS, timeout=12, allow_redirects=True)
        if resp.status_code not in (200, 301, 302):
            log.warning(f"Feed HTTP {resp.status_code}: {url}")
            return []
        feed        = feedparser.parse(resp.content)
        source_name = feed.feed.get("title", url.split("/")[2])

        for entry in feed.entries[:max_items]:
            title   = entry.get("title", "").strip()
            summary = entry.get("summary", "") or entry.get("description", "")
            summary = re.sub(r"<[^>]+>", "", summary)[:400].strip()
            link    = entry.get("link", "")
            if not title:
                continue

            published = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published = datetime(
                        *entry.published_parsed[:6], tzinfo=timezone.utc
                    ).isoformat()
                except Exception:
                    pass

            articles.append({
                "title":     title,
                "summary":   summary,
                "url":       link,
                "source":    source_name,
                "category":  category,
                "published": published or datetime.now(timezone.utc).isoformat(),
                "keywords":  list(extract_keywords(title + " " + summary)),
                "age_mins":  0,
            })
    except requests.exceptions.Timeout:
        log.warning(f"Feed timeout: {url}")
    except Exception as e:
        log.warning(f"Feed error ({url}): {e}")
    return articles


def articles_with_age(articles: list) -> list:
    now = datetime.now(timezone.utc)
    result = []
    for a in articles:
        try:
            pub = datetime.fromisoformat(a["published"])
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            age = int((now - pub).total_seconds() / 60)
        except Exception:
            age = 9999
        result.append({**a, "age_mins": age})
    return result


# ── Signal Matching ────────────────────────────────────────────────────────────
def run_signal_matching() -> list:
    with _lock:
        all_markets = [m for ms in state["markets"].values() for m in ms]
        all_news    = [a for ns in state["news"].values()    for a in ns]

    signals  = []
    seen     = set()

    for market in all_markets:
        m_kw       = set(market.get("keywords", []))
        resolution = market.get("resolution", {})

        for article in all_news:
            a_kw  = set(article.get("keywords", []))
            # Use resolution-weighted score — matches resolution language score higher
            score = resolution_weighted_score(m_kw, a_kw, resolution)
            if score < 0.07:
                continue
            key = (market["id"], article["url"][:80])
            if key in seen:
                continue
            seen.add(key)
            signals.append({
                "id":                  f"sig_{market['id']}_{abs(hash(article['url'])) % 99999:05d}",
                "score":               score,
                "market_id":           market["id"],
                "market_title":        market["title"],
                "market_url":          market["url"],
                "yes_price":           market["yes_price"],
                "resolution_criteria": resolution.get("criteria_text", ""),
                "requires_official":   resolution.get("requires_official", False),
                "article_title":       article["title"],
                "article_url":         article["url"],
                "article_source":      article["source"],
                "article_published":   article["published"],
                "article_age_mins":    article.get("age_mins", 9999),
                "category":         market["category"],
                "matched_at":       datetime.now(timezone.utc).isoformat(),
                "matched_keywords": list(
                    set(market.get("keywords", [])) & set(article.get("keywords", []))
                )[:8],
            })

    signals.sort(key=lambda x: (x["score"], -x.get("article_age_mins", 9999)), reverse=True)
    return signals[:300]


# ── Background Threads ─────────────────────────────────────────────────────────
def market_scanner_thread():
    while True:
        try:
            log.info("Scanning Polymarket tags and markets...")
            tags         = fetch_all_tags()
            categories   = {}
            markets_by_cat = defaultdict(list)
            total = 0

            for tag in tags:
                slug     = (tag.get("slug") or "").lower()
                label    = tag.get("label", slug)
                tag_id   = tag.get("id")

                # Self-discovering: unknown tags become their own category
                # instead of silently dumping into a catch-all bucket.
                # Every Polymarket tag gets full coverage regardless of
                # whether it was in our static map at build time.
                category = TAG_MAP.get(slug, slug)  # Use slug itself as category key

                if category not in categories:
                    cfg = CATEGORIES.get(category, {})
                    # Auto-generate config for unmapped tags with fallback feeds
                    categories[category] = {
                        "label":        cfg.get("label", label),
                        "icon":         cfg.get("icon",  "◌"),
                        "color":        cfg.get("color", "#888888"),
                        "market_count": 0,
                        "tags":         [],
                        "auto":         category not in CATEGORIES,  # flag as auto-discovered
                    }
                    # If auto-generated, register fallback feeds so news still runs
                    if category not in CATEGORIES:
                        CATEGORIES[category] = {
                            "label": label,
                            "icon":  "◌",
                            "color": "#888888",
                            "feeds": [
                                "https://apnews.com/rss",
                                "https://feeds.reuters.com/reuters/topNews",
                                "https://feeds.bbci.co.uk/news/rss.xml",
                            ],
                        }
                        log.info(f"Auto-discovered new category: {label} ({slug})")

                categories[category]["tags"].append(label)

                events = fetch_events_for_tag(tag_id, limit=25)
                for event in events:
                    markets_by_cat[category].append(
                        normalize_event(event, category)
                    )
                    total += 1

                time.sleep(0.25)  # Rate limit courtesy

            # Sort each category by volume, deduplicate by id
            for cat in markets_by_cat:
                seen_ids = set()
                deduped  = []
                for m in sorted(markets_by_cat[cat], key=lambda x: x["volume"], reverse=True):
                    if m["id"] not in seen_ids:
                        seen_ids.add(m["id"])
                        deduped.append(m)
                markets_by_cat[cat] = deduped
                if cat in categories:
                    categories[cat]["market_count"] = len(deduped)

            with _lock:
                state["categories"]       = categories
                state["markets"]          = markets_by_cat
                state["all_tags"]         = [t.get("label") for t in tags]
                state["total_markets"]    = total
                state["last_market_scan"] = datetime.now(timezone.utc).isoformat()

            log.info(f"Market scan done: {total} markets / {len(categories)} categories")

            signals = run_signal_matching()
            with _lock:
                state["signals"] = signals
            log.info(f"Signals matched: {len(signals)}")
            queue_briefs_for_high_signals()

        except Exception as e:
            log.error(f"Market scanner: {e}")
            with _lock:
                state["errors"].append(f"market_scan|{e}")

        time.sleep(300)  # Re-scan every 5 minutes


def news_fetcher_thread():
    while True:
        try:
            log.info("Fetching news feeds...")
            news_by_cat = defaultdict(list)
            total       = 0

            for category, cfg in CATEGORIES.items():
                for feed_url in cfg.get("feeds", []):
                    articles = parse_feed(feed_url, category)
                    news_by_cat[category].extend(articles)
                    total += len(articles)
                    time.sleep(0.4)

                # Deduplicate, add age, sort by recency
                seen_urls = set()
                deduped   = []
                for a in news_by_cat[category]:
                    if a["url"] not in seen_urls:
                        seen_urls.add(a["url"])
                        deduped.append(a)

                deduped = articles_with_age(deduped)
                deduped.sort(key=lambda x: x["age_mins"])
                news_by_cat[category] = deduped[:60]

            with _lock:
                state["news"]          = news_by_cat
                state["total_news"]    = total
                state["last_news_scan"] = datetime.now(timezone.utc).isoformat()

            log.info(f"News done: {total} articles")

            signals = run_signal_matching()
            with _lock:
                state["signals"] = signals

        except Exception as e:
            log.error(f"News fetcher: {e}")

        time.sleep(600)  # Refetch every 10 minutes


# ── REST Endpoints ─────────────────────────────────────────────────────────────
@app.route("/debug/api")
def debug_api():
    """
    Tests the Anthropic API connection with a minimal call.
    Shows exactly what's failing without guessing.
    """
    key_set = bool(ANTHROPIC_API_KEY)
    if not key_set:
        return jsonify({
            "ok": False,
            "error": "ANTHROPIC_API_KEY not set in environment",
            "fix": "Add ANTHROPIC_API_KEY to your Render environment variables",
        })

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 20,
                "messages":   [{"role": "user", "content": "Reply with just the word: WORKING"}],
            },
            timeout=15,
        )
        if resp.status_code == 200:
            text = resp.json()["content"][0]["text"].strip()
            return jsonify({"ok": True, "response": text, "model": "claude-haiku-4-5-20251001"})
        return jsonify({
            "ok":     False,
            "status": resp.status_code,
            "error":  resp.text[:500],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/debug/feeds")
def debug_feeds():
    """
    Test all configured RSS feeds and report which ones are returning data.
    Useful for diagnosing feed blocking in production environments.
    Hit this endpoint once after deploy to see feed health.
    """
    results = []
    for category, cfg in CATEGORIES.items():
        for url in cfg.get("feeds", []):
            try:
                resp = requests.get(url, headers=RSS_HEADERS, timeout=10)
                feed = feedparser.parse(resp.content)
                count = len(feed.entries)
                results.append({
                    "url":      url,
                    "category": category,
                    "status":   resp.status_code,
                    "entries":  count,
                    "ok":       resp.status_code == 200 and count > 0,
                    "error":    None,
                })
            except Exception as e:
                results.append({
                    "url":      url,
                    "category": category,
                    "status":   0,
                    "entries":  0,
                    "ok":       False,
                    "error":    str(e),
                })

    ok_count   = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count
    return jsonify({
        "total":   len(results),
        "ok":      ok_count,
        "failing": fail_count,
        "feeds":   sorted(results, key=lambda x: x["ok"]),  # failing first
    })


@app.route("/tags/discovered")
def get_discovered_tags():
    """Shows which tags were auto-discovered vs. explicitly mapped."""
    with _lock:
        auto = {
            k: v for k, v in state["categories"].items()
            if v.get("auto")
        }
        mapped = {
            k: v for k, v in state["categories"].items()
            if not v.get("auto")
        }
    return jsonify({
        "auto_discovered": list(auto.keys()),
        "explicitly_mapped": list(mapped.keys()),
        "total_tags": len(state["all_tags"]),
        "coverage": f"{len(mapped)}/{len(auto)+len(mapped)} explicitly mapped",
    })


@app.route("/movers")
def get_movers():
    """Markets with significant price movement in the last hour."""
    direction = request.args.get("direction", "")  # UP | DOWN | ""
    limit     = int(request.args.get("limit", 30))
    with _lock:
        data = list(state["movers"])
    if direction:
        data = [m for m in data if m["direction"] == direction.upper()]
    return jsonify({
        "movers":    data[:limit],
        "count":     len(data),
        "window":    f"{VELOCITY_WINDOW // 60}m",
        "threshold": f"{VELOCITY_MIN_MOVE * 100:.0f}%",
        "last_scan": state.get("last_velocity_scan"),
    })


@app.route("/whales")
def get_whales():
    """Large orders recently placed on Polymarket."""
    limit    = int(request.args.get("limit", 50))
    category = request.args.get("category", "")
    tier     = request.args.get("tier", "")  # WHALE | LARGE | MEGA
    with _lock:
        data = list(state["whale_trades"])
    if category:
        data = [w for w in data if w.get("category") == category]
    if tier:
        data = [w for w in data if w.get("tier") == tier.upper()]
    return jsonify({
        "whales":    data[:limit],
        "count":     len(data),
        "threshold": f"${WHALE_THRESHOLD:,}",
        "last_scan": state.get("last_whale_scan"),
    })


@app.route("/briefs")
def get_briefs():
    """Auto-generated analyst briefs for high-score signals."""
    limit = int(request.args.get("limit", 20))
    with _lock:
        data = sorted(state["briefs"].values(),
                      key=lambda x: x.get("generated_at", ""),
                      reverse=True)
    return jsonify({
        "briefs":    data[:limit],
        "count":     len(data),
        "threshold": BRIEF_MIN_SCORE,
        "api_ready": bool(ANTHROPIC_API_KEY),
    })


@app.route("/brief/<signal_id>")
def get_brief(signal_id: str):
    with _lock:
        brief = state["briefs"].get(signal_id)
    if not brief:
        return jsonify({"error": "Brief not found or not yet generated"}), 404
    return jsonify(brief)


@app.route("/portfolio")
def get_portfolio():
    with _lock:
        positions = list(state["positions"])
        pnl       = state["portfolio_pnl"]
        last_scan = state["last_portfolio_scan"]
    return jsonify({
        "positions":   positions,
        "open_count":  len(positions),
        "total_pnl":   pnl,
        "log_file":    TRADES_LOG_FILE,
        "log_exists":  os.path.exists(TRADES_LOG_FILE),
        "last_scan":   last_scan,
    })


@app.route("/alerts")
def get_alerts():
    limit = int(request.args.get("limit", 50))
    with _lock:
        data = list(state["alerts_sent"])
    return jsonify({
        "alerts":      data[:limit],
        "count":       len(data),
        "telegram_on": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
    })


@app.route("/status")
def status():
    with _lock:
        return jsonify({
            "status":             "live",
            "total_markets":      state["total_markets"],
            "total_news":         state["total_news"],
            "total_signals":      len(state["signals"]),
            "total_movers":       len(state["movers"]),
            "total_whales":       len(state["whale_trades"]),
            "total_briefs":       len(state["briefs"]),
            "total_positions":    len(state["positions"]),
            "portfolio_pnl":      state["portfolio_pnl"],
            "alerts_sent":        len(state["alerts_sent"]),
            "categories_count":   len(state["categories"]),
            "last_market_scan":   state["last_market_scan"],
            "last_news_scan":     state["last_news_scan"],
            "last_velocity_scan": state["last_velocity_scan"],
            "last_whale_scan":    state["last_whale_scan"],
            "last_portfolio_scan": state["last_portfolio_scan"],
            "api_key_set":        bool(ANTHROPIC_API_KEY),
            "telegram_on":        bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
            "errors":             state["errors"][-5:],
            "server_time":        datetime.now(timezone.utc).isoformat(),
        })


@app.route("/categories")
def get_categories():
    with _lock:
        return jsonify({"categories": state["categories"]})


@app.route("/markets")
def get_markets():
    category  = request.args.get("category", "")
    limit     = int(request.args.get("limit", 30))
    min_vol   = float(request.args.get("min_volume", 0))
    with _lock:
        if category:
            data = state["markets"].get(category, [])
        else:
            data = [m for ms in state["markets"].values() for m in ms]
            data.sort(key=lambda x: x["volume"], reverse=True)
        if min_vol:
            data = [m for m in data if m["volume"] >= min_vol]
        data = data[:limit]
    return jsonify({"markets": data, "count": len(data)})


@app.route("/news")
def get_news():
    category = request.args.get("category", "")
    limit    = int(request.args.get("limit", 25))
    with _lock:
        if category:
            data = state["news"].get(category, [])[:limit]
        else:
            data = [a for ns in state["news"].values() for a in ns]
            data.sort(key=lambda x: x.get("age_mins", 9999))
            data = data[:limit]
    return jsonify({"news": data, "count": len(data)})


@app.route("/signals")
def get_signals():
    category  = request.args.get("category", "")
    min_score = float(request.args.get("min_score", 0.0))
    limit     = int(request.args.get("limit", 50))
    with _lock:
        data = list(state["signals"])
    if category:
        data = [s for s in data if s.get("category") == category]
    if min_score:
        data = [s for s in data if s["score"] >= min_score]
    return jsonify({"signals": data[:limit], "count": len(data)})


@app.route("/market/<market_id>/signals")
def market_signals(market_id: str):
    with _lock:
        matched = [s for s in state["signals"]
                   if str(s.get("market_id")) == str(market_id)]
    matched.sort(key=lambda x: x["score"], reverse=True)
    return jsonify({"signals": matched[:15], "count": len(matched)})


@app.route("/search")
def search():
    """Search markets and news by keyword."""
    q      = request.args.get("q", "").lower().strip()
    limit  = int(request.args.get("limit", 20))
    if not q:
        return jsonify({"markets": [], "news": [], "query": q})

    terms = set(q.split())
    with _lock:
        all_markets = [m for ms in state["markets"].values() for m in ms]
        all_news    = [a for ns in state["news"].values()    for a in ns]

    mkt_results = [
        m for m in all_markets
        if any(t in m["title"].lower() for t in terms)
    ][:limit]

    news_results = [
        a for a in all_news
        if any(t in a["title"].lower() for t in terms)
    ][:limit]

    return jsonify({
        "markets": mkt_results,
        "news":    news_results,
        "query":   q,
    })



# ══════════════════════════════════════════════════════════════════
# LAYER 1 — PRICE VELOCITY TRACKER
# Snapshots every market's YES price every 5 minutes.
# Surfaces any market that moved >8% in the last hour.
# No price movement after a major news signal = mispricing candidate.
# ══════════════════════════════════════════════════════════════════

def price_velocity_thread():
    SNAP_INTERVAL = 300  # 5 minutes between snapshots

    while True:
        try:
            with _lock:
                all_markets = [m for ms in state["markets"].values() for m in ms]

            now     = time.time()
            movers  = []
            updates = {}

            for market in all_markets:
                token_id = market.get("token_id")
                if not token_id:
                    continue
                price = market.get("yes_price", 0)
                if not price:
                    continue

                with _lock:
                    snaps = state["price_snapshots"][token_id]
                    snaps.append((now, price))
                    # Prune snapshots older than 2x window
                    while snaps and snaps[0][0] < now - VELOCITY_WINDOW * 2:
                        snaps.popleft()
                    snap_list = list(snaps)

                # Find oldest price within the look-back window
                window_snaps = [(ts, p) for ts, p in snap_list
                                if ts >= now - VELOCITY_WINDOW]
                if len(window_snaps) < 2:
                    continue

                oldest_price = window_snaps[0][1]
                newest_price = window_snaps[-1][1]
                if oldest_price == 0:
                    continue

                change     = (newest_price - oldest_price) / oldest_price
                change_abs = abs(change)

                if change_abs >= VELOCITY_MIN_MOVE:
                    movers.append({
                        "market_id":    market["id"],
                        "market_title": market["title"],
                        "market_url":   market["url"],
                        "category":     market["category"],
                        "yes_price":    newest_price,
                        "price_1h_ago": oldest_price,
                        "change_pct":   round(change * 100, 2),
                        "change_abs":   round(change_abs * 100, 2),
                        "direction":    "UP" if change > 0 else "DOWN",
                        "volume":       market.get("volume", 0),
                        "detected_at":  datetime.now(timezone.utc).isoformat(),
                    })

            movers.sort(key=lambda x: x["change_abs"], reverse=True)

            with _lock:
                state["movers"]              = movers[:50]
                state["last_velocity_scan"]  = datetime.now(timezone.utc).isoformat()

            if movers:
                log.info(f"Velocity: {len(movers)} movers | top: "
                         f"{movers[0]['market_title'][:40]} "
                         f"{movers[0]['change_pct']:+.1f}%")

        except Exception as e:
            log.error(f"Velocity tracker: {e}")

        time.sleep(SNAP_INTERVAL)


# ══════════════════════════════════════════════════════════════════
# LAYER 2 — WHALE ORDER DETECTOR
# Polls the CLOB /trades endpoint every 90 seconds.
# Flags any trade above WHALE_THRESHOLD USD.
# Large orders often precede news — this is the leading signal.
# ══════════════════════════════════════════════════════════════════

def whale_watcher_thread():
    POLL_INTERVAL = 90  # seconds

    while True:
        try:
            r = requests.get(
                f"{CLOB_BASE}/trades",
                params={"limit": 500},
                headers={**CLOB_HEADERS, "User-Agent": RSS_HEADERS["User-Agent"]},
                timeout=15,
            )
            if r.status_code != 200:
                log.warning(f"CLOB trades HTTP {r.status_code}")
                time.sleep(POLL_INTERVAL)
                continue

            raw         = r.json()
            # CLOB returns {data:[...]} or [...] or {trades:[...]}
            if isinstance(raw, list):
                trades = raw
            elif isinstance(raw, dict):
                trades = raw.get("data") or raw.get("trades") or raw.get("results", [])
            else:
                trades = []

            if not trades:
                log.info(f"CLOB: no trades in response. Keys: {list(raw.keys()) if isinstance(raw, dict) else type(raw)}")
                time.sleep(POLL_INTERVAL)
                continue

            # Build reverse lookup: token_id -> market info
            with _lock:
                all_markets   = [m for ms in state["markets"].values() for m in ms]
                seen_ids      = state["seen_trade_ids"]

            token_to_market = {m["token_id"]: m for m in all_markets if m.get("token_id")}
            new_whales      = []

            for trade in trades:
                trade_id = trade.get("id") or trade.get("tradeId", "")
                if not trade_id or trade_id in seen_ids:
                    continue

                # Compute USD size
                try:
                    size  = float(trade.get("size",  0))
                    price = float(trade.get("price", 0))
                    usd   = size * price
                except (TypeError, ValueError):
                    continue

                if usd < WHALE_THRESHOLD:
                    continue

                token_id = trade.get("asset_id") or trade.get("tokenId", "")
                market   = token_to_market.get(token_id, {})

                whale = {
                    "trade_id":     trade_id,
                    "token_id":     token_id,
                    "market_id":    market.get("id"),
                    "market_title": market.get("title", "Unknown market"),
                    "market_url":   market.get("url", ""),
                    "category":     market.get("category", ""),
                    "side":         trade.get("side", "").upper(),
                    "price":        round(price, 4),
                    "size":         round(size, 2),
                    "usd_size":     round(usd, 2),
                    "outcome":      trade.get("outcome", ""),
                    "trader":       (trade.get("maker_address") or
                                    trade.get("makerAddress", ""))[:10] + "...",
                    "detected_at":  datetime.now(timezone.utc).isoformat(),
                    "tier": (
                        "MEGA"  if usd >= 50000 else
                        "LARGE" if usd >= 20000 else
                        "WHALE"
                    ),
                }
                new_whales.append(whale)
                seen_ids.add(trade_id)

            if new_whales:
                new_whales.sort(key=lambda x: x["usd_size"], reverse=True)
                with _lock:
                    for w in new_whales:
                        state["whale_trades"].appendleft(w)
                    state["last_whale_scan"] = datetime.now(timezone.utc).isoformat()
                log.info(f"Whale: {len(new_whales)} new orders | largest: "
                         f"${new_whales[0]['usd_size']:,.0f} on "
                         f"{new_whales[0]['market_title'][:35]}")

        except Exception as e:
            log.error(f"Whale watcher: {e}")

        time.sleep(POLL_INTERVAL)


# ══════════════════════════════════════════════════════════════════
# LAYER 3 — AUTO-BRIEF GENERATOR
# When a signal scores >= BRIEF_MIN_SCORE, sends the market title,
# resolution context, and article summary to Claude (claude-haiku-4-5
# for speed/cost). Returns a 2-paragraph analyst brief.
# Requires ANTHROPIC_API_KEY in environment.
# ══════════════════════════════════════════════════════════════════

def generate_brief(signal: dict) -> str | None:
    if not ANTHROPIC_API_KEY:
        return None

    resolution = signal.get("resolution_criteria", "")
    prompt = f"""You are a prediction market analyst. Be extremely concise.

MARKET: {signal['market_title']}
CURRENT YES PRICE: {signal['yes_price'] * 100:.0f}¢
{f'RESOLUTION CRITERIA: {resolution}' if resolution else ''}
NEWS HEADLINE: {signal['article_title']}
SOURCE: {signal['article_source']}
MATCHED KEYWORDS: {', '.join(signal.get('matched_keywords', []))}

Write exactly 2 short paragraphs:
1. How does this news directly affect the probability of this market resolving YES? Be specific about causality.
2. What would need to happen next for YES to move significantly? Name the specific trigger.

No fluff. No caveats. Just direct analysis."""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 350,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=25,
        )
        if resp.status_code == 200:
            data = resp.json()
            # Defensive: handle both text blocks and tool_use blocks
            for block in data.get("content", []):
                if block.get("type") == "text":
                    return block["text"].strip()
            log.warning(f"Brief: unexpected content structure: {data.get('content')}")
            return None
        # Log full error body so we know exactly what's wrong
        log.warning(f"Brief API {resp.status_code}: {resp.text[:300]}")
        return None
    except Exception as e:
        log.warning(f"Brief generation failed: {e}")
        return None


def brief_engine_thread():
    """
    Consumes signals from brief_queue, generates briefs via Claude,
    stores results in state['briefs']. Runs serially to avoid API hammering.
    One brief every 3 seconds max.
    """
    while True:
        try:
            signal = None
            with _lock:
                if state["brief_queue"]:
                    signal = state["brief_queue"].pop()

            if signal:
                sig_id = signal.get("id")
                with _lock:
                    already = sig_id in state["briefs"]

                if not already:
                    log.info(f"Generating brief for: {signal['market_title'][:40]}")
                    brief_text = generate_brief(signal)
                    if brief_text:
                        with _lock:
                            state["briefs"][sig_id] = {
                                "signal_id":    sig_id,
                                "market_title": signal["market_title"],
                                "brief":        brief_text,
                                "generated_at": datetime.now(timezone.utc).isoformat(),
                                "score":        signal.get("score"),
                            }
                        log.info(f"Brief ready: {sig_id}")

            time.sleep(3)

        except Exception as e:
            log.error(f"Brief engine: {e}")
            time.sleep(10)


def queue_briefs_for_high_signals():
    """
    Called after every signal refresh. Queues new high-score signals
    for brief generation if they don't already have one.
    """
    with _lock:
        sigs       = list(state["signals"])
        have_brief = set(state["briefs"].keys())
        queued     = {s.get("id") for s in state["brief_queue"]}

    for sig in sigs:
        if (sig.get("score", 0) >= BRIEF_MIN_SCORE
                and sig.get("id") not in have_brief
                and sig.get("id") not in queued):
            with _lock:
                state["brief_queue"].appendleft(sig)



# ══════════════════════════════════════════════════════════════════
# ALERT ENGINE
# Sends Telegram notifications for: MEGA/LARGE whale orders,
# movers above ALERT_MOVER_PCT, signals above ALERT_SIGNAL_SCORE.
# Falls back to console log if Telegram not configured.
# ══════════════════════════════════════════════════════════════════

def _cooldown_key(market_id, alert_type: str) -> str:
    return f"{market_id}:{alert_type}"


def _check_cooldown(market_id, alert_type: str) -> bool:
    """Returns True if alert is allowed (not in cooldown)."""
    key = _cooldown_key(market_id, alert_type)
    with _lock:
        last = state["alert_cooldowns"].get(key, 0)
    return (time.time() - last) > ALERT_COOLDOWN


def _set_cooldown(market_id, alert_type: str):
    key = _cooldown_key(market_id, alert_type)
    with _lock:
        state["alert_cooldowns"][key] = time.time()


def send_telegram(message: str) -> bool:
    """Send a message via Telegram Bot API. Returns True on success."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")
        return False


def fire_alert(alert_type: str, title: str, body: str, url: str = "",
               market_id: str = "", priority: str = "normal"):
    """
    Unified alert dispatcher.
    Logs to console always. Sends Telegram if configured.
    Records to state['alerts_sent'].
    """
    emoji = {
        "whale":  "🐋",
        "mover":  "📈",
        "signal": "⚡",
        "brief":  "🧠",
    }.get(alert_type, "🔔")

    console_msg = f"[ALERT:{alert_type.upper()}] {title} — {body}"
    log.warning(console_msg)

    tg_msg = (
        f"{emoji} <b>{title}</b>\n"
        f"{body}"
        + (f"\n<a href='{url}'>View Market →</a>" if url else "")
    )
    sent_tg = send_telegram(tg_msg)

    record = {
        "type":      alert_type,
        "title":     title,
        "body":      body,
        "url":       url,
        "market_id": market_id,
        "priority":  priority,
        "sent_tg":   sent_tg,
        "fired_at":  datetime.now(timezone.utc).isoformat(),
    }
    with _lock:
        state["alerts_sent"].appendleft(record)


def alert_engine_thread():
    """
    Checks movers, whales, and signals every 60s.
    Fires alerts for anything crossing the threshold that isn't in cooldown.
    """
    while True:
        try:
            now = time.time()

            # ── Whale alerts ──────────────────────────────────────
            with _lock:
                recent_whales = list(state["whale_trades"])[:20]

            for w in recent_whales:
                if w.get("tier") not in ALERT_WHALE_TIER:
                    continue
                mid = str(w.get("market_id", w.get("trade_id", "")))
                if not _check_cooldown(mid, "whale"):
                    continue
                _set_cooldown(mid, "whale")
                fire_alert(
                    alert_type="whale",
                    title=f"{w['tier']} ORDER — ${w['usd_size']:,.0f}",
                    body=(
                        f"{w['side']} {w['size']:,.0f} shares @ {w['price']*100:.1f}¢\n"
                        f"{w['market_title'][:80]}"
                    ),
                    url=w.get("market_url", ""),
                    market_id=mid,
                    priority="high" if w["tier"] == "MEGA" else "normal",
                )

            # ── Mover alerts ──────────────────────────────────────
            with _lock:
                movers = list(state["movers"])

            for m in movers:
                if m.get("change_abs", 0) < ALERT_MOVER_PCT:
                    continue
                mid = str(m.get("market_id", ""))
                if not _check_cooldown(mid, "mover"):
                    continue
                _set_cooldown(mid, "mover")
                direction = "▲" if m["direction"] == "UP" else "▼"
                fire_alert(
                    alert_type="mover",
                    title=f"PRICE MOVER {direction} {m['change_pct']:+.1f}%",
                    body=(
                        f"{m['market_title'][:80]}\n"
                        f"Now: {m['yes_price']*100:.1f}¢  "
                        f"1h ago: {m['price_1h_ago']*100:.1f}¢"
                    ),
                    url=m.get("market_url", ""),
                    market_id=mid,
                )

            # ── High-score signal alerts ──────────────────────────
            with _lock:
                signals = list(state["signals"])[:50]

            for s in signals:
                if s.get("score", 0) < ALERT_SIGNAL_SCORE:
                    continue
                if s.get("article_age_mins", 9999) > 60:
                    continue  # Only alert on articles <1 hour old
                mid = str(s.get("market_id", ""))
                if not _check_cooldown(mid, "signal"):
                    continue
                _set_cooldown(mid, "signal")
                fire_alert(
                    alert_type="signal",
                    title=f"SIGNAL {int(s['score']*100)}% — {s['article_source']}",
                    body=(
                        f"{s['article_title'][:100]}\n"
                        f"→ {s['market_title'][:80]}\n"
                        f"YES: {s['yes_price']*100:.0f}¢"
                    ),
                    url=s.get("market_url", ""),
                    market_id=mid,
                )

        except Exception as e:
            log.error(f"Alert engine: {e}")

        time.sleep(60)


# ══════════════════════════════════════════════════════════════════
# PORTFOLIO OVERLAY
# Reads trades.jsonl (written by polymarket_auto_buy.py).
# Computes open positions, entry prices, unrealised P&L against
# current market prices, and distance to TP/SL targets.
# ══════════════════════════════════════════════════════════════════

def load_trade_log() -> list:
    if not os.path.exists(TRADES_LOG_FILE):
        return []
    records = []
    with open(TRADES_LOG_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def compute_positions(records: list, markets_by_token: dict) -> list:
    """
    Matches ENTRY and EXIT records to build open position list.
    Enriches with current price and P&L from live market data.
    """
    entries = {r["order_id"]: r for r in records
               if r.get("event") == "ENTRY" and not r.get("dry_run")}
    exits   = {r["order_id"] for r in records
               if r.get("event") == "EXIT"  and not r.get("dry_run")}

    positions = []
    for order_id, entry in entries.items():
        if order_id in exits:
            continue  # closed

        token_id    = entry.get("token_id", "")
        entry_price = entry.get("entry_price", 0)
        shares      = entry.get("shares", 0)
        market      = markets_by_token.get(token_id, {})
        current_price = market.get("yes_price", entry_price)

        cost          = entry_price * shares
        current_val   = current_price * shares
        pnl_usdc      = round(current_val - cost, 4)
        pnl_pct       = round((pnl_usdc / cost) * 100, 2) if cost else 0

        positions.append({
            "order_id":       order_id,
            "token_id":       token_id,
            "market_title":   market.get("title", token_id[:20] + "..."),
            "market_url":     market.get("url", ""),
            "category":       market.get("category", ""),
            "entry_price":    round(entry_price, 4),
            "current_price":  round(current_price, 4),
            "shares":         round(shares, 4),
            "cost_usdc":      round(cost, 4),
            "current_val":    round(current_val, 4),
            "pnl_usdc":       pnl_usdc,
            "pnl_pct":        pnl_pct,
            "signal_source":  entry.get("signal_source", ""),
            "entered_at":     entry.get("logged_at", ""),
            "status":         "OPEN",
        })

    return sorted(positions, key=lambda x: abs(x["pnl_pct"]), reverse=True)


def portfolio_tracker_thread():
    """Reloads trades.jsonl every 2 minutes, enriches with live prices."""
    while True:
        try:
            records = load_trade_log()
            if not records:
                time.sleep(120)
                continue

            with _lock:
                all_markets = [m for ms in state["markets"].values() for m in ms]

            markets_by_token = {m["token_id"]: m for m in all_markets
                                if m.get("token_id")}

            positions  = compute_positions(records, markets_by_token)
            total_cost = sum(p["cost_usdc"]    for p in positions)
            total_val  = sum(p["current_val"]  for p in positions)
            total_pnl  = round(total_val - total_cost, 4)

            # Alert on positions moving against us significantly
            for pos in positions:
                if pos["pnl_pct"] <= -8.0:
                    mid = pos["order_id"]
                    if _check_cooldown(mid, "position_loss"):
                        _set_cooldown(mid, "position_loss")
                        fire_alert(
                            alert_type="mover",
                            title=f"POSITION DOWN {pos['pnl_pct']:.1f}%",
                            body=(
                                f"{pos['market_title'][:80]}\n"
                                f"Entry: {pos['entry_price']*100:.1f}¢  "
                                f"Now: {pos['current_price']*100:.1f}¢  "
                                f"P&L: ${pos['pnl_usdc']:+.2f}"
                            ),
                            url=pos.get("market_url", ""),
                            market_id=mid,
                            priority="high",
                        )

            with _lock:
                state["positions"]           = positions
                state["portfolio_pnl"]       = total_pnl
                state["last_portfolio_scan"] = datetime.now(timezone.utc).isoformat()

            if positions:
                log.info(f"Portfolio: {len(positions)} open | P&L: ${total_pnl:+.2f}")

        except Exception as e:
            log.error(f"Portfolio tracker: {e}")

        time.sleep(120)


# ── Boot ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  Intel Tripwire — Intelligence Server v3")
    print("=" * 60)
    print(f"  Categories   : {len(CATEGORIES)}")
    rss_count = sum(len(v['feeds']) for v in CATEGORIES.values())
    print(f"  RSS feeds    : {rss_count}")
    print(f"  Whale floor  : ${WHALE_THRESHOLD:,}")
    print(f"  Brief score  : {BRIEF_MIN_SCORE}")
    print(f"  Mover alert  : {ALERT_MOVER_PCT}%")
    print(f"  Signal alert : {ALERT_SIGNAL_SCORE}")
    print(f"  Anthropic    : {'SET' if ANTHROPIC_API_KEY else 'NOT SET — briefs disabled'}")
    print(f"  Telegram     : {'SET' if TELEGRAM_BOT_TOKEN else 'NOT SET — console only'}")
    print(f"  Trades log   : {TRADES_LOG_FILE}")
    print(f"  API          : http://localhost:5001")
    print(f"  Dashboard    : Open intelligence_dashboard.html")
    print("=" * 60)
    print()
    print("  Endpoints:")
    print("    GET /status          — system health + all counters")
    print("    GET /markets         — live markets by category")
    print("    GET /signals         — news-to-market signal matches")
    print("    GET /movers          — price velocity alerts")
    print("    GET /whales          — large order flow")
    print("    GET /briefs          — AI analyst briefs")
    print("    GET /portfolio       — open positions + P&L")
    print("    GET /alerts          — fired alert log")
    print("    GET /tags/discovered — auto-discovered Polymarket tags")
    print("    GET /search?q=...    — cross-search markets + news")
    print()
    print("  .env keys:")
    print("    ANTHROPIC_API_KEY   — enables AI briefs")
    print("    TELEGRAM_BOT_TOKEN  — enables push alerts")
    print("    TELEGRAM_CHAT_ID    — your Telegram chat/channel ID")
    print()

    threads = [
        threading.Thread(target=market_scanner_thread,   daemon=True, name="market-scanner"),
        threading.Thread(target=news_fetcher_thread,     daemon=True, name="news-fetcher"),
        threading.Thread(target=price_velocity_thread,   daemon=True, name="velocity-tracker"),
        threading.Thread(target=whale_watcher_thread,    daemon=True, name="whale-watcher"),
        threading.Thread(target=brief_engine_thread,     daemon=True, name="brief-engine"),
        threading.Thread(target=alert_engine_thread,     daemon=True, name="alert-engine"),
        threading.Thread(target=portfolio_tracker_thread, daemon=True, name="portfolio-tracker"),
    ]
    for t in threads:
        t.start()
        log.info(f"Thread started: {t.name}")

    port = int(os.environ.get("PORT", 5001))
    log.info(f"Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

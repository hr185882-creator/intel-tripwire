import os

bind = f"0.0.0.0:{os.environ.get('PORT', '5001')}"

# IMPORTANT: keep a single worker because the app uses background threads.
# Multiple workers would duplicate the scanners / alerts.
workers = 1
threads = 8

timeout = 120
graceful_timeout = 30
keepalive = 5


def post_worker_init(worker):
    # Start background threads once the worker process is ready.
    from app import start_background_threads

    start_background_threads()

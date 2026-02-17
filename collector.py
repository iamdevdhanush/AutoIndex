"""
AutoIndex – collector.py
Runs autoindex.autoindex_run() every minute, logging output.
Designed to be kept running as a background process or systemd service.

Usage:
    python collector.py [--min-ms 100] [--interval 60]
"""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone

from db import DB_CONFIG, call_procedure, check_pg_stat_statements

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("autoindex_collector.log"),
    ],
)
logger = logging.getLogger("autoindex.collector")

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True


def _shutdown(sig, frame):  # noqa: ANN001
    global _running
    logger.info("Shutdown signal received (%s).  Stopping after current run.", sig)
    _running = False


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_once(min_ms: int) -> None:
    """Execute one full AutoIndex analysis cycle."""
    start = time.monotonic()
    logger.info("─── AutoIndex cycle starting  (threshold=%d ms) ───", min_ms)
    try:
        call_procedure("autoindex.autoindex_run", min_ms)
        elapsed = time.monotonic() - start
        logger.info("─── Cycle complete in %.2fs ───", elapsed)
    except Exception as exc:
        logger.error("Cycle failed: %s", exc, exc_info=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="AutoIndex background collector")
    parser.add_argument(
        "--min-ms",
        type=int,
        default=100,
        metavar="MS",
        help="Slow query threshold in milliseconds (default: 100)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        metavar="SEC",
        help="Seconds between analysis runs (default: 60)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Execute a single cycle and exit",
    )
    args = parser.parse_args()

    logger.info("AutoIndex Collector starting up.")
    logger.info(
        "  Target DB : %s:%s/%s",
        DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"],
    )
    logger.info("  Threshold : %d ms", args.min_ms)
    logger.info("  Interval  : %d s",  args.interval)

    # Preflight check
    if not check_pg_stat_statements():
        logger.critical(
            "pg_stat_statements extension not found or not accessible. "
            "Add 'shared_preload_libraries = pg_stat_statements' to "
            "postgresql.conf and restart PostgreSQL."
        )
        sys.exit(1)

    if args.run_once:
        run_once(args.min_ms)
        return

    logger.info("Entering collection loop.  Press Ctrl+C to stop.")
    while _running:
        run_once(args.min_ms)
        # Sleep in small ticks so SIGINT is handled quickly
        deadline = time.monotonic() + args.interval
        while _running and time.monotonic() < deadline:
            time.sleep(1)

    logger.info("AutoIndex Collector stopped.")


if __name__ == "__main__":
    main()

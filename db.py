"""
AutoIndex – db.py
Centralised database connection handling using psycopg2.
"""

import os
import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)

# ── Connection defaults (override via environment variables) ──────────────────
DB_CONFIG: dict = {
    "host":     os.getenv("AUTOINDEX_HOST",     "localhost"),
    "port":     int(os.getenv("AUTOINDEX_PORT", "5432")),
    "dbname":   os.getenv("AUTOINDEX_DBNAME",   "postgres"),
    "user":     os.getenv("AUTOINDEX_USER",     "postgres"),
    "password": os.getenv("AUTOINDEX_PASSWORD", ""),
    "options":  "-c search_path=autoindex,public",
    "connect_timeout": 10,
}


def get_connection() -> PgConnection:
    """Return a new psycopg2 connection.  Caller is responsible for closing."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        logger.debug("Database connection established.")
        return conn
    except psycopg2.OperationalError as exc:
        logger.critical("Cannot connect to PostgreSQL: %s", exc)
        raise


@contextmanager
def db_cursor(
    autocommit: bool = False,
    cursor_factory=psycopg2.extras.RealDictCursor,
) -> Generator:
    """
    Context manager that yields a (connection, cursor) pair.

    Usage::

        with db_cursor() as (conn, cur):
            cur.execute("SELECT 1")
            conn.commit()
    """
    conn = get_connection()
    conn.autocommit = autocommit
    try:
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield conn, cur
        if not autocommit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def check_pg_stat_statements() -> bool:
    """Return True when pg_stat_statements is loaded and accessible."""
    try:
        with db_cursor(autocommit=True) as (_, cur):
            cur.execute(
                "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'"
            )
            return cur.fetchone() is not None
    except Exception as exc:
        logger.error("pg_stat_statements check failed: %s", exc)
        return False


def call_procedure(name: str, *args) -> None:
    """Execute a stored procedure (CALL schema.proc(args))."""
    placeholders = ", ".join(["%s"] * len(args))
    sql = f"CALL {name}({placeholders})"
    with db_cursor() as (conn, cur):
        cur.execute(sql, args)
        # Surface NOTICE messages
        for notice in conn.notices:
            logger.info(notice.strip())
        conn.notices.clear()


def call_function(name: str, *args):
    """Execute a stored function and return the scalar result."""
    placeholders = ", ".join(["%s"] * len(args))
    sql = f"SELECT {name}({placeholders})"
    with db_cursor() as (_, cur):
        cur.execute(sql, args)
        row = cur.fetchone()
        return list(row.values())[0] if row else None

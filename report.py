"""
AutoIndex – report.py
Prints human-readable recommendation and alert reports.

Usage:
    python report.py                    # full recommendations table
    python report.py --seq-scans        # sequential scan alerts
    python report.py --unoptimized      # queries with no recommendation yet
    python report.py --stats            # summary statistics
    python report.py --apply <rec_id>   # mark recommendation as 'applied'
    python report.py --dismiss <rec_id> # mark recommendation as 'dismissed'
"""

import argparse
import sys
import textwrap
from typing import Any

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

from db import db_cursor

# ── ANSI colours ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def _colour(text: str, code: str) -> str:
    return f"{code}{text}{RESET}"


def _fmt_pct(pct: Any) -> str:
    if pct is None:
        return "N/A"
    pct = float(pct)
    if pct >= 70:
        return _colour(f"{pct:.1f}%", GREEN)
    if pct >= 40:
        return _colour(f"{pct:.1f}%", YELLOW)
    return _colour(f"{pct:.1f}%", RED)


def _fmt_status(status: str) -> str:
    colours = {"pending": YELLOW, "applied": GREEN, "dismissed": RED}
    return _colour(status.upper(), colours.get(status, RESET))


def _print_table(rows: list[dict], headers: list[str], title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'─' * 70}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 70}{RESET}")

    if not rows:
        print("  (no data)\n")
        return

    data = [[row.get(h, "") for h in headers] for row in rows]

    if HAS_TABULATE:
        print(tabulate(data, headers=headers, tablefmt="rounded_outline"))
    else:
        # Fallback: fixed-width columns
        col_widths = [
            max(len(str(h)), max((len(str(r[i])) for r in data), default=0))
            for i, h in enumerate(headers)
        ]
        sep = "  ".join("-" * w for w in col_widths)
        header_line = "  ".join(str(h).ljust(w) for h, w in zip(headers, col_widths))
        print(header_line)
        print(sep)
        for row in data:
            print("  ".join(str(v).ljust(w) for v, w in zip(row, col_widths)))

    print()


# ── Report sections ───────────────────────────────────────────────────────────

def report_recommendations(limit: int = 50) -> None:
    sql = """
        SELECT
            rec_id,
            query_id,
            mean_ms,
            calls,
            cost_before,
            cost_after,
            improvement_pct,
            status,
            left(reason, 80)             AS reason,
            left(recommended_index_sql, 90) AS index_sql
        FROM autoindex.v_recommendations_summary
        LIMIT %s
    """
    with db_cursor() as (_, cur):
        cur.execute(sql, (limit,))
        rows = [dict(r) for r in cur.fetchall()]

    # Apply colour formatting
    for r in rows:
        r["improvement_pct"] = _fmt_pct(r.get("improvement_pct"))
        r["status"]          = _fmt_status(str(r.get("status", "")))

    _print_table(
        rows,
        headers=["rec_id","query_id","mean_ms","calls",
                 "cost_before","cost_after","improvement_pct",
                 "status","reason","index_sql"],
        title="INDEX RECOMMENDATIONS",
    )


def report_seq_scans(limit: int = 30) -> None:
    sql = """
        SELECT
            id               AS query_id,
            mean_ms,
            calls,
            rows_removed_by_filter,
            captured_at,
            left(query_preview, 100) AS query_preview
        FROM autoindex.v_seq_scan_alerts
        LIMIT %s
    """
    with db_cursor() as (_, cur):
        cur.execute(sql, (limit,))
        rows = [dict(r) for r in cur.fetchall()]

    _print_table(
        rows,
        headers=["query_id","mean_ms","calls","rows_removed_by_filter",
                 "captured_at","query_preview"],
        title="SEQUENTIAL SCAN ALERTS",
    )


def report_unoptimized(limit: int = 30) -> None:
    sql = """
        SELECT
            id          AS query_id,
            mean_ms,
            calls,
            captured_at,
            left(query_preview, 100) AS query_preview
        FROM autoindex.v_unoptimized_queries
        LIMIT %s
    """
    with db_cursor() as (_, cur):
        cur.execute(sql, (limit,))
        rows = [dict(r) for r in cur.fetchall()]

    _print_table(
        rows,
        headers=["query_id","mean_ms","calls","captured_at","query_preview"],
        title="SLOW QUERIES WITH NO RECOMMENDATION YET",
    )


def report_stats() -> None:
    sql = """
        SELECT
            (SELECT count(*) FROM autoindex.slow_query_log)            AS total_captured,
            (SELECT count(*) FROM autoindex.query_plan_history)        AS plans_analyzed,
            (SELECT count(*) FROM autoindex.index_recommendations)     AS total_recs,
            (SELECT count(*) FROM autoindex.index_recommendations
             WHERE status = 'pending')                                  AS pending,
            (SELECT count(*) FROM autoindex.index_recommendations
             WHERE status = 'applied')                                  AS applied,
            (SELECT count(*) FROM autoindex.index_recommendations
             WHERE status = 'dismissed')                                AS dismissed,
            (SELECT round(avg(mean_time)::NUMERIC, 2)
             FROM autoindex.slow_query_log)                             AS avg_mean_ms,
            (SELECT round(max(mean_time)::NUMERIC, 2)
             FROM autoindex.slow_query_log)                             AS max_mean_ms
    """
    with db_cursor() as (_, cur):
        cur.execute(sql)
        row = dict(cur.fetchone())

    print(f"\n{BOLD}{CYAN}{'─' * 50}{RESET}")
    print(f"{BOLD}{CYAN}  AUTOINDEX STATISTICS{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 50}{RESET}")
    for k, v in row.items():
        label = k.replace("_", " ").title().ljust(26)
        print(f"  {label}: {_colour(str(v), BOLD)}")
    print()


def report_full_query(query_id: int) -> None:
    """Print the full query text and its recommendations."""
    with db_cursor() as (_, cur):
        cur.execute(
            "SELECT query_text, mean_time, calls, captured_at "
            "FROM autoindex.slow_query_log WHERE id = %s",
            (query_id,),
        )
        row = cur.fetchone()
        if not row:
            print(f"query_id {query_id} not found.")
            return

        row = dict(row)
        print(f"\n{BOLD}Query ID: {query_id}{RESET}")
        print(f"Mean time : {row['mean_time']:.2f} ms   Calls: {row['calls']}")
        print(f"Captured  : {row['captured_at']}")
        print(f"\n{BOLD}Full SQL:{RESET}")
        print(textwrap.indent(row["query_text"], "    "))

        cur.execute(
            "SELECT recommended_index_sql, reason, status, "
            "estimated_cost_before, estimated_cost_after "
            "FROM autoindex.index_recommendations "
            "WHERE query_id = %s ORDER BY created_at",
            (query_id,),
        )
        recs = cur.fetchall()
        if recs:
            print(f"\n{BOLD}Recommendations:{RESET}")
            for r in recs:
                r = dict(r)
                print(f"  [{_fmt_status(r['status'])}] {r['recommended_index_sql']}")
                print(f"         Reason : {r['reason']}")
                if r["estimated_cost_before"]:
                    pct = None
                    if r["estimated_cost_before"] > 0:
                        pct = (r["estimated_cost_before"] - r["estimated_cost_after"]) \
                              / r["estimated_cost_before"] * 100
                    print(f"         Cost   : {r['estimated_cost_before']:.2f} → "
                          f"{r['estimated_cost_after']:.2f}  "
                          f"({_fmt_pct(pct)} improvement)")
                print()


def set_status(rec_id: int, status: str) -> None:
    """Update the status of a recommendation."""
    valid = {"applied", "dismissed", "pending"}
    if status not in valid:
        print(f"Invalid status '{status}'. Choose from: {valid}")
        sys.exit(1)

    with db_cursor() as (conn, cur):
        cur.execute(
            "UPDATE autoindex.index_recommendations "
            "SET status = %s WHERE id = %s RETURNING id",
            (status, rec_id),
        )
        row = cur.fetchone()
        if row:
            print(f"Recommendation {rec_id} marked as {_fmt_status(status)}.")
        else:
            print(f"Recommendation {rec_id} not found.")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="AutoIndex – recommendation report viewer"
    )
    parser.add_argument(
        "--seq-scans",    action="store_true",
        help="Show sequential scan alerts"
    )
    parser.add_argument(
        "--unoptimized",  action="store_true",
        help="Show slow queries with no recommendation"
    )
    parser.add_argument(
        "--stats",        action="store_true",
        help="Show summary statistics"
    )
    parser.add_argument(
        "--query",        type=int, metavar="QUERY_ID",
        help="Show full details for a specific query_id"
    )
    parser.add_argument(
        "--apply",        type=int, metavar="REC_ID",
        help="Mark a recommendation as applied"
    )
    parser.add_argument(
        "--dismiss",      type=int, metavar="REC_ID",
        help="Mark a recommendation as dismissed"
    )
    parser.add_argument(
        "--limit",        type=int, default=50, metavar="N",
        help="Max rows to display (default: 50)"
    )
    args = parser.parse_args()

    if args.apply:
        set_status(args.apply, "applied")
    elif args.dismiss:
        set_status(args.dismiss, "dismissed")
    elif args.query:
        report_full_query(args.query)
    elif args.seq_scans:
        report_seq_scans(args.limit)
    elif args.unoptimized:
        report_unoptimized(args.limit)
    elif args.stats:
        report_stats()
    else:
        # Default: show everything
        report_stats()
        report_recommendations(args.limit)
        report_seq_scans(args.limit)


if __name__ == "__main__":
    main()
```

---

## `requirements.txt`
```
psycopg2-binary>=2.9.9
tabulate>=0.9.0

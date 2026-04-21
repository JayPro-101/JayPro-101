"""
flarelogic/main.py
==================
The Orchestrator — FlareLogic Sentinel Entry Point

ANALOGY: This is the "Operations Manager" of the entire system. It doesn't do
the actual work itself — that's handled by the specialists (engine.py,
database.py, whatsapp.py). Instead, it coordinates them in the correct order,
handles failures gracefully, and ensures the entire flow runs from start to
finish. A CRON job calls this file every morning at 6:00 AM.

USAGE:
  # Daily CRON execution (no arguments — runs full sentinel sweep):
  python main.py

  # Ingest a new CSV file:
  python main.py --ingest /path/to/inventory.csv

  # Run the sentinel scan without sending a WhatsApp message:
  python main.py --scan-only

  # Test the WhatsApp message formatting without sending:
  python main.py --dry-run

  # Verify Twilio credentials:
  python main.py --verify-twilio

  # Interactive mode — step through each phase:
  python main.py --interactive

CRON SETUP (Termux):
  # Install crond: pkg install cronie
  # Open crontab: crontab -e
  # Add this line (runs at 6:00 AM every day):
  0 6 * * * /data/data/com.termux/files/usr/bin/python /path/to/main.py >> /path/to/logs/cron.log 2>&1
"""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import argparse
import csv
import logging
import sys
from datetime import date
from pathlib import Path

# ─────────────────────────────────────────────
# LOGGING CONFIGURATION
# Must be set up BEFORE any module-level loggers are used.
# ─────────────────────────────────────────────

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        # Console output — you see this when running manually
        logging.StreamHandler(sys.stdout),
        # File output — persists for debugging and audit
        logging.FileHandler(LOG_DIR / "sentinel.log", encoding="utf-8"),
    ],
)

log = logging.getLogger("flarelogic.main")

# ─────────────────────────────────────────────
# PROJECT IMPORTS (after logging is configured)
# ─────────────────────────────────────────────

from flarelogic import database, engine
from flarelogic.models import DataSource, RawInventoryRow
from flarelogic.services.whatsapp import send_whatsapp_report, verify_twilio_credentials

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DEBUG_LOG = DATA_DIR / "debug.txt"
DB_PATH = DATA_DIR / "sentinel.db"

DATA_DIR.mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════════
# PHASE 1: INGESTION
# ═══════════════════════════════════════════════════════════════════

def ingest_csv(csv_path: Path, store_name: str = "Default Store") -> int:
    """
    Phase 1 of the flowchart: CSV Ingestion + Pydantic Validation.

    Reads each row, validates it, writes clean rows to SQLite.
    Bad rows are logged to debug.txt.

    Returns:
        Number of successfully ingested rows.
    """
    log.info(f"━━━━ PHASE 1: INGESTION ━━━━")
    log.info(f"Ingesting CSV: {csv_path}")

    if not csv_path.exists():
        log.critical(f"CSV file not found: {csv_path}")
        _append_debug(f"INGESTION FAILED: File not found: {csv_path}")
        return 0

    success_count = 0
    fail_count = 0

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        # utf-8-sig handles the BOM (Byte Order Mark) that Microsoft Excel
        # adds to UTF-8 files. Without this, the first column header gets
        # a invisible garbage character prepended to it.
        reader = csv.DictReader(f)

        for line_num, row in enumerate(reader, start=2):  # start=2 because row 1 is headers
            # Pass through the Pydantic validation checkpoint
            item = engine.parse_raw_row(
                raw_data=dict(row),
                debug_log_path=DEBUG_LOG,
            )

            if item is None:
                fail_count += 1
                log.warning(f"Row {line_num}: Validation failed. Logged to debug.txt.")
                continue

            # Step 3: Write/update in SQLite
            try:
                database.upsert_item(item, db_path=DB_PATH)
                success_count += 1
            except Exception as e:
                log.error(f"Row {line_num}: Database error: {e}")
                _append_debug(f"DB ERROR on row {line_num}: {e} | Data: {row}")
                fail_count += 1

    log.info(f"Ingestion complete: {success_count} ✓ | {fail_count} ✗")

    if fail_count > 0:
        log.warning(
            f"⚠️  {fail_count} rows failed validation. "
            f"Review: {DEBUG_LOG}"
        )

    return success_count


def _append_debug(message: str) -> None:
    """Append a timestamped message to debug.txt."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(DEBUG_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


# ═══════════════════════════════════════════════════════════════════
# PHASE 2 & 3: DATABASE + SENTINEL SCAN
# ═══════════════════════════════════════════════════════════════════

def run_sentinel_scan(store_name: str = "Default Store") -> engine.DailyReport:
    """
    Phase 2 & 3: Load inventory, run Sentinel math, update database.

    Returns:
        A fully populated DailyReport ready for dispatch.
    """
    log.info(f"━━━━ PHASE 3: SENTINEL SCAN ━━━━")

    # Load all active inventory from SQLite
    items = database.fetch_all_items(db_path=DB_PATH)

    if not items:
        log.warning("Database is empty. Run with --ingest first.")
        # Return an empty report rather than crashing
        from flarelogic.models import DailyReport
        from decimal import Decimal
        return DailyReport(
            report_date=date.today(),
            store_name=store_name,
            total_items_scanned=0,
            critical_count=0,
            warning_count=0,
            safe_count=0,
            total_potential_recovery=Decimal("0.00"),
            critical_items=[],
            warning_items=[],
        )

    # Run Sentinel math on every item
    processed_items = engine.run_batch_sentinel(items)

    # Persist updated statuses to SQLite (Step 4: assign risk flags)
    log.info("Persisting Sentinel results to database...")
    for item in processed_items:
        database.update_sentinel_fields(item, db_path=DB_PATH)

    # Build the report
    report = engine.build_daily_report(processed_items, store_name=store_name)

    # Log the scan to audit table
    database.log_scan(
        total=report.total_items_scanned,
        critical=report.critical_count,
        warning=report.warning_count,
        safe=report.safe_count,
        report_sent=False,  # Will update after WhatsApp send
        db_path=DB_PATH,
    )

    log.info(
        f"Scan complete | "
        f"Critical: {report.critical_count} | "
        f"Warning: {report.warning_count} | "
        f"Safe: {report.safe_count} | "
        f"Potential Recovery: ₦{report.total_potential_recovery:,.2f}"
    )

    return report


# ═══════════════════════════════════════════════════════════════════
# PHASE 4: WHATSAPP COMMUNICATION GATEWAY
# ═══════════════════════════════════════════════════════════════════

def send_report(report, dry_run: bool = False) -> bool:
    """
    Phase 4: Send the Daily Report via WhatsApp.

    Args:
        report: The DailyReport from run_sentinel_scan().
        dry_run: If True, prints message instead of sending.

    Returns:
        True if sent (or dry_run), False on failure.
    """
    log.info(f"━━━━ PHASE 4: WHATSAPP GATEWAY ━━━━")
    success = send_whatsapp_report(report, dry_run=dry_run)

    if success:
        log.info("✅ Report dispatched to manager.")
    else:
        log.error("❌ Failed to send WhatsApp report.")
        _append_debug("WhatsApp send failed — check sentinel.log for details.")

    return success


# ═══════════════════════════════════════════════════════════════════
# CLI ARGUMENT PARSER
# ═══════════════════════════════════════════════════════════════════

def build_arg_parser() -> argparse.ArgumentParser:
    """
    ArgumentParser builds the command-line interface.

    ANALOGY: This is like designing the control panel for the system.
    Each `--flag` is a button or switch the operator can press.
    """
    parser = argparse.ArgumentParser(
        prog="sentinel",
        description="FlareLogic Sentinel — Retail Inventory Intelligence Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          Run full daily sweep
  python main.py --ingest inventory.csv  Ingest a new CSV file
  python main.py --scan-only             Scan without sending WhatsApp
  python main.py --dry-run               Show message without sending
  python main.py --verify-twilio         Test Twilio credentials
        """
    )
    parser.add_argument(
        "--ingest",
        metavar="CSV_PATH",
        type=Path,
        help="Path to a CSV inventory file to ingest.",
    )
    parser.add_argument(
        "--store-name",
        default="FlareLogic Store",
        help="Store name to display in reports. (default: 'FlareLogic Store')",
    )
    parser.add_argument(
        "--scan-only",
        action="store_true",
        help="Run the Sentinel scan but do not send WhatsApp report.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run everything, but print the WhatsApp message instead of sending.",
    )
    parser.add_argument(
        "--verify-twilio",
        action="store_true",
        help="Check Twilio credentials and exit.",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize the database schema and exit.",
    )
    return parser


# ═══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

def main() -> int:
    """
    The main function. Returns an exit code:
      0 = success (CRON sees this as OK)
      1 = failure (CRON can trigger alerts on non-zero exit)

    `if __name__ == "__main__"` explained:
    Python sets the special variable __name__ to "__main__" ONLY when the
    file is run directly (python main.py). If another file imports main.py,
    __name__ is "flarelogic.main" instead. This guard ensures that the
    main() function only runs when YOU execute this file, not when it's
    imported as a module for testing.
    """
    parser = build_arg_parser()
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  FlareLogic Sentinel — Starting")
    log.info(f"  Store: {args.store_name}")
    log.info(f"  Date:  {date.today().isoformat()}")
    log.info("=" * 60)

    # ── Utility commands (exit early) ──

    if args.init_db:
        database.initialize_database(db_path=DB_PATH)
        log.info("✅ Database initialized.")
        return 0

    if args.verify_twilio:
        success = verify_twilio_credentials()
        return 0 if success else 1

    # ── Ensure database is ready ──
    database.initialize_database(db_path=DB_PATH)

    # ── Phase 1: Ingest if a CSV was provided ──
    if args.ingest:
        ingested = ingest_csv(args.ingest, store_name=args.store_name)
        if ingested == 0:
            log.error("No rows were successfully ingested. Aborting scan.")
            return 1

    # ── Phase 2 + 3: Sentinel Scan ──
    report = run_sentinel_scan(store_name=args.store_name)

    # ── Phase 4: WhatsApp Gateway ──
    if args.scan_only:
        log.info("--scan-only flag set. Skipping WhatsApp report.")
        # Still print a summary to the terminal
        print(f"\n📊 Sentinel Summary for {report.report_date}")
        print(f"   Critical: {report.critical_count} items")
        print(f"   Warning:  {report.warning_count} items")
        print(f"   Safe:     {report.safe_count} items")
        print(f"   Potential Recovery: ₦{report.total_potential_recovery:,.2f}")
        return 0

    sent = send_report(report, dry_run=args.dry_run)

    # ── Phase 5: Log completion ──
    log.info("=" * 60)
    log.info("  FlareLogic Sentinel — Cycle Complete")
    log.info(f"  WhatsApp Sent: {'✅ Yes' if sent else '❌ No'}")
    log.info("=" * 60)

    return 0 if sent else 1


if __name__ == "__main__":
    sys.exit(main())

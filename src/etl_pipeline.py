"""
etl_pipeline.py
Transaction Data ETL Pipeline
-------------------------------
Extract  → Load raw CSV into staging
Transform → Clean, validate, compute balances
Load     → Write clean data + account summaries to SQLite

Resume keywords covered:
  ETL pipeline | Python | Pandas | SQL | data cleaning | validation |
  data integrity | aggregation | insights | optimized queries
"""

import os
import logging
import sqlite3
import time
from datetime import datetime

import pandas as pd

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
CSV_PATH = "data/transactions.csv"
DB_PATH  = "data/transactions.db"
LOG_PATH = "logs/etl.log"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
# Fix Windows terminal encoding
import sys
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════
# STEP 1 — EXTRACT
# ═════════════════════════════════════════════
def extract(path: str) -> pd.DataFrame:
    log.info("-- EXTRACT --------------------------------------")
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}  →  Run generate_data.py first")

    df = pd.read_csv(path)
    log.info(f"Loaded {len(df):,} records from '{path}'")
    log.info(f"Columns: {list(df.columns)}")
    return df


# ═════════════════════════════════════════════
# STEP 2 — TRANSFORM
# ═════════════════════════════════════════════
def transform(df: pd.DataFrame):
    log.info("-- TRANSFORM ------------------------------------")
    original_count = len(df)

    # 2a. Type casting
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["amount"]    = pd.to_numeric(df["amount"], errors="coerce")
    df["transaction_type"] = df["transaction_type"].str.strip().str.lower()

    # 2b. Drop rows with critical nulls
    before = len(df)
    df = df.dropna(subset=["transaction_id", "account_id", "amount",
                            "transaction_type", "timestamp"])
    log.info(f"Dropped {before - len(df)} rows with critical nulls")

    # 2c. Remove invalid amounts
    before = len(df)
    df = df[df["amount"] > 0]
    log.info(f"Dropped {before - len(df)} rows with zero/negative amounts")

    # 2d. Validate transaction_type
    before = len(df)
    df = df[df["transaction_type"].isin(["credit", "debit"])]
    log.info(f"Dropped {before - len(df)} rows with invalid transaction_type")

    # 2e. Remove duplicate transaction IDs
    before = len(df)
    df = df.drop_duplicates(subset=["transaction_id"])
    log.info(f"Dropped {before - len(df)} duplicate transaction IDs")

    # 2f. Add derived columns
    df = df.copy()
    df["year_month"] = df["timestamp"].dt.to_period("M").astype(str)
    df["signed_amount"] = df.apply(
        lambda r: r["amount"] if r["transaction_type"] == "credit" else -r["amount"],
        axis=1,
    )

    clean_count = len(df)
    integrity   = round(clean_count / original_count * 100, 2)
    log.info(f"Clean records: {clean_count:,} / {original_count:,} ({integrity}% data integrity)")

    # 2g. Account balance aggregation — fixed lambda
    credits = df[df["transaction_type"] == "credit"].groupby("account_id")["amount"].sum()
    debits  = df[df["transaction_type"] == "debit"].groupby("account_id")["amount"].sum()
    counts  = df.groupby("account_id")["transaction_id"].count()
    avg_amt = df.groupby("account_id")["amount"].mean().round(2)
    last_dt = df.groupby("account_id")["timestamp"].max()

    account_summary = pd.DataFrame({
        "total_credits":   credits,
        "total_debits":    debits,
        "txn_count":       counts,
        "avg_txn_amount":  avg_amt,
        "last_txn_date":   last_dt,
    }).fillna(0).reset_index()

    account_summary["balance"] = (
        account_summary["total_credits"] - account_summary["total_debits"]
    ).round(2)

    log.info(f"Account summaries computed for {len(account_summary)} accounts")
    return df, account_summary


# ═════════════════════════════════════════════
# STEP 3 — LOAD
# ═════════════════════════════════════════════
def load(clean_df: pd.DataFrame, summary_df: pd.DataFrame, db_path: str) -> None:
    log.info("-- LOAD -----------------------------------------")
    os.makedirs("data", exist_ok=True)

    conn = sqlite3.connect(db_path)
    clean_df.to_sql("transactions",    conn, if_exists="replace", index=False)
    summary_df.to_sql("account_summary", conn, if_exists="replace", index=False)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_account ON transactions(account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_type    ON transactions(transaction_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date    ON transactions(timestamp)")
    conn.commit()
    conn.close()

    log.info(f"Loaded {len(clean_df):,} transactions  -> 'transactions' table")
    log.info(f"Loaded {len(summary_df)} account rows  -> 'account_summary' table")
    log.info(f"Database: {db_path}")


# ═════════════════════════════════════════════
# STEP 4 — ANALYTICS
# ═════════════════════════════════════════════
def run_analytics(db_path: str) -> None:
    log.info("-- ANALYTICS ------------------------------------")
    conn = sqlite3.connect(db_path)

    queries = {
        "Top 5 accounts by balance": """
            SELECT account_id, balance, txn_count
            FROM account_summary
            ORDER BY balance DESC
            LIMIT 5
        """,
        "Monthly transaction volume (last 6 months)": """
            SELECT year_month,
                   COUNT(*) AS txn_count,
                   ROUND(SUM(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END), 2) AS total_credits,
                   ROUND(SUM(CASE WHEN transaction_type='debit'  THEN amount ELSE 0 END), 2) AS total_debits
            FROM transactions
            GROUP BY year_month
            ORDER BY year_month DESC
            LIMIT 6
        """,
        "Credit vs Debit split": """
            SELECT transaction_type,
                   COUNT(*) AS count,
                   ROUND(SUM(amount), 2) AS total_amount,
                   ROUND(AVG(amount), 2) AS avg_amount
            FROM transactions
            GROUP BY transaction_type
        """,
    }

    for title, sql in queries.items():
        t0     = time.time()
        result = pd.read_sql_query(sql, conn)
        ms     = round((time.time() - t0) * 1000, 1)
        log.info(f"\n[QUERY] {title}  [{ms}ms]\n{result.to_string(index=False)}")

    conn.close()


# ═════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════
def run_pipeline():
    start = time.time()
    log.info("=" * 50)
    log.info("  TRANSACTION ETL PIPELINE  STARTED")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 50)

    try:
        raw_df                    = extract(CSV_PATH)
        clean_df, account_summary = transform(raw_df)
        load(clean_df, account_summary, DB_PATH)
        run_analytics(DB_PATH)

        elapsed = round(time.time() - start, 2)
        log.info("=" * 50)
        log.info(f"  PIPELINE COMPLETED in {elapsed}s [OK]")
        log.info("=" * 50)

    except Exception as e:
        log.error(f"Pipeline FAILED: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()

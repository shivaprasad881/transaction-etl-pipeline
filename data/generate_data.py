"""
generate_data.py
Generates 5,200 realistic bank transaction records → data/transactions.csv
"""

import csv
import random
import uuid
from datetime import datetime, timedelta

OUTPUT_PATH = "data/transactions.csv"
TOTAL_RECORDS = 100
NUM_ACCOUNTS = 50

random.seed(42)

# Generate account IDs
accounts = [f"ACC{str(i).zfill(4)}" for i in range(1, NUM_ACCOUNTS + 1)]

# Categories per type
CREDIT_CATEGORIES = ["salary", "refund", "transfer_in", "bonus", "interest"]
DEBIT_CATEGORIES  = ["shopping", "food", "utilities", "rent", "transfer_out", "atm"]

start_date = datetime(2023, 1, 1)
end_date   = datetime(2024, 12, 31)


def random_date():
    delta = end_date - start_date
    return start_date + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_row(i):
    # Inject ~5% nulls and ~2% bad data to simulate real-world mess
    if random.random() < 0.03:
        return {
            "transaction_id":   str(uuid.uuid4()),
            "account_id":       random.choice(accounts),
            "amount":           None,                    # null amount
            "transaction_type": random.choice(["credit", "debit"]),
            "timestamp":        random_date().isoformat(),
            "category":         random.choice(CREDIT_CATEGORIES),
            "description":      "null amount row",
        }

    if random.random() < 0.02:
        return {
            "transaction_id":   str(uuid.uuid4()),
            "account_id":       random.choice(accounts),
            "amount":           round(random.uniform(10, 5000), 2),
            "transaction_type": "unknown",               # bad type
            "timestamp":        random_date().isoformat(),
            "category":         "misc",
            "description":      "bad type row",
        }

    if random.random() < 0.01:
        return {
            "transaction_id":   str(uuid.uuid4()),
            "account_id":       None,                    # null account
            "amount":           round(random.uniform(10, 5000), 2),
            "transaction_type": random.choice(["credit", "debit"]),
            "timestamp":        random_date().isoformat(),
            "category":         "misc",
            "description":      "null account row",
        }

    txn_type = random.choice(["credit", "debit"])
    category = random.choice(CREDIT_CATEGORIES if txn_type == "credit" else DEBIT_CATEGORIES)

    return {
        "transaction_id":   str(uuid.uuid4()),
        "account_id":       random.choice(accounts),
        "amount":           round(random.uniform(10, 9999), 2),
        "transaction_type": txn_type,
        "timestamp":        random_date().isoformat(),
        "category":         category,
        "description":      f"{category.replace('_', ' ').title()} transaction",
    }


def main():
    rows = [generate_row(i) for i in range(TOTAL_RECORDS)]

    # Add ~10 exact duplicate transaction_ids to test dedup logic
    for _ in range(10):
        rows.append(random.choice(rows))

    random.shuffle(rows)

    fieldnames = ["transaction_id", "account_id", "amount",
                  "transaction_type", "timestamp", "category", "description"]

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Generated {len(rows)} records → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
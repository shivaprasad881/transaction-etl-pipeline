# Transaction ETL Pipeline – Project Flow

## 🔄 Overall Flow

generate_data.py → transactions.csv → etl_pipeline.py → SQLite DB → Analytics

---

## 🧩 Step-by-Step Explanation

### 1️⃣ Data Generation (generate_data.py)

* Generates realistic bank transaction data
* Includes:

  * Valid records
  * Null values
  * Invalid transaction types
  * Duplicate entries
* Output file:
  data/transactions.csv

Purpose: Simulates real-world messy data

---

### 2️⃣ EXTRACT

* Reads CSV file using Pandas
* Loads data into DataFrame
* Logs:

  * Total records
  * Column names

No transformation is done here

---

### 3️⃣ TRANSFORM (Core Logic)

#### ✔️ Data Cleaning

* Removes null values
* Filters invalid transaction types
* Removes duplicate transactions
* Removes zero/negative amounts

#### ✔️ Feature Engineering

* Adds year_month column (for monthly analysis)
* Adds signed_amount (+credit / -debit)

#### ✔️ Data Integrity

* Example:
  4913 / 5210 → 94.3% data retained

#### ✔️ Aggregation

For each account:

* Total credits
* Total debits
* Transaction count
* Average transaction amount
* Last transaction date
* Balance = credits - debits

---

### 4️⃣ LOAD

* Stores processed data into SQLite database
* Tables:

  * transactions
  * account_summary
* Creates indexes on:

  * account_id
  * transaction_type
  * timestamp

Purpose: Faster query performance

---

### 5️⃣ ANALYTICS

Runs SQL queries:

* Top 5 accounts by balance
* Monthly transaction trends
* Credit vs Debit distribution

Query performance: ~1–5 ms

---

### 6️⃣ FINAL OUTPUT

* Clean database → data/transactions.db
* Logs → logs/etl.log
* Insights printed in terminal

---

## 🎯 Summary

This project simulates a real-world ETL pipeline:

1. Generate raw data
2. Clean and validate
3. Store in database
4. Analyze using SQL

---

## 🔥 Interview One-Liner

“I built an ETL pipeline that processes raw transaction data, improves data integrity to ~94%, computes account balances, and generates insights using optimized SQL queries.”

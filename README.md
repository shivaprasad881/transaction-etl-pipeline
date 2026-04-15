# Transaction ETL Pipeline

## 📌 Overview

This project implements a complete **ETL (Extract, Transform, Load) pipeline** to process financial transaction data and generate meaningful insights.

* Extracts raw transaction data from CSV
* Cleans and validates data using Pandas
* Computes account-level summaries and balances
* Loads processed data into SQLite database
* Runs optimized SQL queries for analytics

---

## ⚙️ Tech Stack

* Python
* Pandas
* SQLite
* SQL

---

## 🚀 Key Features

* Data cleaning (null removal, duplicates, invalid records)
* Data validation and integrity checks
* Account balance computation (credit vs debit)
* Monthly transaction analysis
* Optimized queries using indexing

---

## 📊 Metrics (Real Run Results)

* Processed **5,210 transaction records**
* Cleaned dataset to **4,913 valid records**
* Achieved **94.3% data integrity**
* Removed:

  * 196 null records
  * 91 invalid transaction types
  * 10 duplicate transactions
* Generated summaries for **50 accounts**
* Pipeline execution time: **~0.28 seconds** ⚡

---

## 📈 Sample Insights

* Identified **Top 5 accounts** with balance up to **₹1.35L+**
* Monthly transaction volume analysis (credits vs debits)
* Credit vs Debit distribution:

  * ~2,487 credits vs 2,426 debits
* Query performance: **1–5 ms (optimized with indexing)**

---

## 📂 Project Structure

etl_pipeline/
│── etl_pipeline.py
│── data/
│   ├── transactions.csv
│   └── transactions.db
│── logs/
│   └── etl.log

---

## ▶️ How to Run

```bash
python etl_pipeline.py
```

---

## 🎯 Outcome

* Built a scalable ETL pipeline using Python & SQL
* Improved data quality through validation and cleaning
* Generated actionable insights from raw transaction data
* Demonstrated real-world data engineering workflow

---

## 🔗 Author

Shiva Prasad Reddy
GitHub: https://github.com/shivaprasad881

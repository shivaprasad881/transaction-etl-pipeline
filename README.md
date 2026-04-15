# Transaction ETL Pipeline

## 📌 Overview

This project implements an **ETL (Extract, Transform, Load) pipeline** to process transaction data and generate meaningful insights.

* Extracts raw transaction data from CSV
* Cleans and validates data using Pandas
* Computes account-level summaries
* Loads processed data into SQLite database
* Runs analytical SQL queries for insights

---

## ⚙️ Tech Stack

* Python
* Pandas
* SQLite
* SQL

---

## 🚀 Features

* Data cleaning and validation (nulls, duplicates, invalid values)
* Data integrity check (~% of valid records retained)
* Account balance calculation (credit vs debit)
* Monthly transaction analysis
* Optimized queries using indexing

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

## 📊 Sample Insights

* Top accounts by balance
* Monthly transaction volume
* Credit vs Debit distribution

---

## 🎯 Outcome

* Built a complete ETL pipeline using Python & SQL
* Improved data quality through validation and cleaning
* Generated actionable insights from raw transaction data

---

## 🔗 Author

Shiva Prasad Reddy
GitHub: https://github.com/shivaprasad881

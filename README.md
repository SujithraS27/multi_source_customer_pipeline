# Multi-Source Customer Pipeline

This project is an end-to-end **ETL (Extract, Transform, Load) pipeline** built with **Python**, **Pandas**, and **SQL**, using the real-world **Olist Brazilian E-Commerce dataset**.

The goal is to process customer-related data coming from multiple sources — including orders, order items, and customer information — and transform it into clean, consistent, and valuable insights.

---

## 🗂️ Project Layers: Bronze → Silver → Gold

**This pipeline follows the standard multi-layer architecture:**

- **Bronze Layer:** Raw data as received from the source (CSV files) — no modifications, just loaded as-is.
- **Silver Layer:** Cleaned and standardized data — duplicates removed, missing values handled, datatypes corrected.
- **Gold Layer:** Final aggregated data ready for analysis or reporting — built using SQL queries to join, filter, and aggregate cleaned data.

---

## ⚙️ Tools & Stack

- **Python 3.12**
- **Pandas** for data manipulation
- **MySQL + mysql.connector** (planned) for storing and transforming cleaned data
- **VS Code** for development
- **Git & GitHub** for version control and project tracking

---

## 📚 Dataset

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce):  
A public dataset containing real orders, products, payments, and customer information from a Brazilian online marketplace.

---

## ✅ Progress

### ✔️ 1. Extract

- Loaded raw datasets: `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, and `olist_customers_dataset.csv` from the `/bronze/` folder.
- Used **Pandas** `read_csv()` to read the CSV files into DataFrames.
- Verified successful loading using `.head()` to view sample rows and `.shape` to check row/column count.

---

### ✔️ 2. Inspect

- Explored basic info for each DataFrame with `.info()` to check data types and null counts.
- Used `.describe()` to understand numeric columns and basic stats.
- Identified potential nulls and duplicate rows to clean in the next step.

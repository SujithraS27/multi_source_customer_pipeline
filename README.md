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
- Checked for missing values in `orders`:
  - `order_approved_at`: 160 nulls
  - `order_delivered_carrier_date`: 1,783 nulls
  - `order_delivered_customer_date`: 2,965 nulls
- Confirmed `customers` and `order_items` have no missing values.
- Checked for duplicate rows in all three datasets using `.duplicated().sum()` — found **0 duplicates**.

---

### ✔️ 3. Clean

- Added `delivery_status` flag to `orders` to mark delivered vs. cancelled/pending.
- Kept meaningful NULLs in `order_approved_at` and `order_delivered_carrier_date` because they reflect the true state of each order.
- Verified no duplicate rows in `orders`, `customers`, and `order_items`.
- Converted all timestamp columns (`order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, etc.) to `datetime64[ns]` datatype in Pandas.
  - This ensures correct date-based filtering, sorting, and consistent loading into SQL `DATETIME` fields.
- Kept `customer_id` and other IDs as `object` type because they are alphanumeric keys, mapping to `VARCHAR` in SQL.
- Saved all cleaned files to `/silver/` layer with the correct datatypes for smooth loading into the database.

---

### ✔️ 4. Load to MySQL

- Created **`olist_db`** database in MySQL and created tables for `orders`, `customers`, and `order_items` with appropriate datatypes (`VARCHAR`, `DATETIME`, `INT`).
- Wrote Python `pipeline.py` code to **connect to MySQL** using `mysql.connector` and insert rows.
- Used **row-wise insert** logic with `iterrows()` to iterate over DataFrames and insert each record safely.
- Applied `row.where(pd.notnull(row), None)` to replace any Pandas `NaN` values with Python `None`, ensuring MySQL stores them as `NULL` and avoids `'nan'` string errors.
- Verified successful row counts in MySQL match the `/silver/` cleaned CSVs.

---

### ✅ 5. Gold Layer: Summary Tables

- Created:
  - **`customer_summary`**: total orders, total spend, first & last order date per customer.
  - **`monthly_orders_summary`**: total orders & revenue by year-month.
  - **`product_summary`**: total items sold & revenue per product.
  - **`delivery_performance`**: average delivery days per month.
- Used backticks (`) for column names that might conflict with reserved words.
- Ensured `GROUP BY` clauses align with aggregate fields.

Example:
```sql
CREATE TABLE `monthly_orders_summary` AS
SELECT
  DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS `year_month`,
  COUNT(DISTINCT o.order_id) AS `total_orders`,
  SUM(oi.price) AS `total_revenue`
FROM `orders` o
JOIN `order_items` oi ON o.order_id = oi.order_id
GROUP BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m`);
```

---

### ✅ 6. Testing & Validation

- Ran `pipeline.py` end-to-end after loading cleaned CSVs.
- Verified that all **Silver Layer tables** (`orders`, `customers`, `order_items`) were populated correctly in MySQL.
- Checked row counts in MySQL match the original cleaned CSVs.
- Validated that **Gold Layer summary tables** (`customer_summary`, `monthly_orders_summary`, `product_summary`, `delivery_performance`) are created with correct aggregate results.
- Confirmed that `NULL` values are handled as expected — no `'nan'` strings appear.
- Used MySQL CLI or GUI tools to run `SELECT *` queries to inspect the final data.
- Verified no leftover open connections — `cursor` and `conn` are closed gracefully at the end of the script.
- Ensured no syntax errors remain (used backticks for reserved keywords if needed).

---

### ✅ 7. Conclusion

This project demonstrates a complete **ETL pipeline** for customer and order data, starting from raw CSVs, performing data cleaning, transforming into curated Silver Layer datasets, and loading summarized insights into a **MySQL data warehouse**.

**Key Outcomes:**
- Clean, consistent, and validated data ready for downstream analytics.
- Silver and Gold Layer tables designed for easy reporting.
- Reproducible Python pipeline with best practices for null handling and database inserts.
- Solid foundation for adding orchestration, monitoring, and BI dashboards.

**Future Enhanacements:**
- Automate the pipeline using a scheduler like **Apache Airflow**.
- Add monitoring and logging for production.
- Connect **BI tools** (Tableau, Power BI) to create dynamic dashboards.

---

## 📚 8. Learnings

- Learned how to build an end-to-end **data pipeline** using Python, Pandas, and MySQL.
- Understood how to handle **missing values** and maintain data quality during transformation.
- Practiced writing efficient **SQL** for creating summary tables.
- Gained experience in **loading data into relational databases** using Python.
- Understood the importance of **Silver and Gold Layer architecture** for scalable analytics.

---

> ✅ *Project successfully tested and deployed locally. Ready for future enhancements!*

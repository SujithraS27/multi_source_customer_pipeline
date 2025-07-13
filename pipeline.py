<<<<<<< HEAD
import pandas as pd
=======

import pandas as pd;
orders=pd.read_csv(r"E:\DE PROJECT\multi_source_customer_pipeline\bronze\olist_orders_dataset.csv");
order_items=pd.read_csv(r"E:\DE PROJECT\multi_source_customer_pipeline\bronze\olist_order_items_dataset.csv");
customers=pd.read_csv(r"E:\DE PROJECT\multi_source_customer_pipeline\bronze\olist_customers_dataset.csv");
print(orders.head());
print(orders.info());
print(order_items.head());
print(order_items.info());
print(customers.head());
print(customers.info())
print(orders.isnull().sum())
print(order_items.isnull().sum())
print(customers.isnull().sum())
print(orders.duplicated().sum())
print(customers.duplicated().sum())
print(order_items.duplicated().sum())
print(orders.describe())
orders['delivery_status'] = orders['order_delivered_customer_date'].notnull()
print(orders.order_id)
print(customers.customer_unique_id)
print(order_items.seller_id)
orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_approved_at'] = pd.to_datetime(orders['order_approved_at'])
orders['order_delivered_carrier_date'] = pd.to_datetime(orders['order_delivered_carrier_date'])
orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])
print(orders.dtypes)
print(customers.dtypes)
order_items['shipping_limit_date'] = pd.to_datetime(order_items['shipping_limit_date'])
print(order_items.dtypes)
orders.to_csv("E:/DE PROJECT/multi_source_customer_pipeline/silver/orders_cleaned.csv", index=False)
customers.to_csv("E:/DE PROJECT/multi_source_customer_pipeline/silver/customers.csv", index=False)
order_items.to_csv("E:/DE PROJECT/multi_source_customer_pipeline/silver/order_items.csv", index=False)

#CONNECTING SQL FROM PYTHON

>>>>>>> 62c7ec3d6c7f0fce176904047213f9b1abb2e471
import mysql.connector

# ------------------------
# 1. Load Bronze data and CLEAN
# ------------------------
orders = pd.read_csv(r"E:/DE PROJECT/multi_source_customer_pipeline/bronze/olist_orders_dataset.csv")
order_items = pd.read_csv(r"E:/DE PROJECT/multi_source_customer_pipeline/bronze/olist_order_items_dataset.csv")
customers = pd.read_csv(r"E:/DE PROJECT/multi_source_customer_pipeline/bronze/olist_customers_dataset.csv")

# Remove nulls and duplicates (basic)
orders.drop_duplicates(inplace=True)
order_items.drop_duplicates(inplace=True)
customers.drop_duplicates(inplace=True)

orders['delivery_status'] = orders['order_delivered_customer_date'].notnull()

# Convert date columns
date_cols = [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
]
for col in date_cols:
    orders[col] = pd.to_datetime(orders[col])

order_items['shipping_limit_date'] = pd.to_datetime(order_items['shipping_limit_date'])

# Save to Silver
orders.to_csv(r"E:/DE PROJECT/multi_source_customer_pipeline/silver/orders_cleaned.csv", index=False)
order_items.to_csv(r"E:/DE PROJECT/multi_source_customer_pipeline/silver/order_items.csv", index=False)
customers.to_csv(r"E:/DE PROJECT/multi_source_customer_pipeline/silver/customers.csv", index=False)
print("Silver Layer CSVs saved.")

# ------------------------
# 2. Connect to MySQL
# ------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="olist_db"
)
cursor = conn.cursor()

# ------------------------
# 3. Truncate Silver tables
# ------------------------
cursor.execute("TRUNCATE TABLE orders")
cursor.execute("TRUNCATE TABLE customers")
cursor.execute("TRUNCATE TABLE order_items")
conn.commit()
print("Silver Layer tables truncated.")

# ------------------------
# 4. Insert Silver Layer data
# ------------------------
for _, row in orders.iterrows():
    row = row.where(pd.notnull(row), None)
    sql = """
    INSERT INTO orders
    (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
     order_delivered_carrier_date, order_delivered_customer_date,
     order_estimated_delivery_date, delivery_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, tuple(row))
print("Orders insert done.")

for _, row in customers.iterrows():
    row = row.where(pd.notnull(row), None)
    sql = """
    INSERT INTO customers
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state)
    VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(sql, tuple(row))
print("Customers insert done.")

for _, row in order_items.iterrows():
    row = row.where(pd.notnull(row), None)
    sql = """
    INSERT INTO order_items
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, tuple(row))
print("Order_items insert done.")

conn.commit()
print("Silver Layer data loaded.")

# ------------------------
# 5. Gold Layer tables
# ------------------------

## 5.1 Customer Summary
cursor.execute("DROP TABLE IF EXISTS customer_summary;")
cursor.execute("""
    CREATE TABLE customer_summary (
        customer_id VARCHAR(50),
        total_orders INT,
        total_spend DECIMAL(10,2),
        first_order_date DATETIME,
        last_order_date DATETIME
    );
""")
cursor.execute("""
    INSERT INTO customer_summary (customer_id, total_orders, total_spend, first_order_date, last_order_date)
    SELECT
      c.customer_id,
      COUNT(DISTINCT o.order_id) AS total_orders,
      SUM(oi.price) AS total_spend,
      MIN(o.order_purchase_timestamp) AS first_order_date,
      MAX(o.order_purchase_timestamp) AS last_order_date
    FROM
      orders o
      JOIN order_items oi ON o.order_id = oi.order_id
      JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY
      c.customer_id;
""")
print("customer_summary created.")

## 5.2 Monthly Orders Summary
cursor.execute("DROP TABLE IF EXISTS monthly_orders_summary;")
cursor.execute("""
    CREATE TABLE monthly_orders_summary (
        `year_month` VARCHAR(7),
        total_orders INT,
        total_revenue DECIMAL(10,2)
    );
""")
cursor.execute("""
    INSERT INTO monthly_orders_summary (`year_month`, total_orders, total_revenue)
    SELECT
      DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS `year_month`,
      COUNT(DISTINCT o.order_id) AS total_orders,
      SUM(oi.price) AS total_revenue
    FROM
      orders o
      JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY
      DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m');
""")
print("monthly_orders_summary created.")

## 5.3 Product Summary
cursor.execute("DROP TABLE IF EXISTS product_summary;")
cursor.execute("""
    CREATE TABLE product_summary (
        product_id VARCHAR(50),
        total_items_sold INT,
        total_revenue DECIMAL(10,2)
    );
""")
cursor.execute("""
    INSERT INTO product_summary (product_id, total_items_sold, total_revenue)
    SELECT
      product_id,
      COUNT(*) AS total_items_sold,
      SUM(price) AS total_revenue
    FROM
      order_items
    GROUP BY product_id;
""")
print("product_summary created.")

## 5.4 Delivery Performance
cursor.execute("DROP TABLE IF EXISTS delivery_performance;")
cursor.execute("""
    CREATE TABLE delivery_performance (
        `year_month` VARCHAR(7),
        avg_delivery_days DECIMAL(5,2)
    );
""")
cursor.execute("""
    INSERT INTO delivery_performance (`year_month`, avg_delivery_days)
    SELECT
      DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS `year_month`,
      AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) AS avg_delivery_days
    FROM
      orders
    WHERE order_delivered_customer_date IS NOT NULL
    GROUP BY
      DATE_FORMAT(order_purchase_timestamp, '%Y-%m');
""")
print("delivery_performance created.")

# ------------------------
# 6. Done
# ------------------------
conn.commit()
cursor.close()
conn.close()
print("Pipeline completed successfully.")

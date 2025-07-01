
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

import mysql.connector

# 1. Load cleaned CSV
orders = pd.read_csv(r"E:/DE PROJECT/multi_source_customer_pipeline/silver/orders_cleaned.csv")


# 2. Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="olist_db"
)
cursor = conn.cursor()

# 3. Loop through rows and insert
for _, row in orders.iterrows():
    row = row.where(pd.notnull(row), None)
    sql = """
    INSERT INTO orders
    (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
     order_delivered_carrier_date, order_delivered_customer_date,
     order_estimated_delivery_date, delivery_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = tuple(row)
    cursor.execute(sql, values)
print("Orders insert done.")

for _, row in customers.iterrows():
    row = row.where(pd.notnull(row), None)
    sql = """
    INSERT INTO customers
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state)
    VALUES (%s, %s, %s, %s, %s)
    """
    values = tuple(row)
    cursor.execute(sql, values)
print("Customers insert done.")

for _, row in order_items.iterrows():
    row = row.where(pd.notnull(row), None)
    sql = """
    INSERT INTO order_items
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = tuple(row)
    cursor.execute(sql, values)
print("Order_items insert done.")


conn.commit()
conn.close()

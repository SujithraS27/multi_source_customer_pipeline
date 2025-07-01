
import pandas as pd;
orders=pd.read_csv(r"E:\DE PROJECT\multi_source_customer_pipeline\bronze\olist_orders_dataset.csv");
order_items=pd.read_csv(r"E:\DE PROJECT\multi_source_customer_pipeline\bronze\olist_order_items_dataset.csv");
customers=pd.read_csv(r"E:\DE PROJECT\multi_source_customer_pipeline\bronze\olist_customers_dataset.csv");
'''print(orders.head());
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
print(orders.describe())'''
orders['delivery_status'] = orders['order_delivered_customer_date'].notnull()
print(orders.order_delivered_carrier_date)
orders.to_csv("E:/DE PROJECT/multi_source_customer_pipeline/silver/orders_cleaned.csv", index=False)
customers.to_csv("E:/DE PROJECT/multi_source_customer_pipeline/silver/customers.csv", index=False)
order_items.to_csv("E:/DE PROJECT/multi_source_customer_pipeline/silver/order_items.csv", index=False)


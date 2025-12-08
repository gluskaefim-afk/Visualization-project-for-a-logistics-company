import pandas as pd
import psycopg2


df = pd.read_csv('C:/Users/1/Downloads/slovakia_dataset_1000.csv', encoding='utf-8')


numeric_cols = ['Unit Price', 'Profit', 'Quantity ordered new', 'Sales']
for col in numeric_cols:
    df[col] = df[col].astype(str)
    df[col] = df[col].str.replace(r'[^0-9\.-]', '', regex=True)
    df[col] = pd.to_numeric(df[col], errors='coerce')

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="SECRET",
    port=5432
)

cur = conn.cursor()


cur.execute("""
CREATE TABLE IF NOT EXISTS sales_data (
    id SERIAL PRIMARY KEY,
    unit_price NUMERIC,
    customer_id INTEGER,
    customer_name VARCHAR(255),
    customer_segment VARCHAR(100),
    product_category VARCHAR(100),
    product_sub_category VARCHAR(100),
    product_name VARCHAR(255),
    city VARCHAR(100),
    order_weekday VARCHAR(20),
    weekday_helper INTEGER,
    profit NUMERIC,
    quantity_ordered INTEGER,
    sales NUMERIC,
    order_id INTEGER,
    order_returned BOOLEAN,
    order_date DATE
)
""")
conn.commit()

for i, row in df.iterrows():
    cur.execute("""
        INSERT INTO sales_data (
            unit_price, customer_id, customer_name, customer_segment,
            product_category, product_sub_category, product_name, city,
            order_weekday, weekday_helper, profit, quantity_ordered,
            sales, order_id, order_returned, order_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Unit Price'], row['Customer ID'], row['Customer Name'], row['Customer Segment'],
        row['Product Category'], row['Product Sub-Category'], row['Product Name'], row['City'],
        row['Order Weekday'], row['Weekday helper'], row['Profit'], row['Quantity ordered new'],
        row['Sales'], row['Order ID'], row['Order returned'], row['Order Date']
    ))
conn.commit()

cur.close()
conn.close()

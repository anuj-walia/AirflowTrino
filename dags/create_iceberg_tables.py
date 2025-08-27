"""
Airflow DAG to create Iceberg tables using Trino.
This DAG demonstrates creating multiple Iceberg tables with different schemas.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.empty import EmptyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'create_iceberg_tables',
    default_args=default_args,
    description='Create Iceberg tables using Trino',
    schedule_interval='@once',  # Run once, can be changed to cron expression
    catchup=False,
    tags=['iceberg', 'trino', 'table-creation'],
)

# Start task
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Create catalog and schema if not exists
create_catalog_schema = TrinoOperator(
    task_id='create_catalog_schema',
    sql="""
    CREATE SCHEMA IF NOT EXISTS memory.analytics
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Create customers table
create_customers_table = TrinoOperator(
    task_id='create_customers_table',
    sql="""
    CREATE TABLE IF NOT EXISTS memory.analytics.customers (
        customer_id BIGINT,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        phone VARCHAR(20),
        address VARCHAR(200),
        city VARCHAR(50),
        state VARCHAR(50),
        zip_code VARCHAR(10),
        country VARCHAR(50),
        registration_date DATE,
        last_login_timestamp TIMESTAMP,
        is_active BOOLEAN,
        customer_segment VARCHAR(20)
    )
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Create orders table
create_orders_table = TrinoOperator(
    task_id='create_orders_table',
    sql="""
    CREATE TABLE IF NOT EXISTS memory.analytics.orders (
        order_id BIGINT,
        customer_id BIGINT,
        order_date DATE,
        order_timestamp TIMESTAMP,
        total_amount DECIMAL(10,2),
        currency VARCHAR(3),
        order_status VARCHAR(20),
        payment_method VARCHAR(30),
        shipping_address VARCHAR(200),
        shipping_city VARCHAR(50),
        shipping_state VARCHAR(50),
        shipping_country VARCHAR(50),
        discount_amount DECIMAL(10,2),
        tax_amount DECIMAL(10,2),
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Create products table
create_products_table = TrinoOperator(
    task_id='create_products_table',
    sql="""
    CREATE TABLE IF NOT EXISTS memory.analytics.products (
        product_id BIGINT,
        product_name VARCHAR(100),
        category VARCHAR(50),
        subcategory VARCHAR(50),
        brand VARCHAR(50),
        price DECIMAL(10,2),
        cost DECIMAL(10,2),
        weight DECIMAL(8,2),
        dimensions VARCHAR(50),
        color VARCHAR(30),
        size VARCHAR(20),
        material VARCHAR(50),
        description VARCHAR(500),
        is_available BOOLEAN,
        stock_quantity INTEGER,
        created_date DATE,
        last_updated TIMESTAMP
    )
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Create order_items table
create_order_items_table = TrinoOperator(
    task_id='create_order_items_table',
    sql="""
    CREATE TABLE IF NOT EXISTS memory.analytics.order_items (
        order_item_id BIGINT,
        order_id BIGINT,
        product_id BIGINT,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        total_price DECIMAL(10,2),
        discount_percentage DECIMAL(5,2),
        created_at TIMESTAMP
    )
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Insert sample data into customers table
insert_sample_customers = TrinoOperator(
    task_id='insert_sample_customers',
    sql="""
    INSERT INTO memory.analytics.customers VALUES
    (1, 'John', 'Doe', 'john.doe@email.com', '+1-555-0101', '123 Main St', 'New York', 'NY', '10001', 'USA', DATE '2023-01-15', TIMESTAMP '2024-01-15 10:30:00', true, 'Premium'),
    (2, 'Jane', 'Smith', 'jane.smith@email.com', '+1-555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'USA', DATE '2023-02-20', TIMESTAMP '2024-01-14 15:45:00', true, 'Standard'),
    (3, 'Bob', 'Johnson', 'bob.johnson@email.com', '+1-555-0103', '789 Pine Rd', 'Chicago', 'IL', '60601', 'USA', DATE '2023-03-10', TIMESTAMP '2024-01-13 09:15:00', false, 'Basic'),
    (4, 'Alice', 'Brown', 'alice.brown@email.com', '+44-20-7946-0958', '10 Downing St', 'London', 'England', 'SW1A 2AA', 'UK', DATE '2023-04-05', TIMESTAMP '2024-01-12 14:20:00', true, 'Premium'),
    (5, 'Charlie', 'Wilson', 'charlie.wilson@email.com', '+33-1-42-86-83-26', '1 Rue de Rivoli', 'Paris', 'Île-de-France', '75001', 'France', DATE '2023-05-12', TIMESTAMP '2024-01-11 11:00:00', true, 'Standard')
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Insert sample data into products table
insert_sample_products = TrinoOperator(
    task_id='insert_sample_products',
    sql="""
    INSERT INTO memory.analytics.products VALUES
    (1, 'Wireless Headphones', 'Electronics', 'Audio', 'TechBrand', 99.99, 45.00, 0.25, '20x15x8 cm', 'Black', 'One Size', 'Plastic', 'High-quality wireless headphones with noise cancellation', true, 150, DATE '2023-01-01', TIMESTAMP '2024-01-15 12:00:00'),
    (2, 'Running Shoes', 'Footwear', 'Athletic', 'SportsBrand', 129.99, 60.00, 0.80, '30x20x12 cm', 'Blue', '10', 'Synthetic', 'Comfortable running shoes for daily training', true, 75, DATE '2023-01-15', TIMESTAMP '2024-01-14 10:30:00'),
    (3, 'Coffee Maker', 'Appliances', 'Kitchen', 'HomeBrand', 199.99, 90.00, 3.50, '35x25x40 cm', 'Silver', 'Large', 'Stainless Steel', 'Programmable coffee maker with timer', true, 30, DATE '2023-02-01', TIMESTAMP '2024-01-13 14:15:00'),
    (4, 'Laptop Backpack', 'Accessories', 'Bags', 'TravelBrand', 79.99, 35.00, 1.20, '45x30x15 cm', 'Gray', 'Large', 'Nylon', 'Durable laptop backpack with multiple compartments', true, 200, DATE '2023-02-15', TIMESTAMP '2024-01-12 16:45:00'),
    (5, 'Smartphone Case', 'Electronics', 'Accessories', 'TechBrand', 24.99, 8.00, 0.05, '15x8x1 cm', 'Clear', 'Universal', 'Silicone', 'Protective smartphone case with drop protection', true, 500, DATE '2023-03-01', TIMESTAMP '2024-01-11 09:20:00')
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# End task
end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> create_catalog_schema
create_catalog_schema >> [create_customers_table, create_products_table, create_orders_table, create_order_items_table]
create_customers_table >> insert_sample_customers
create_products_table >> insert_sample_products
[insert_sample_customers, insert_sample_products, create_orders_table, create_order_items_table] >> end_task

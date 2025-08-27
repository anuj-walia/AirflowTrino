"""
Airflow DAG to read and analyze data from multiple Iceberg tables using Trino.
This DAG demonstrates various data reading and analysis operations across Iceberg tables.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'read_iceberg_tables',
    default_args=default_args,
    description='Read and analyze data from multiple Iceberg tables using Trino',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['iceberg', 'trino', 'data-analysis', 'reporting'],
)

def log_analysis_start():
    """Log the start of data analysis"""
    logging.info("Starting comprehensive data analysis across Iceberg tables")
    return "Analysis started successfully"

# Start task
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Log analysis start
log_start = PythonOperator(
    task_id='log_analysis_start',
    python_callable=log_analysis_start,
    dag=dag,
)

# Read customer data with basic statistics
analyze_customers = TrinoOperator(
    task_id='analyze_customers',
    sql="""
    SELECT 
        'Customer Analysis' as analysis_type,
        COUNT(*) as total_customers,
        COUNT(DISTINCT country) as countries_count,
        COUNT(CASE WHEN is_active = true THEN 1 END) as active_customers,
        COUNT(CASE WHEN customer_segment = 'Premium' THEN 1 END) as premium_customers,
        MIN(registration_date) as earliest_registration,
        MAX(registration_date) as latest_registration,
        CURRENT_TIMESTAMP as analysis_timestamp
    FROM memory.analytics.customers
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Read product data with inventory analysis
analyze_products = TrinoOperator(
    task_id='analyze_products',
    sql="""
    SELECT 
        'Product Analysis' as analysis_type,
        COUNT(*) as total_products,
        COUNT(DISTINCT category) as categories_count,
        COUNT(DISTINCT brand) as brands_count,
        COUNT(CASE WHEN is_available = true THEN 1 END) as available_products,
        SUM(stock_quantity) as total_stock,
        AVG(price) as average_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        CURRENT_TIMESTAMP as analysis_timestamp
    FROM memory.analytics.products
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Customer segmentation analysis
customer_segmentation = TrinoOperator(
    task_id='customer_segmentation',
    sql="""
    SELECT 
        customer_segment,
        country,
        COUNT(*) as customer_count,
        COUNT(CASE WHEN is_active = true THEN 1 END) as active_count,
        ROUND(COUNT(CASE WHEN is_active = true THEN 1 END) * 100.0 / COUNT(*), 2) as active_percentage
    FROM memory.analytics.customers
    GROUP BY customer_segment, country
    ORDER BY customer_segment, country
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Product category analysis
product_category_analysis = TrinoOperator(
    task_id='product_category_analysis',
    sql="""
    SELECT 
        category,
        subcategory,
        COUNT(*) as product_count,
        SUM(stock_quantity) as total_stock,
        AVG(price) as avg_price,
        SUM(stock_quantity * price) as inventory_value,
        COUNT(CASE WHEN is_available = true THEN 1 END) as available_products
    FROM memory.analytics.products
    GROUP BY category, subcategory
    ORDER BY inventory_value DESC
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Cross-table analysis: Customer and Product insights
customer_product_insights = TrinoOperator(
    task_id='customer_product_insights',
    sql="""
    WITH customer_stats AS (
        SELECT 
            country,
            customer_segment,
            COUNT(*) as customers_in_segment
        FROM memory.analytics.customers
        WHERE is_active = true
        GROUP BY country, customer_segment
    ),
    product_stats AS (
        SELECT 
            category,
            COUNT(*) as products_in_category,
            AVG(price) as avg_category_price
        FROM memory.analytics.products
        WHERE is_available = true
        GROUP BY category
    )
    SELECT 
        'Cross Analysis' as analysis_type,
        cs.country,
        cs.customer_segment,
        cs.customers_in_segment,
        ps.category,
        ps.products_in_category,
        ROUND(ps.avg_category_price, 2) as avg_category_price,
        CURRENT_TIMESTAMP as analysis_timestamp
    FROM customer_stats cs
    CROSS JOIN product_stats ps
    WHERE cs.customers_in_segment > 0 AND ps.products_in_category > 0
    ORDER BY cs.country, cs.customer_segment, ps.category
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Time-based analysis
time_based_analysis = TrinoOperator(
    task_id='time_based_analysis',
    sql="""
    WITH monthly_registrations AS (
        SELECT 
            DATE_TRUNC('month', registration_date) as month,
            COUNT(*) as new_customers,
            COUNT(CASE WHEN customer_segment = 'Premium' THEN 1 END) as premium_customers
        FROM memory.analytics.customers
        GROUP BY DATE_TRUNC('month', registration_date)
    ),
    product_creation_trends AS (
        SELECT 
            DATE_TRUNC('month', created_date) as month,
            COUNT(*) as new_products,
            AVG(price) as avg_price_new_products
        FROM memory.analytics.products
        GROUP BY DATE_TRUNC('month', created_date)
    )
    SELECT 
        COALESCE(mr.month, pct.month) as month,
        COALESCE(mr.new_customers, 0) as new_customers,
        COALESCE(mr.premium_customers, 0) as premium_customers,
        COALESCE(pct.new_products, 0) as new_products,
        ROUND(COALESCE(pct.avg_price_new_products, 0), 2) as avg_price_new_products
    FROM monthly_registrations mr
    FULL OUTER JOIN product_creation_trends pct ON mr.month = pct.month
    ORDER BY month
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Data quality checks
data_quality_checks = TrinoOperator(
    task_id='data_quality_checks',
    sql="""
    WITH customer_quality AS (
        SELECT 
            'customers' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_ids,
            COUNT(CASE WHEN email IS NULL OR email = '' THEN 1 END) as missing_emails,
            COUNT(CASE WHEN email NOT LIKE '%@%' THEN 1 END) as invalid_emails,
            COUNT(DISTINCT customer_id) as unique_ids
        FROM memory.analytics.customers
    ),
    product_quality AS (
        SELECT 
            'products' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN product_id IS NULL THEN 1 END) as null_ids,
            COUNT(CASE WHEN price <= 0 THEN 1 END) as invalid_prices,
            COUNT(CASE WHEN stock_quantity < 0 THEN 1 END) as negative_stock,
            COUNT(DISTINCT product_id) as unique_ids
        FROM memory.analytics.products
    )
    SELECT * FROM customer_quality
    UNION ALL
    SELECT 
        table_name,
        total_records,
        null_ids,
        invalid_prices as missing_emails,  -- Reusing column for different metrics
        negative_stock as invalid_emails,  -- Reusing column for different metrics
        unique_ids
    FROM product_quality
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Generate summary report
generate_summary_report = TrinoOperator(
    task_id='generate_summary_report',
    sql="""
    WITH summary_stats AS (
        SELECT 
            'Summary Report' as report_type,
            (SELECT COUNT(*) FROM memory.analytics.customers) as total_customers,
            (SELECT COUNT(*) FROM memory.analytics.products) as total_products,
            (SELECT COUNT(DISTINCT country) FROM memory.analytics.customers) as countries_served,
            (SELECT COUNT(DISTINCT category) FROM memory.analytics.products) as product_categories,
            CURRENT_TIMESTAMP as report_generated_at
    )
    SELECT 
        report_type,
        total_customers,
        total_products,
        countries_served,
        product_categories,
        report_generated_at,
        'Data analysis completed successfully across all Iceberg tables' as status
    FROM summary_stats
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
start_task >> log_start
log_start >> [analyze_customers, analyze_products]
analyze_customers >> customer_segmentation
analyze_products >> product_category_analysis
[customer_segmentation, product_category_analysis] >> customer_product_insights
customer_product_insights >> time_based_analysis
time_based_analysis >> data_quality_checks
data_quality_checks >> generate_summary_report
generate_summary_report >> end_task
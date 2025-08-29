"""
Airflow DAG to test basic Trino connectivity using Memory catalog.
This is a simplified version that tests the Trino connection without Iceberg.
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
    'test_trino_connection',
    default_args=default_args,
    description='Test Trino connection using Memory catalog',
    schedule_interval='@once',  # Run once, can be changed to cron expression
    catchup=False,
    tags=['trino', 'test'],
)

# Start task
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Test basic Trino connectivity
test_connection = TrinoOperator(
    task_id='test_connection',
    sql="SELECT 'Hello Trino!' as message",
    trino_conn_id='trino_default',
    dag=dag,
)

# Show available catalogs
show_catalogs = TrinoOperator(
    task_id='show_catalogs',
    sql="SHOW CATALOGS",
    trino_conn_id='trino_default',
    dag=dag,
)

# Create a simple table in memory catalog
create_test_table = TrinoOperator(
    task_id='create_test_table',
    sql="""
    CREATE TABLE iceberg.default.test_table (
        id INTEGER,
        name VARCHAR(50),
        created_at TIMESTAMP
    )
    """,
    trino_conn_id='trino_default',
    dag=dag,
)

# Insert test data
insert_test_data = TrinoOperator(
    task_id='insert_test_data',
    sql="""
    INSERT INTO iceberg.default.test_table VALUES
    (1, 'Alice', TIMESTAMP '2025-08-27 10:00:00'),
    (2, 'Bob', TIMESTAMP '2025-08-27 11:00:00'),
    (3, 'Charlie', TIMESTAMP '2025-08-27 12:00:00')
    """,
    trino_conn_id='trino_default',
    dag=dag,
)


query_test_data = TrinoOperator(
    task_id='query_test_data',
    sql="CREATE SCHEMA IF NOT EXISTS iceberg.analytics",
    trino_conn_id='trino_default',
    dag=dag,
)

# End task
end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> [test_connection, show_catalogs]
[test_connection, show_catalogs] >> create_test_table
create_test_table >> insert_test_data >> query_test_data >> end_task

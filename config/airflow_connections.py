"""
Script to set up Airflow connections for Trino.
Run this script after setting up Airflow to configure the necessary connections.
"""

from airflow.models import Connection
from airflow.utils.db import create_session

def create_trino_connection():
    """Create Trino connection in Airflow"""
    
    # Trino connection configuration
    trino_conn = Connection(
        conn_id='trino_default',
        conn_type='trino',
        host='trino-coordinator',  # Updated to use Docker service name
        port=8080,         # Update with your Trino coordinator port
        login='admin',     # Update with your Trino username
        password='',       # Update with password if required
        schema='memory',  # Default schema
        extra={
            'catalog': 'memory',
            'http_scheme': 'http',  # Use 'https' if SSL is enabled
            'verify': False,        # Set to True for SSL verification
        }
    )
    
    # Add connection to Airflow
    with create_session() as session:
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'trino_default'
        ).first()
        
        if existing_conn:
            print("Trino connection already exists. Updating...")
            existing_conn.host = trino_conn.host
            existing_conn.port = trino_conn.port
            existing_conn.login = trino_conn.login
            existing_conn.password = trino_conn.password
            existing_conn.schema = trino_conn.schema
            existing_conn.extra = trino_conn.extra
        else:
            print("Creating new Trino connection...")
            session.add(trino_conn)
        
        session.commit()
        print("Trino connection configured successfully!")

if __name__ == "__main__":
    create_trino_connection()
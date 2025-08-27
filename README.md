# Airflow Trino Iceberg Integration

This project demonstrates how to create and manage Apache Iceberg tables using Apache Airflow and Trino. It includes two main DAGs:

1. **create_iceberg_tables.py** - Creates Iceberg tables and populates them with sample data
2. **read_iceberg_tables.py** - Reads and analyzes data from multiple Iceberg tables

## Project Structure

```
AirflowTrino/
├── dags/
│   ├── create_iceberg_tables.py    # DAG to create Iceberg tables
│   └── read_iceberg_tables.py      # DAG to read and analyze data
├── config/
│   └── airflow_connections.py      # Script to configure Airflow connections
├── trino/
│   └── coordinator/
│       ├── config.properties       # Trino coordinator configuration
│       ├── jvm.config             # JVM settings for Trino
│       └── catalog/
│           └── iceberg.properties  # Iceberg catalog configuration
├── docker-compose.yml             # Docker setup for Airflow, Trino, and MinIO
├── requirements.txt               # Python dependencies
└── README.md                     # This file
```

## Features

### Create Iceberg Tables DAG
- Creates a schema in the Iceberg catalog
- Creates multiple tables: customers, orders, products, order_items
- Uses proper partitioning strategies for performance
- Inserts sample data for testing
- Demonstrates table relationships and data types

### Read Iceberg Tables DAG
- Performs comprehensive data analysis across multiple tables
- Customer segmentation analysis
- Product category analysis
- Cross-table insights and correlations
- Time-based trend analysis
- Data quality checks
- Generates summary reports

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- At least 4GB RAM available for Docker

## Quick Start

### 1. Clone and Setup

```bash
git clone <your-repo>
cd AirflowTrino
```

### 2. Start Services

```bash
# Set Airflow UID (Linux/Mac)
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Start all services
docker-compose up -d
```

### 3. Access Services

- **Airflow Web UI**: http://localhost:8080 (admin/airflow)
- **Trino Web UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)

### 4. Configure Airflow Connection

Once Airflow is running, configure the Trino connection:

```bash
# Access Airflow container
docker-compose exec airflow-webserver bash

# Run connection setup script
python /opt/airflow/config/airflow_connections.py
```

Alternatively, configure via Airflow Web UI:
- Go to Admin > Connections
- Add new connection with ID: `trino_default`
- Connection Type: Trino
- Host: `trino-coordinator`
- Port: `8080`
- Login: `admin`
- Schema: `iceberg`

## Usage

### Running the DAGs

1. **Create Tables First**: Run the `create_iceberg_tables` DAG to set up your tables and sample data
2. **Analyze Data**: Run the `read_iceberg_tables` DAG to perform analysis

### Customization

#### Modifying Table Schemas
Edit the SQL statements in `create_iceberg_tables.py` to customize:
- Table schemas and data types
- Partitioning strategies
- Sample data

#### Adding New Analysis
Extend `read_iceberg_tables.py` with additional analysis tasks:
- Add new TrinoOperator tasks
- Create custom analysis queries
- Add data export functionality

#### Storage Configuration
Update `trino/coordinator/catalog/iceberg.properties` to use different storage:
- AWS S3
- Google Cloud Storage
- Azure Blob Storage
- HDFS

## Table Schemas

### Customers Table
- customer_id, personal info, location data
- Partitioned by: country, registration_date

### Products Table
- product_id, details, pricing, inventory
- Partitioned by: category, created_date

### Orders Table
- order_id, customer_id, order details, shipping
- Partitioned by: order_date, shipping_country

### Order Items Table
- order_item_id, order_id, product_id, quantities
- Partitioned by: created_at

## Monitoring and Troubleshooting

### Check Service Status
```bash
docker-compose ps
```

### View Logs
```bash
# Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Trino logs
docker-compose logs trino-coordinator

# MinIO logs
docker-compose logs minio
```

### Common Issues

1. **Connection Refused**: Ensure all services are running and healthy
2. **Memory Issues**: Increase Docker memory allocation
3. **Permission Errors**: Check AIRFLOW_UID environment variable

## Advanced Configuration

### Production Setup
For production deployment:
1. Use external metadata store (PostgreSQL/MySQL)
2. Configure SSL/TLS
3. Set up proper authentication
4. Use external object storage
5. Configure monitoring and alerting

### Scaling
- Add Trino worker nodes
- Use Celery executor for Airflow
- Implement proper resource management

## Development

### Adding New DAGs
1. Create new Python files in the `dags/` directory
2. Follow Airflow DAG best practices
3. Use the existing connection configuration

### Testing
```bash
# Test DAG syntax
docker-compose exec airflow-webserver airflow dags test create_iceberg_tables

# Test individual tasks
docker-compose exec airflow-webserver airflow tasks test create_iceberg_tables create_customers_table 2024-01-01
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review Airflow and Trino documentation
3. Open an issue in the repository
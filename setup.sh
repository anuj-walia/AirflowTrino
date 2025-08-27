#!/bin/bash

# Setup script for Airflow Trino Iceberg project

echo "🚀 Setting up Airflow Trino Iceberg project..."

# Create .env file with AIRFLOW_UID
echo "📝 Creating .env file..."
echo "AIRFLOW_UID=$(id -u)" > .env

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p logs plugins

# Set permissions
echo "🔐 Setting permissions..."
chmod +x setup.sh

# Start services
echo "🐳 Starting Docker services..."
docker-compose up -d

echo "⏳ Waiting for services to start..."
sleep 30

# Check service status
echo "🔍 Checking service status..."
docker-compose ps

echo "✅ Setup complete!"
echo ""
echo "🌐 Access URLs:"
echo "   Airflow Web UI: http://localhost:8080 (admin/airflow)"
echo "   Trino Web UI: http://localhost:8081"
echo "   MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "📋 Next steps:"
echo "1. Wait for all services to be healthy"
echo "2. Configure Trino connection in Airflow"
echo "3. Run the 'create_iceberg_tables' DAG first"
echo "4. Then run the 'read_iceberg_tables' DAG"
echo ""
echo "🔧 To configure Airflow connection:"
echo "   docker-compose exec airflow-webserver python /opt/airflow/config/airflow_connections.py"
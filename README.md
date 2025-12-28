# Twitter Data Streaming Pipeline

A real-time data pipeline that simulates Twitter data generation and streams it through a modern big data architecture using Change Data Capture (CDC) and Apache Spark.

## Architecture Overview

**Data Flow:**
```
PostgreSQL → Debezium (CDC) → Kafka → Spark Streaming → MinIO (Data Lake)
```

**Components:**
- **Data Generator**: Python app generates realistic Twitter data every X seconds
- **PostgreSQL**: Transactional database with logical replication enabled
- **Debezium**: Captures database changes in real-time (CDC)
- **Apache Kafka**: Distributed streaming platform (KRaft mode, no Zookeeper)
- **Apache Spark**: Processes streams and writes to data lake
- **MinIO**: S3-compatible object storage for Parquet files
- **pgAdmin**: PostgreSQL management UI
- **Kafka UI**: Kafka topic monitoring

## Prerequisites

Before running, create environment files from templates:

```powershell
# Database configuration
Copy-Item "db\.env.template" "db\.env"

# Application configuration  
Copy-Item "business_system\.env.template" "business_system\.env"

# Spark configuration
Copy-Item "spark\.env.template" "spark\.env"

# MinIO configuration
Copy-Item "minio\.env.template" "minio\.env"
```

Ensure credentials match between related services.

## Quick Start

```bash
# Start the entire pipeline
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker logs -f spark-app
docker logs -f twitter_data_app
```

## Accessing Services

- **pgAdmin**: http://localhost:8080
- **Kafka UI**: http://localhost:8081
- **Spark Master UI**: http://localhost:8082
- **Debezium Connect**: http://localhost:8083
- **Spark Worker UI**: http://localhost:8084
- **MinIO Console**: http://localhost:9001

## Data Flow Details

1. **Generation**: Python app inserts tweets/users into PostgreSQL every 5 seconds
2. **Capture**: Debezium captures changes via logical replication
3. **Streaming**: Changes published to Kafka topics (`twitter.tweets`, `twitter.users`)
4. **Processing**: Spark reads from Kafka topic `twitter.tweets`, parses Debezium CDC format
5. **Storage**: Parquet files written to MinIO bucket `spark-output/tweets`

## Project Structure

```
├── business_system/      # Twitter data generator
├── db/                   # PostgreSQL schemas and config
├── debezium/            # CDC connector configuration
├── spark/app/           # Spark streaming job
├── minio/               # MinIO bucket setup
├── data/                # Container volumes (gitignored)
└── docker-compose.yml   # Complete stack orchestration
```

## Technologies

- Python 3.11, APScheduler, Faker, psycopg2
- PostgreSQL 15
- Apache Kafka 3.8.0 (KRaft mode)
- Debezium Connect 2.7.3
- Apache Spark 3.5.1 with Structured Streaming
- MinIO (S3-compatible storage)

### Prerequisites
- Docker and Docker Compose installed on your system
- Git (to clone the repository)

## Monitoring and Troubleshooting

### View Application Logs
```bash
docker-compose logs twitter_app
```

### Check Database Status
```bash
docker-compose logs postgres
```

### Access PostgreSQL directly
```bash
docker-compose exec postgres psql -U postgres -d twitter_db
```

### Stop the application
```bash
docker-compose down
```

### Stop and remove all data (complete cleanup)
```bash
# Stop containers and remove volumes
docker-compose down

# Remove the local data directory to completely clean up
Remove-Item -Recurse -Force data  # Windows
# or rm -rf data  # Linux/Mac

# This will completely reset the database and pgAdmin settings
```

### Remove just the database data (keep pgAdmin settings)
```bash
docker-compose down
Remove-Item -Recurse -Force data\postgres  # Windows
# or rm -rf data/postgres  # Linux/Mac
```
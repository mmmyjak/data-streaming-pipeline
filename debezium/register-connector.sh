#!/bin/bash

# Load environment variables from .env file
if [ -f /opt/debezium/.env ]; then
  export $(grep -v '^#' /opt/debezium/.env | xargs)
fi

# Wait for Debezium Connect to be ready
echo "Waiting for Debezium Connect to be ready..."
until curl -f http://debezium:8083/connectors 2>/dev/null; do
  echo "Waiting for Debezium Connect..."
  sleep 5
done

echo "Debezium Connect is ready!"

# Check if connector already exists
if curl -f http://debezium:8083/connectors/twitter-postgres-connector 2>/dev/null; then
  echo "Connector already exists, deleting it first..."
  curl -X DELETE http://debezium:8083/connectors/twitter-postgres-connector
  sleep 2
fi

# Create connector config with substituted environment variables
cat > /tmp/connector-config.json <<EOF
{
  "name": "twitter-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "${POSTGRES_USER}",
    "database.password": "${POSTGRES_PASSWORD}",
    "database.dbname": "${POSTGRES_DB}",
    "topic.prefix": "twitter_postgres",
    "table.include.list": "public.users,public.tweets",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "debezium_pub",
    "publication.autocreate.mode": "filtered",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "twitter.schema.history",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "([^.]+)\\\\.([^.]+)\\\\.([^.]+)",
    "transforms.route.replacement": "twitter.\$3"
  }
}
EOF

# Register the PostgreSQL connector
echo "Registering PostgreSQL connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://debezium:8083/connectors/ \
  -d @/tmp/connector-config.json

echo "Connector registration completed!"

# Show connector status (without jq for simplicity)
echo "Connector status:"
curl -s http://debezium:8083/connectors/twitter-postgres-connector/status
#!/bin/sh
set -e

# Wait for MinIO to be ready by checking the server alias
until mc alias set myminio http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  echo "Waiting for MinIO..."
  sleep 2
done

mc mb -p myminio/spark-output || true

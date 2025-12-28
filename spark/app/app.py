from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, BooleanType, ArrayType
import os

# 1) Create Spark session with MinIO (S3) configuration
spark = (
    SparkSession.builder
    .appName("KafkaToMinIO_Tweets")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2) Define schema for Debezium CDC format - the "after" field contains tweet data
after_schema = StructType([
    StructField("tweet_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("content", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("mentions", ArrayType(StringType()), True),
    StructField("likes_count", IntegerType(), True),
    StructField("retweets_count", IntegerType(), True),
    StructField("replies_count", IntegerType(), True),
    StructField("reply_to_tweet_id", IntegerType(), True),
    StructField("is_retweet", BooleanType(), True),
    StructField("original_tweet_id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True)
])

debezium_schema = StructType([
    StructField("after", after_schema, True),
    StructField("op", StringType(), True)
])

# 3) Read stream from Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "twitter.tweets")
    .option("startingOffsets", "earliest")
    .load()
)

# 4) Parse the Kafka value (Debezium CDC format) and extract the "after" field
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), debezium_schema).alias("cdc"))
    .filter(col("cdc.op").isin("c", "r", "u"))  # create, read, update operations
    .select("cdc.after.*")
)

# 5) Write to MinIO as Parquet with checkpointing
query_minio = (
    df_parsed.writeStream
    .format("parquet")
    .option("path", "s3a://spark-output/tweets")
    .option("checkpointLocation", "/opt/spark/output/checkpoints/tweets")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

# 6) Wait for termination
query_minio.awaitTermination()

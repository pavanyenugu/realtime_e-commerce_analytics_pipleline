from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Spark session
spark = SparkSession.builder \
    .appName("ECommerceAnalytics") \
    .getOrCreate()

# Schema for Kafka messages
schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("quantity", IntegerType()) \
    .add("price", IntegerType()) \
    .add("timestamp", StringType())

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .load()

# Parse JSON
orders = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Aggregate sales by product
agg = orders.groupBy(
    window(col("timestamp"), "1 minute"),
    col("product")
).sum("price")

# Output to console (replace with Redshift sink in prod)
query = agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()

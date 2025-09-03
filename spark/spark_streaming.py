from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# FIXED: Complete schema matching producer data
transaction_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("user_id", StringType()) \
    .add("user_name", StringType()) \
    .add("email", StringType()) \
    .add("age", IntegerType()) \
    .add("gender", StringType()) \
    .add("country", StringType()) \
    .add("city", StringType()) \
    .add("product_id", StringType()) \
    .add("product_name", StringType()) \
    .add("category", StringType()) \
    .add("quantity", IntegerType()) \
    .add("price", DoubleType()) \
    .add("payment_method", StringType()) \
    .add("transaction_status", StringType()) \
    .add("timestamp", StringType())

# FIXED: Added Kafka and Cassandra connector JARs
spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# FIXED: Using internal port 29092 for inter-container communication
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka message with complete schema
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), transaction_schema).alias("data")) \
    .select("data.*")

# FIXED: Better error handling for Cassandra writes
def write_to_cassandra(batch_df, batch_id):
    if batch_df.count() > 0:
        try:
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="transactions", keyspace="ecommerce") \
                .save()
            print(f"✅ Batch {batch_id}: Written {batch_df.count()} records to Cassandra")
        except Exception as e:
            print(f"❌ Batch {batch_id}: Failed to write to Cassandra: {e}")
    else:
        print(f"⚠️  Batch {batch_id}: No records to write")

# Write to Cassandra with better error handling
query = df_parsed.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()

print("🚀 Spark Streaming job started. Waiting for data...")
query.awaitTermination()
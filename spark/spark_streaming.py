from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("EcommerceRealTimeAnalytics") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("user_name", StringType()),
    StructField("email", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("transaction_status", StringType()),
    StructField("timestamp", StringType())
])

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse data
df_parsed = raw_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), transaction_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd %H:%M:%S")) \
    .withColumn("transaction_value", col("price") * col("quantity")) \
    .withColumn("processed_at", current_timestamp())

def process_batch(batch_df, epoch_id):
    """Process each micro-batch with robust analytics and debugging"""
    
    if batch_df.count() == 0:
        print(f"⚠️  Batch {epoch_id}: No records to process")
        return
    
    print(f"🚀 Processing batch {epoch_id} with {batch_df.count()} records...")
    
    try:
        # 🔍 DEBUG: Show sample data and transaction statuses
        print(f"🔍 DEBUG - Batch {epoch_id} transaction statuses:")
        batch_df.groupBy("transaction_status").count().show()
        
        # Show sample records
        print(f"🔍 DEBUG - Sample records:")
        batch_df.select("transaction_id", "transaction_status", "transaction_value").show(5, truncate=False)
        
        # 1️⃣ SAVE RAW TRANSACTIONS
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="transactions", keyspace="ecommerce") \
            .save()
        print(f"✅ Batch {epoch_id}: Raw transactions saved")
        
        # 2️⃣ CONSISTENT TRANSACTION METRICS - Use exact case matching throughout
        transaction_metrics = batch_df.agg(
            count("*").alias("total_transactions"),
            # Use exact case matching consistently (your producer sends "Success", "Failed", "Pending")
            count(when(col("transaction_status") == "Success", 1)).alias("successful_transactions"),
            count(when(col("transaction_status") == "Failed", 1)).alias("failed_transactions"),
            count(when(col("transaction_status") == "Pending", 1)).alias("pending_transactions"),
            # Count null/empty statuses
            count(when(col("transaction_status").isNull() | (col("transaction_status") == ""), 1)).alias("unknown_status"),
            round(avg("transaction_value"), 2).alias("avg_transaction_value"),
            round(sum(when(col("transaction_status") == "Success", col("transaction_value")).otherwise(0)), 2).alias("total_revenue"),
            round(max("transaction_value"), 2).alias("max_transaction_value"),
            round(min("transaction_value"), 2).alias("min_transaction_value")
        ).withColumn("batch_id", lit(epoch_id)) \
         .withColumn("batch_time", current_timestamp())
        
        # Calculate success rate with null safety
        transaction_metrics = transaction_metrics.withColumn(
            "success_rate_percentage", 
            when(col("total_transactions") > 0, 
                 round((col("successful_transactions").cast("double") / col("total_transactions").cast("double")) * 100, 2)
            ).otherwise(0.0)
        )
        
        # 🔍 DEBUG: Show metrics before saving
        print(f"🔍 DEBUG - Batch {epoch_id} metrics:")
        transaction_metrics.show(truncate=False)
        
        transaction_metrics.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="transaction_metrics", keyspace="ecommerce") \
            .save()
        
        # 3️⃣ USER METRICS (Simplified)
        user_metrics = batch_df.agg(
            approx_count_distinct("user_id").alias("unique_users"),
            round(avg("age"), 2).alias("avg_customer_age"),
            count(when(col("gender") == "Male", 1)).alias("male_customers"),
            count(when(col("gender") == "Female", 1)).alias("female_customers"),
            count(when(col("gender") == "Other", 1)).alias("other_gender_customers"),
            round(sum("transaction_value") / approx_count_distinct("user_id"), 2).alias("avg_spend_per_user")
        ).withColumn("batch_id", lit(epoch_id)) \
         .withColumn("batch_time", current_timestamp())
        
        user_metrics.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="user_metrics", keyspace="ecommerce") \
            .save()
        
        # 4️⃣ CATEGORY METRICS
        category_metrics = batch_df.groupBy("category").agg(
            count("*").alias("order_count"),
            round(sum("transaction_value"), 2).alias("category_revenue"),
            round(avg("transaction_value"), 2).alias("avg_order_value"),
            approx_count_distinct("product_id").alias("unique_products"),
            approx_count_distinct("user_id").alias("unique_customers")
        ).withColumn("batch_id", lit(epoch_id)) \
         .withColumn("batch_time", current_timestamp())
        
        category_metrics.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="category_metrics", keyspace="ecommerce") \
            .save()
        
        # 5️⃣ PAYMENT METRICS - Fixed to use consistent case matching
        payment_metrics = batch_df.groupBy("payment_method").agg(
            count("*").alias("transaction_count"),
            round(sum(when(col("transaction_status") == "Success", col("transaction_value")).otherwise(0)), 2).alias("successful_revenue"),
            count(when(col("transaction_status") == "Failed", 1)).alias("failed_count"),
            round(avg("transaction_value"), 2).alias("avg_transaction_value")
        ).withColumn("batch_id", lit(epoch_id)) \
         .withColumn("batch_time", current_timestamp()) \
         .withColumn("success_rate", 
                    round(((col("transaction_count") - col("failed_count")) / col("transaction_count")) * 100, 2))
        
        payment_metrics.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="payment_metrics", keyspace="ecommerce") \
            .save()
        
        # 6️⃣ GEO METRICS - Fixed to use consistent case matching
        geo_metrics = batch_df.groupBy("country", "city").agg(
            count("*").alias("transaction_count"),
            round(sum(when(col("transaction_status") == "Success", col("transaction_value")).otherwise(0)), 2).alias("revenue"),
            approx_count_distinct("user_id").alias("unique_customers"),
            round(avg("transaction_value"), 2).alias("avg_transaction_value")
        ).withColumn("batch_id", lit(epoch_id)) \
         .withColumn("batch_time", current_timestamp())
        
        geo_metrics.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="geo_metrics", keyspace="ecommerce") \
            .save()
        
        # 7️⃣ FAILED TRANSACTIONS LOG - Fixed to use consistent case matching
        failed_transactions = batch_df.filter(col("transaction_status") == "Failed").select(
            "transaction_id", "user_id", "user_name", "email", "product_name", 
            "payment_method", "transaction_value", "country", "city", "event_time"
        ).withColumn("batch_id", lit(epoch_id)) \
         .withColumn("logged_at", current_timestamp())
        
        if failed_transactions.count() > 0:
            failed_transactions.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="failed_transactions", keyspace="ecommerce") \
                .save()
            print(f"⚠️  Batch {epoch_id}: {failed_transactions.count()} failed transactions logged")
        
        # 8️⃣ HIGH-VALUE ALERTS
        high_value_transactions = batch_df.filter(col("transaction_value") > 1000).select(
            "transaction_id", "user_id", "user_name", "product_name", 
            "transaction_value", "payment_method", "country"
        ).withColumn("alert_time", current_timestamp())
        
        if high_value_transactions.count() > 0:
            high_value_transactions.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="high_value_alerts", keyspace="ecommerce") \
                .save()
            print(f"🔥 Batch {epoch_id}: {high_value_transactions.count()} high-value transactions!")
        
        # Print batch summary
        metrics = transaction_metrics.collect()[0]
        print(f"""
📊 BATCH {epoch_id} DETAILED SUMMARY:
   Total Transactions: {metrics['total_transactions']}
   Successful: {metrics['successful_transactions']}
   Failed: {metrics['failed_transactions']}
   Pending: {metrics['pending_transactions']}
   Unknown Status: {metrics['unknown_status']}
   Success Rate: {metrics['success_rate_percentage']}%
   Total Revenue: ${metrics['total_revenue']}
   Avg Transaction: ${metrics['avg_transaction_value']}
        """)
        
        # 🔍 Alert if success rate is 0
        if metrics['success_rate_percentage'] == 0.0:
            print(f"⚠️  WARNING: Batch {epoch_id} has 0% success rate!")
            print("🔍 Possible causes:")
            print("   - All transactions failed")
            print("   - Transaction status values don't match 'Success'")
            print("   - Data quality issues")
            print("   - Upstream service failures")
            
            # Show actual status values for debugging
            print("🔍 Actual status values in this batch:")
            distinct_statuses = batch_df.select("transaction_status").distinct().collect()
            for row in distinct_statuses:
                print(f"   '{row['transaction_status']}'")
        
    except Exception as e:
        print(f"❌ Batch {epoch_id}: Error - {e}")
        import traceback
        traceback.print_exc()

# REMOVED THE PROBLEMATIC DEBUG FUNCTION CALL

# Start streaming
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

print("🚀 Real-time E-commerce Analytics Pipeline Started!")
print("📊 Processing transactions with comprehensive business metrics...")
print("🔗 Access Spark UI at: http://localhost:4040")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n⏹️  Stopping streaming job...")
    query.stop()
    spark.stop()
    print("✅ Pipeline stopped successfully!")
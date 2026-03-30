from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("MarketAnomalyDetector") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "market_ticks") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

time_df = parsed_df.withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))

# 30-second tumbling windows with 10-second late data tolerance
windowed_df = time_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "30 seconds"),
        col("ticker")
    ) \
    .agg(avg("price").alias("avg_price")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ticker"),
        col("avg_price")
    )

def write_to_sqlite(batch_df, batch_id):
    pdf = batch_df.toPandas()
    if not pdf.empty:
        import sqlite3
        
        # SQLite lacks native datetime/struct types; cast to ISO 8601 strings
        pdf['window_start'] = pdf['window_start'].astype(str)
        pdf['window_end'] = pdf['window_end'].astype(str)
        
        conn = sqlite3.connect('market_data.db')
        pdf.to_sql('market_aggs', conn, if_exists='append', index=False)
        conn.close()

query = windowed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_sqlite) \
    .start()

print("Listening to stream and writing to SQLite market_data.db...")
query.awaitTermination()
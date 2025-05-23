from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StringType, IntegerType

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("MonitoringGudangRealTime") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ===== Schema Suhu dan Kelembaban =====
suhu_schema = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

kelembaban_schema = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# ===== Stream dari Topik Suhu =====
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), suhu_schema).alias("data"), "timestamp") \
    .select("data.*", "timestamp")

# ===== Stream dari Topik Kelembaban =====
kelembaban_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

kelembaban_parsed = kelembaban_df.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), kelembaban_schema).alias("data"), "timestamp") \
    .select("data.*", "timestamp")

# ===== Join berdasarkan gudang_id dan window waktu 10 detik =====
suhu_window = suhu_parsed.withWatermark("timestamp", "10 seconds").alias("suhu")
kelembaban_window = kelembaban_parsed.withWatermark("timestamp", "10 seconds").alias("kelembaban")

gabung_df = suhu_window.join(
    kelembaban_window,
    expr("""
        suhu.gudang_id = kelembaban.gudang_id AND
        suhu.timestamp BETWEEN kelembaban.timestamp - interval 10 seconds AND kelembaban.timestamp + interval 10 seconds
    """)
)

# ===== Tambahkan logika status =====
result_df = gabung_df.withColumn(
    "status",
    expr("""
    CASE 
        WHEN suhu.suhu > 80 AND kelembaban.kelembaban > 70 THEN 'BAHAYA TINGGI'
        WHEN suhu.suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
        WHEN kelembaban.kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
        ELSE 'AMAN'
    END
    """)
).select(
    col("suhu.gudang_id").alias("gudang_id"),
    col("suhu.suhu").alias("suhu"),
    col("kelembaban.kelembaban").alias("kelembaban"),
    col("status")
)

# ===== Output ke console =====
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "D:/Kuliah/sem4/BigData/Tugas 3/checkpoint") \
    .start()

query.awaitTermination()

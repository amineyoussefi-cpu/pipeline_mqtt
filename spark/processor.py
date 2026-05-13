import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

# Schéma des messages JSON
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("capteur_id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("pression", DoubleType()) \
    .add("vibration", DoubleType())

# Démarrage de Spark
spark = SparkSession.builder \
    .appName("IoT Pipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Écriture dans TimescaleDB
def ecrire_dans_timescaledb(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/iot_data") \
        .option("dbtable", "mesures") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"✅ Batch {batch_id} écrit dans TimescaleDB")

# Workaround for Spark 4.1.1 bug: KafkaMicroBatchStream.metrics() throws NPE
# in continuous streaming. Use AvailableNow trigger instead — processes all
# pending Kafka messages and exits cleanly each iteration, bypassing the
# buggy metrics path entirely.
while True:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", "iot-sensor") \
        .option("startingOffsets", "earliest") \
        .load()

    mesures = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select(
        to_timestamp(col("data.timestamp")).alias("timestamp"),
        col("data.capteur_id"),
        col("data.temperature"),
        col("data.pression"),
        col("data.vibration")
    )

    query = mesures.writeStream \
        .foreachBatch(ecrire_dans_timescaledb) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()
    time.sleep(5)

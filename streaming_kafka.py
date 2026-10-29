from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("OlistStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("id_pedido", IntegerType()) \
    .add("estado", StringType()) \
    .add("valor", DoubleType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_olist") \
    .load()

df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

query = df_parsed.groupBy("estado").count() \
    .writeStream.outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

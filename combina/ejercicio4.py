import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")  # Ruta de tu instalación de Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Ejercicio4") \
    .getOrCreate()

# Configurar el nivel de log
spark.sparkContext.setLogLevel("WARN")

# Esquema para los datos meteorológicos
weather_schema = StructType([
    StructField("ciudad", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperatura", DoubleType(), True),
    StructField("velocidad_viento", DoubleType(), True)
])

# Esquema para los datos de aviones
aviation_schema = StructType([
    StructField("ac", ArrayType(StructType([
        StructField("flight", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("alt_baro", DoubleType(), True),
        StructField("category", StringType(), True)
    ])), True),
    StructField("ctime", StringType(), True),
    StructField("msg", StringType(), True),
    StructField("now", StringType(), True),
    StructField("ptime", DoubleType(), True),
    StructField("total", DoubleType(), True)
])

# Configuración del flujo de datos meteorológicos desde Kafka
weather_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "Cloudera02:9092") \
    .option("subscribe", "sjuanmonTopic") \
    .option("startingOffsets", "earliest") \
    .load()

# Procesar los datos meteorológicos
weather_df = weather_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.ciudad", "data.timestamp", "data.temperatura", "data.velocidad_viento")

# Configuración del flujo de datos de aviones desde un socket
aviation_socket = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 21009) \
    .load()

# Procesar los datos de aviones
aviation_df = aviation_socket \
    .selectExpr("value as json") \
    .select(from_json(col("json"), aviation_schema).alias("data")) \
    .selectExpr("explode(data.ac) as flight_data") \
    .select("flight_data.flight", "flight_data.lat", "flight_data.lon", "flight_data.alt_baro", "flight_data.category")

# Mostrar el flujo meteorológico con trigger de 10 segundos
weather_query = weather_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()

# Mostrar el flujo de aviones
aviation_query = aviation_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Esperar a que los streams terminen
try:
    weather_query.awaitTermination()
    aviation_query.awaitTermination()
except KeyboardInterrupt:
    print("Stream detenido por el usuario.")






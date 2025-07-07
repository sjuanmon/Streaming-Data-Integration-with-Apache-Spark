from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

# Leer los mensajes desde Kafka (usando el topic de Kafka)
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "Cloudera02:9092") \
    .option("subscribe", "sjuanmonTopic") \
    .option("startingOffsets", "earliest") \
    .load()

# Los datos de Kafka están en formato binario, por lo que necesitamos deserializarlos
# Decodificar los valores de Kafka y convertirlos en un DataFrame estructurado
df_json = df.selectExpr("CAST(value AS STRING) AS json_value")

# Parsear el JSON recibido (extraer los campos del JSON)
df_parsed = df_json.selectExpr(
    "json_tuple(json_value, 'ciudad', 'timestamp', 'temperatura', 'velocidad_viento') AS (ciudad, timestamp, temperatura, velocidad_viento)"
)

# Convertir las columnas a sus tipos adecuados
df_final = df_parsed \
    .withColumn("temperatura", col("temperatura").cast("double")) \
    .withColumn("velocidad_viento", col("velocidad_viento").cast("double"))

# Escribir el resultado en la consola (para ver en tiempo real)
query = df_final \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar hasta que termine el streaming
query.awaitTermination()
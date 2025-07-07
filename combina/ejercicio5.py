import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")  # Ruta de tu instalación de Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, unix_timestamp, udf, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType  # Importación correcta de ArrayType y otros tipos
from math import radians, sin, cos, sqrt, atan2
import time

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Ejercicio5") \
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
    StructField("ac", ArrayType(StructType([  # Asegúrate de usar ArrayType aquí
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

# Añadir la columna de tiempo de los datos meteorológicos
weather_df_with_time = weather_df \
    .withColumn("weather_time", unix_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

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
    .select("data.ac", "data.now")  # Seleccionamos 'ac' y 'now' directamente desde 'data'

# Explosión de la columna "ac" y selección de las columnas
aviation_df_exploded = aviation_df \
    .select(explode(col("ac")).alias("flight_data"), "now")  # Explosion de 'ac' y 'now'

# Selección de las columnas necesarias del 'flight_data'
aviation_df_final = aviation_df_exploded \
    .select(
        "flight_data.flight", 
        "flight_data.lat", 
        "flight_data.lon", 
        "flight_data.alt_baro", 
        "flight_data.category", 
        "now"  # Aquí aseguramos que 'now' se mantiene accesible
    )

# Coordenadas de las ciudades
barcelona = (41.9774, 2.08)
girona = (41.9028, 2.8214)
tarragona = (41.1184, 1.25)

# Función haversine para calcular la distancia entre dos puntos
def haversine(lat1, lon1, lat2, lon2):
    # Radio de la Tierra en kilómetros
    R = 6371.0
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    return R * c

# Convertir la función haversine a UDF
haversine_udf = udf(haversine, DoubleType())

# Aplicar la función haversine para calcular la ciudad más cercana
aviation_df_with_city = aviation_df_final \
    .withColumn(
        "distance_to_barcelona", 
        haversine_udf(col("lat"), col("lon"), lit(barcelona[0]), lit(barcelona[1]))
    ) \
    .withColumn(
        "distance_to_girona", 
        haversine_udf(col("lat"), col("lon"), lit(girona[0]), lit(girona[1]))
    ) \
    .withColumn(
        "distance_to_tarragona", 
        haversine_udf(col("lat"), col("lon"), lit(tarragona[0]), lit(tarragona[1]))
    )

# Determinar la ciudad más cercana
aviation_df_with_city = aviation_df_with_city \
    .withColumn(
        "ciudad", 
        when(col("distance_to_barcelona") < col("distance_to_girona"), 
             when(col("distance_to_barcelona") < col("distance_to_tarragona"), lit("Barcelona")).otherwise(lit("Tarragona"))
        ).otherwise(
            when(col("distance_to_girona") < col("distance_to_tarragona"), lit("Girona")).otherwise(lit("Tarragona"))
        )
    )

# Unir los flujos de datos de meteorología y aviación por la columna "ciudad"
combined_df = weather_df_with_time.join(aviation_df_with_city, "ciudad") \
    .withColumn("aviation_time", unix_timestamp("now", "yyyy-MM-dd HH:mm:ss").cast("timestamp"))  # Convertir 'now' a timestamp

# Convertir ambos timestamps a tipo timestamp
combined_df = combined_df.withColumn(
    "aviation_time", (col("aviation_time").cast("timestamp"))
)

# Filtrar los datos con más de 60 segundos de retardo del dataframe de aviones
combined_df = combined_df.filter(
    (unix_timestamp("aviation_time") - unix_timestamp("weather_time")).cast("double") <= 60  # 60 segundos de retardo en aviones
)

# Filtrar los datos con más de 30 minutos de retardo del dataframe de meteorología
combined_df = combined_df.filter(
    (unix_timestamp("weather_time") - unix_timestamp("aviation_time")).cast("double") <= 1800  # 1800 segundos (30 minutos) de retardo en meteorología
)

# Calcular la diferencia en segundos entre los dos timestamps
combined_df = combined_df.withColumn(
    "deltaT", 
    (unix_timestamp("aviation_time") - unix_timestamp("weather_time")).cast("double")  # Convertir a segundos
)

# Mostrar el flujo combinado con trigger de 10 segundos
combined_query = combined_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()

# Esperar a que el stream termine
try:
    combined_query.awaitTermination()
except KeyboardInterrupt:
    print("Stream detenido por el usuario.")


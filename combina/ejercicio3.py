import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, udf  # Asegúrate de que 'udf' esté importado
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
import math

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Ejercicio3").getOrCreate()

# Función Haversine para calcular la distancia
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radio de la Tierra en km
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c  # Resultado en km
    return distance

# Crear un UDF (User Defined Function) de Haversine para utilizarla en el DataFrame
haversine_udf = udf(haversine, DoubleType())

# Coordenadas de los aeropuertos
barcelona = (41.9774, 2.0800)
tarragona = (41.1184, 1.2500)
girona = (41.9028, 2.8214)

# Crear un DataFrame de ejemplo que contiene las distancias calculadas a los aeropuertos
# Suponemos que ya tienes un DataFrame con distancias a Barcelona, Tarragona y Girona
data = [
    (1, 41.9774, 2.0800, 10.5, 50.2, 120.8),  # Aeronave 1 (cerca de Barcelona)
    (2, 41.9028, 2.8214, 120.0, 70.8, 30.4),  # Aeronave 2 (cerca de Girona)
    (3, 41.1184, 1.2500, 30.0, 45.0, 10.0),   # Aeronave 3 (cerca de Tarragona)
]

columns = ["id_aeronave", "Latitud", "Longitud", "barcelona_distance", "girona_distance", "tarragona_distance"]

# Crear el DataFrame
df = spark.createDataFrame(data, columns)

# Añadir la columna "ciudad" con el aeropuerto más cercano
df_con_ciudad = df.withColumn(
    "ciudad",
    when(col("barcelona_distance") < col("girona_distance"), "Barcelona")
    .when(col("barcelona_distance") < col("tarragona_distance"), "Barcelona")
    .when(col("girona_distance") < col("tarragona_distance"), "Girona")
    .otherwise("Tarragona")
)

# Mostrar el resultado
df_con_ciudad.show()
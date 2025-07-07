import requests
import json
from kafka import KafkaProducer
from requests.auth import HTTPBasicAuth
import time
import random

ciudades=[{"ciudad":"Barcelona",
     "latitud":"41.3874",
     "longitud": "2.1686"},
     {"ciudad":"Tarragona",
     "latitud":"41.1189",
     "longitud": "1.2445"},
     {"ciudad":"Girona",
     "latitud":"41.9794",
     "longitud": "2.8214"}
]

LIMITE=5 # Número máximo de consultas a la API, para evitar que el script quede permanentemente realizando consultas
TIEMPO_LIMITE=4500 # Tiempo máximo de funcionamiento del script (en segundos)
tiempo=0
contador=0


producer = KafkaProducer(
    bootstrap_servers='Cloudera02:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

inicio = time.time()

while True:
    for ciudad in ciudades:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": ciudad["latitud"],
            "longitude": ciudad["longitud"],
            "current_weather": True
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            datos = response.json()
            if "current_weather" in datos:
                temperatura = datos["current_weather"]["temperature"]
                viento = datos["current_weather"]["windspeed"]
                timestamp = datos["current_weather"]["time"]

                mensaje = {
                    "ciudad": ciudad["ciudad"],
                    "timestamp": timestamp,
                    "temperatura": temperatura,
                    "velocidad_viento": viento
                }

                # Enviar mensaje a Kafka
                producer.send('sjuanmonTopic', mensaje)
                print("Enviado a Kafka: {}".format(mensaje))  # Versión compatible con todas las versiones de Python

    contador += 1
    fin = time.time()
    tiempo = fin - inicio
    if tiempo > TIEMPO_LIMITE or contador >= LIMITE:
        break

    time.sleep(900)  # Espera de 15 minutos (900 segundos)

# Cerrar el producer después de salir del bucle
producer.close()
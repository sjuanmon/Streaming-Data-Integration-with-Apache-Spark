# Data Stream Integration with Spark Structured Streaming
This project is part of an advanced activity focused on integrating multiple real-time data streams using Apache Spark Structured Streaming. The goal is to combine live air traffic data with real-time weather information from Open-Meteo to produce meaningful, 
enriched insights for aviation monitoring.


## 🧪 Overview of the Proyect
The activity is structured in two main blocks:

Weather Data Streaming

Using Open-Meteo API to retrieve weather conditions.

Publishing data into a Kafka topic.

Reading it back using Spark Structured Streaming.

Stream Integration

Preparing aviation and weather data.

Merging both streams using common fields (e.g. destination city).

Performing real-time analytics on the joined streams.

## 🌦️ Block 1: Weather Data Stream // Meteo File

### ✅ Exercise 1 – Stream Weather Data to Kafka
Modified kafkaOpenMeteo.py to:

Query Open-Meteo API using coordinates for Barcelona, Tarragona, and Girona.

Extract:

Temperature at 2 meters

Wind speed at 10 meters

Timestamp

Add the city name to each record.

Send this data to a user-defined Kafka topic using KafkaProducer.

Output was printed to the console for verification.

📁 Deliverables:

kafkaOpenMeteo.py

Screenshot: ejercicio1.png

### ✅ Exercise 2 – Ingest Weather Data with Spark
Created ejercicio2.py to read from the Kafka topic using Spark Structured Streaming.

Parsed JSON messages to extract:

City

Temperature

Wind speed

Timestamp

Displayed the structured DataFrame on the console.

📁 Deliverables:

ejercicio2.py

Screenshot: ejercicio2.png showing both Kafka producer and Spark consumer terminals

## ✈️ Block 2: Stream Integration // Combina File

### ✅ Exercise 3 – Prepare Aviation Data for Joining
Adapted the flight stream from Activity 4 to:

Identify the nearest airport (Barcelona, Girona, or Tarragona)

Add a new column city representing the assumed destination of each aircraft

📁 Deliverables:

ejercicio3.py

Screenshot: ejercicio3.png

### ✅ Exercise 4 – Display Both Streams
Created a Spark script to simultaneously read:

Weather stream (meteo)

Flight stream (aviones)

Displayed both streams in real time.

📁 Deliverables:

ejercicio4.py

Flight data producer script

Screenshot: ejercicio4.png

### ✅ Exercise 5 – Join Streams by City and Time Constraints
Extended ejercicio4.py to:

Join both streams on the city field.

Apply time constraints:

Discard flight data delayed by more than 60 seconds

Discard weather data delayed by more than 30 minutes

Add a column deltaT with the timestamp difference between records.

Print the enriched, merged stream to the console.

📁 Deliverables:

ejercicio5.py

Screenshot: ejercicio5.png

### 📌 Notes
Kafka topic naming convention used: <UOC_username>Topic

API responses were filtered and formatted according to the task constraints.

Time windows and watermarking were essential for stream join consistency.

All code was tested in a terminal-based environment using multiple concurrent sessions.




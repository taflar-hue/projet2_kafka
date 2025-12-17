from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json, statistics, time
from datetime import datetime


# Kafka Consumer

consumer = KafkaConsumer(
    "capteurs",
    bootstrap_servers="localhost:9092",  
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='capteurs-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# InfluxDB

INFLUX_URL = "http://localhost:8086"  
INFLUX_TOKEN = "b2cWipS1p4G2a_dXPrx2qjndLcD6gqHG73oE0auDUyoxwfpPYtcbQ6NDMtMzYezXvfOvFQYiyolHcjwBdexU0A=="
INFLUX_ORG = "myorg"
BUCKET = "capteurs"

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Buffer d'agrégation

temp_buffer = []
humidity_buffer = []
aggregation_count = 0

print("\n Consumer  démarré (temps réel avec agrégation)\n")

try:
    for msg in consumer:
        data = msg.value
        offset = msg.offset

       
        # Affichage TEMPS RÉEL
        
        print("  Message reçu")
        print(f"    Offset    : {offset}")
        print(f"    Capteur   : {data['capteur_id']}")
        print(f"    Temp      : {data['temperature']} °C")
        print(f"    Humidité  : {data['humidity']} %")
        print(f"    Time      : {datetime.fromtimestamp(data['timestamp'])}")

        
        # Filtrage / Anomalie
       
        data["anomaly"] = data["temperature"] > 30
        if data["anomaly"]:
            print("    ALERTE : Température élevée !")

       
        # Stockage dans InfluxDB 
       
        point = Point("capteur") \
            .tag("capteur_id", str(data["capteur_id"])) \
            .field("temperature", float(data["temperature"])) \
            .field("humidity", float(data["humidity"])) \
            .time(int(data["timestamp"]*1e9))  # ns
        
        
        write_api.write(bucket=BUCKET, org=INFLUX_ORG, record=point)

       
        # Agrégation 
       
        temp_buffer.append(float(data["temperature"]))
        humidity_buffer.append(float(data["humidity"]))
        aggregation_count += 1

        if len(temp_buffer) == 10:
            # Calculs statistiques pour température
            avg_temp = statistics.mean(temp_buffer)
            min_temp = min(temp_buffer)
            max_temp = max(temp_buffer)
            std_temp = statistics.stdev(temp_buffer) if len(temp_buffer) > 1 else 0
            
            # Calculs statistiques pour humidité
            avg_humidity = statistics.mean(humidity_buffer)
            min_humidity = min(humidity_buffer)
            max_humidity = max(humidity_buffer)
            std_humidity = statistics.stdev(humidity_buffer) if len(humidity_buffer) > 1 else 0
            
            # Nombre d'alertes dans le buffer
            alert_count = sum(1 for temp in temp_buffer if temp > 30)

            print("\n AGRÉGATION (10 messages)")
            print(f"    Moyenne Temp   : {avg_temp:.2f}°C")
            print(f"    Min Temp       : {min_temp:.2f}°C")
            print(f"    Max Temp       : {max_temp:.2f}°C")
            print(f"    Écart-type Temp: {std_temp:.2f}°C")
            print(f"    Alertes Temp   : {alert_count}/10")
            print(f"    Moyenne Hum    : {avg_humidity:.2f}%")
            print("-" * 50)

            
            # Stockage des métriques agrégées dans InfluxDB
           
            
            stats_point = Point("statistiques") \
                .tag("type_agregat", "global") \
                .tag("fenetre", "10_messages") \
                .field("moyenne_temp", avg_temp) \
                .field("min_temp", min_temp) \
                .field("max_temp", max_temp) \
                .field("std_temp", std_temp) \
                .field("count_temp", len(temp_buffer)) \
                .field("alertes_count", alert_count) \
                .field("moyenne_humidity", avg_humidity) \
                .field("min_humidity", min_humidity) \
                .field("max_humidity", max_humidity) \
                .field("std_humidity", std_humidity) \
                .time(int(time.time() * 1e9))  # timestamp actuel en ns
            
            # Écrire les statistiques agrégées
            write_api.write(bucket=BUCKET, org=INFLUX_ORG, record=stats_point)
            
            # Réinitialiser les buffers
            temp_buffer.clear()
            humidity_buffer.clear()
            aggregation_count = 0

except KeyboardInterrupt:
    print("\n Consumer arrêté par l'utilisateur")

finally:
    consumer.close()
    client.close()
    print(" Connexions fermées")
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient
import json
import time

def simple_validation():
    
    topic = 'capteurs'
    kafka_bootstrap = 'localhost:9092'
    influx_url = 'http://localhost:8086'
    influx_token = 'b2cWipS1p4G2a_dXPrx2qjndLcD6gqHG73oE0auDUyoxwfpPYtcbQ6NDMtMzYezXvfOvFQYiyolHcjwBdexU0A=='
    org = 'myorg'
    bucket = 'capteurs'

   
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )

   
    client = InfluxDBClient(url=influx_url, token=influx_token, org=org)
    query_api = client.query_api()

    print("Attente d'un message Kafka...")
    messages = []
    for message in consumer:
        messages.append(message)
        if len(messages) >= 1:
            break

    if not messages:
        print("Aucun message reçu de Kafka.")
        return

    kafka_message = messages[0].value
    print(f"Message Kafka reçu: {kafka_message}")

    
    print("Attente de 2 secondes pour l'écriture dans InfluxDB...")
    time.sleep(2)

    
    capteur_id = str(kafka_message['capteur_id'])
    timestamp = kafka_message['timestamp']
    temperature = kafka_message['temperature']
    humidity = kafka_message['humidity']

    
    timestamp_ns = int(float(timestamp) * 1e9)
    start_ns = timestamp_ns - 1_000_000_000  
    stop_ns = timestamp_ns + 1_000_000_000   

    query = f'''
    from(bucket: "{bucket}")
      |> range(start: time(v: {start_ns}), stop: time(v: {stop_ns}))
      |> filter(fn: (r) => r["_measurement"] == "capteur")
      |> filter(fn: (r) => r["capteur_id"] == "{capteur_id}")
    '''

    print(f"Exécution de la requête Flux: {query}")

    
    result = query_api.query(org=org, query=query)

  
    temperature_found = None
    humidity_found = None

    for table in result:
        for record in table.records:
            field = record.get_field()
            value = record.get_value()
            if field == "temperature":
                temperature_found = value
            elif field == "humidity":
                humidity_found = value

    # Comparer les valeurs
    success = True
    if temperature_found is None:
        print(" Température non trouvée dans InfluxDB")
        success = False
    elif abs(float(temperature_found) - float(temperature)) > 0.01:
        print(f" Température différente: Kafka={temperature}, InfluxDB={temperature_found}")
        success = False
    else:
        print(f" Température correspondante: {temperature}")

    if humidity_found is None:
        print(" Humidité non trouvée dans InfluxDB")
        success = False
    elif abs(float(humidity_found) - float(humidity)) > 0.1:
        print(f" Humidité différente: Kafka={humidity}, InfluxDB={humidity_found}")
        success = False
    else:
        print(f" Humidité correspondante: {humidity}")

    if success:
        print("\n Validation réussie !")
    else:
        print("\n Validation échouée.")

    # Fermer les connexions
    consumer.close()
    client.close()

if __name__ == "__main__":
    simple_validation()
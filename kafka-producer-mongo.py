from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

# Enviar un mensaje de prueba
producer.send('people', {"name": "AntonioGH1"} ).add_callback(on_send_success).add_errback(on_send_error)

# Leer el archivo JSON desde la URL
url = 'https://raw.githubusercontent.com/adsoftsito/spark-labs/refs/heads/main/results/data.json'
df = pd.read_json(url, orient='columns')

# Iterar sobre las primeras 3 filas del DataFrame
for index, value in df.head(3).iterrows():
    dict_data = dict(value)
    producer.send('people', value=dict_data).add_callback(on_send_success).add_errback(on_send_error)
    print(dict_data)

# Esperar hasta que todos los mensajes sean enviados antes de cerrar el productor
producer.flush()  # Asegura que todos los mensajes sean enviados
producer.close()  # Cierra el productor

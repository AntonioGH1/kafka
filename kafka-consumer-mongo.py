from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
uri = "mongodb+srv://antonioai3000:aA4BTqqfLj3MGPPB@test.fexrf.mongodb.net/?retryWrites=true&w=majority&appName=test"

# Connect to MongoDB and people database
try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.people
    print("MongoDB Connected successfully!")
except Exception as e:
    print(f"Could not connect to MongoDB. Error: {e}")

consumer = KafkaConsumer('people',bootstrap_servers=[
     'localhost:9092'
     ])

# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    try:
       meme_rec = {'data': record }
       print (record)
       record_id = db.people.insert_one(meme_rec)
       print("Data inserted with record ids", record_id)
    except:
       print("Could not insert into MongoDB")

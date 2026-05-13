import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# Connexion à Kafka
# On crée le producteur
# bootstrap_servers : l'adresse de Kafka
# value_serializer : convertit le dict Python en bytes JSON
# (Kafka ne stocke que des bytes, pas des objets Python)
producer = KafkaProducer(
    bootstrap_servers="localhost:9093",  # ← 9093 au lieu de 9092
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Quand un message MQTT arrive, cette fonction est appelée
def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())
    # On envoie dans Kafka
    # "iot-sensor" = le topic (la boîte aux lettres)
    # value=data = le contenu du message
    producer.send("iot-sensor", value=data)
    print(f"→ Kafka : {data}")

# Connexion MQTT
client = mqtt.Client() 
client.on_message = on_message
client.connect("localhost", 1883) # Connexion au broker
client.subscribe("iot/raffinerie")  # Subscribe - redistribute 

print("Bridge démarré. En écoute...")
client.loop_forever()
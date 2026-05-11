import time
import json
import random
import datetime
import paho.mqtt.client as mqtt

# Connexion au broker MQTT
client = mqtt.Client()
client.connect("localhost", 1883)

print("Simulateur démarré. Envoi des mesures...")

while True:
    mesure = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "capteur_id": "pompe_01",
        "temperature": round(random.uniform(60, 120), 2),
        "pression":    round(random.uniform(1.0, 5.0), 2),
        "vibration":   round(random.uniform(0.1, 2.0), 2)
    }

    payload = json.dumps(mesure)
    client.publish("iot/raffinerie", payload)
    print(f"Envoyé : {payload}")

    time.sleep(2)  # une mesure toutes les 2 secondes
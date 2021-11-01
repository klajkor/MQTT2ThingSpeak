# python3.7

"""
Converting MQTT messages and sending them to ThingSpeak
"""

import random

import secrets

from paho.mqtt import client as mqtt_client



# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'

def connect_mqtt() -> mqtt_client:
    """ Connect to MQTT broker """

    def on_connect(client, userdata, flags, rc):
        """ on_connect callback """
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(secrets.MQTT_USERNAME, secrets.MQTT_PASSWORD)
    client.on_connect = on_connect
    client.connect(secrets.MQTT_BROKER, secrets.MQTT_PORT)
    return client


def subscribe_sensor(client: mqtt_client):
    """ Subscribe to MQTT topic """
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(secrets.MQTT_TOPIC_SENSOR)
    client.on_message = on_message


def subscribe_state(client: mqtt_client):
    """ Subscribe to MQTT topic """
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(secrets.MQTT_TOPIC_STATE)
    client.on_message = on_message


def run():
    """ Run MQTT connect """
    client = connect_mqtt()
    subscribe_sensor(client)
    subscribe_state(client)
    client.loop_forever()


if __name__ == '__main__':
    run()

# python3.7

"""
Converting MQTT messages and sending them to ThingSpeak
"""

import secrets
import random
import json
import time
import datetime
import thingspeak
from paho.mqtt import client as mqtt_client


# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'

# How to map JSON topics of MQTT messages to ThingSpeak fields
FIELD_MAPPER_LIST = [
    {"json_topic":"SI7021", "json_subtopic":"Temperature", "ts_field":"field1"},
    {"json_topic":"SI7021", "json_subtopic":"Humidity", "ts_field":"field2"},
    {"json_topic":"BH1750", "json_subtopic":"LightLevel", "ts_field":"field3"},
    {"json_topic":"PIR1", "json_subtopic":"StatusNum", "ts_field":"field4"},
    {"json_topic":"BH1750", "json_subtopic":"LightAlarmStatusNum", "ts_field":"field5"}
]


def connect_mqtt() -> mqtt_client:
    """ Connect to MQTT broker """

    def on_connect(client, userdata, flags, return_code):
        """ on_connect callback """
        time_now = datetime.datetime.now()
        time_now_str = time_now.strftime("%Y-%m-%d %H:%M:%S")
        if return_code == 0:
            print(f"{time_now_str} Connected to MQTT Broker, started to subscribe.")
            subscribe_tele_root(client)
        else:
            print(f"{time_now_str} >>FAILED to connect, return code %d\n", return_code)

    def on_disconnect(client, userdata, return_code):
        """ on_disconnect callback """
        time_now = datetime.datetime.now()
        time_now_str = time_now.strftime("%Y-%m-%d %H:%M:%S")
        if return_code != 0:
            print(f"{time_now_str} >>UNEXPECTED disconnection.")
        else:
            print(f"{time_now_str} >>Normal disconnection.")

    client = mqtt_client.Client(client_id)
    client.username_pw_set(secrets.MQTT_USERNAME, secrets.MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(secrets.MQTT_BROKER, secrets.MQTT_PORT)
    return client


def subscribe_tele_root(client_sensor: mqtt_client):
    """ Subscribe to 'tele root' and additional MQTT sub-topics """
    (result, mid) = client_sensor.subscribe(secrets.MQTT_TOPIC_TELE_ROOT)
    time_now = datetime.datetime.now()
    time_now_str = time_now.strftime("%Y-%m-%d %H:%M:%S")
    if result == mqtt_client.MQTT_ERR_SUCCESS:
        print(f"{time_now_str} Subscribed to: '{secrets.MQTT_TOPIC_TELE_ROOT}'")
        client_sensor.on_message = on_message_tele_root
        client_sensor.message_callback_add(secrets.MQTT_TOPIC_SENSOR, on_message_tele_sensor)
    else:
        print(f"{time_now_str} >>FAILED to subscribe to: '{secrets.MQTT_TOPIC_TELE_ROOT}'")


def on_message_tele_root(client_sensor, userdata, msg):
    """ On message callback for general messages not handled by topic specific callbacks
    Currently do nothing """


def on_message_tele_sensor(client_sensor, userdata, msg):
    """ Decoding incoming MQTT message and sending to ThingSpeak """
    decoded_message = msg.payload.decode("utf-8")
    json_message = json.loads(decoded_message)
    # print(f"SENSOR Received from `{msg.topic}` topic")
    ts_payload = {}

    for field_map in FIELD_MAPPER_LIST:
        json_topic = field_map['json_topic']
        if json_topic in json_message:
            topic_json_message = json_message[json_topic]
            json_subtopic = field_map['json_subtopic']
            if json_subtopic in topic_json_message:
                time_now = datetime.datetime.now()
                time_now_str = time_now.strftime("%Y-%m-%d %H:%M:%S")
                subtopic_value = topic_json_message[json_subtopic]
                print(f"{time_now_str} '{json_subtopic}': '{subtopic_value}'")
                ts_field = field_map['ts_field']
                ts_payload[ts_field] = subtopic_value

    if len(ts_payload) > 0:
        print(f"{time_now_str} TS payload: '{ts_payload}'")
        time.sleep(15)
        THINGSPEAK_CHANNEL.update(ts_payload)


def run(ts_channel_run: thingspeak.Channel):
    """ Run MQTT connect """
    client = connect_mqtt()
    client.loop_forever()


if __name__ == '__main__':
    THINGSPEAK_CHANNEL = thingspeak.Channel(id = secrets.THINGSPEAK_CHANNEL_ID,
        api_key = secrets.THINGSPEAK_API_KEY)
    run(THINGSPEAK_CHANNEL)

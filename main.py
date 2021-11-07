# python3.7

"""
Converting MQTT messages and sending them to ThingSpeak
"""

import secrets
import logging
import sys
import random
import json
import time
import datetime
from queue import Queue
from threading import Thread
from paho.mqtt import client as mqtt_client
import thingspeak


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

thingspeak_queue = Queue()

def send_to_thingspeak(ts_queue):
    """This is the worker thread function. It processes items in the queue one after
    another.  These daemon thread go into an infinite loop, and only exit when
    the main thread ends.
    """
    while True:
        q_ts_payload = ts_queue.get()
        logging.info("Sending payload from queue to TS: '%s'", q_ts_payload)
        THINGSPEAK_CHANNEL.update(q_ts_payload)
        time.sleep(15)
        ts_queue.task_done()


def connect_mqtt() -> mqtt_client:
    """ Connect to MQTT broker """

    def on_connect(client, userdata, flags, return_code):
        """ on_connect callback """
        if return_code == 0:
            logging.info("Connected to MQTT Broker, started to subscribe.")
            subscribe_tele_root(client)
        else:
            logging.error(">>FAILED to connect, return code:%d\n", return_code)

    def on_disconnect(client, userdata, return_code):
        """ on_disconnect callback """
        if return_code != 0:
            logging.info(">> UNEXPECTED disconnection, return code:{return_code}\n")
        else:
            logging.warning(">> Normal disconnection.")

    logging.info("Starting MQTT connection to %s", secrets.MQTT_BROKER)
    client = mqtt_client.Client(client_id)
    client.username_pw_set(secrets.MQTT_USERNAME, secrets.MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(secrets.MQTT_BROKER, secrets.MQTT_PORT)
    return client


def subscribe_tele_root(client_sensor: mqtt_client):
    """ Subscribe to 'tele root' and additional MQTT sub-topics """
    (result, mid) = client_sensor.subscribe(secrets.MQTT_TOPIC_TELE_ROOT)
    if result == mqtt_client.MQTT_ERR_SUCCESS:
        logging.info("Subscribed to: '%s'", secrets.MQTT_TOPIC_TELE_ROOT)
        client_sensor.on_message = on_message_tele_root
        client_sensor.message_callback_add(secrets.MQTT_TOPIC_SENSOR, on_message_tele_sensor)
    else:
        logging.error(">> FAILED to subscribe to: '%s'", secrets.MQTT_TOPIC_TELE_ROOT)


def on_message_tele_root(client_sensor, userdata, msg):
    """ On message callback for general messages not handled by topic specific callbacks
    Currently does nothing """


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
                subtopic_value = topic_json_message[json_subtopic]
                logging.info("'%s': '%d'", json_subtopic, subtopic_value)
                ts_field = field_map['ts_field']
                ts_payload[ts_field] = subtopic_value

    if len(ts_payload) > 0:
        logging.info("Adding TS payload to queue: '%s'", ts_payload)
        thingspeak_queue.put(ts_payload)


def run(ts_channel_run: thingspeak.Channel):
    """ Run MQTT connect """
    client = connect_mqtt()
    client.loop_forever()


def setup_logging():
    """ Setup logging to file and console """
    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("log_file.txt"),
        logging.StreamHandler(sys.stdout)
    ]
)


if __name__ == '__main__':
    setup_logging()
    # Init worker thread of sending to ThingSpeak queue
    worker = Thread(target=send_to_thingspeak, args=(thingspeak_queue,))
    worker.setDaemon(True)
    worker.start()
    THINGSPEAK_CHANNEL = thingspeak.Channel(id = secrets.THINGSPEAK_CHANNEL_ID,
        api_key = secrets.THINGSPEAK_API_KEY)
    run(THINGSPEAK_CHANNEL)

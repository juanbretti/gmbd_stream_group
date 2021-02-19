# https://kafka-python.readthedocs.io/en/master/usage.html
# https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1

from kafka import KafkaConsumer
from json import loads
from constants import *

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(KAFKA_TOPIC,
                         bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                         value_deserializer=lambda x: x.decode('utf-8'))

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

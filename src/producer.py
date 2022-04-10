from inspect import _void
import json
from multiprocessing.sharedctypes import Value
from time import sleep
import logging
import random
import string

from kafka import KafkaProducer

topic_name = 'test-topic'

producer = KafkaProducer (
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(metadata):
    print('messages sent')
    
def on_send_failure(ex):
    print(ex)
    logging.error('[ERROR] Error occured while sending message', exc_info=ex)

def random_string(len: int) -> str:
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(len))

def run(seconds: int, number_of_messages: int) -> None:
    assert seconds >= 0, 'seconds can\'t be negative'
    assert number_of_messages > 0, 'number of messages must be greater than 0'
    s = 0
    while s < seconds:
        for i in range(number_of_messages):
            producer.send(topic_name, {"x": random_string(8)}).add_callback(on_send_success).add_errback(on_send_failure)
        try:
            producer.flush()
        except:
            print('Error while flushing')
            break
        s += 1
        sleep(1)

if __name__ == '__main__':
    run(2, 1)
    producer.close()
    

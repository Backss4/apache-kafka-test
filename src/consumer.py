from abc import abstractclassmethod, abstractmethod
from email.message import Message
import json
from time import sleep
import threading
import asyncio
from typing import *

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from kafka.consumer.fetcher import ConsumerRecord


topics_name = ('test-topic')

g_consumer = KafkaConsumer (
    topics_name,
    bootstrap_servers = 'localhost:9092',
    value_deserializer = lambda v: json.loads(v.decode('utf-8'))
)

#for topic in topics_name:
#g_consumer.subscribe(topics_name)

@runtime_checkable
class MessageCallback(Protocol):
    def __call__(self, topic: TopicPartition, msg: ConsumerRecord) -> None: ...


def message_listener(consumer: KafkaConsumer, msg_callback: MessageCallback, empty_callback: Callable[[None], None] = None, error_callback: Callable[[Exception], None] = None):
    # assert isinstance(msg_callback, MessageCallback), 'Bad callback function'
    while True:
        topics = consumer.poll()
        if not topics:
            if empty_callback is not None:
                empty_callback()
        else:
            for topic in topics:
                for msg in topics[topic]:
                    msg_callback(topic, msg)
        sleep(1)


def empty_callback() -> None:
    print('Empty msg')


def msg_callback(topic: TopicPartition, msg: ConsumerRecord) -> None:
    print(type(topic))
    print(type(msg))
    print(f"[MSG] Topic: {msg.topic} | message: {msg.key}")
    for key, value in msg.value.items():
        print(f'[{key} : {value}')


if __name__ == '__main__':
    t_message_listener = threading.Thread(target=message_listener, args=[g_consumer, msg_callback, empty_callback])
    t_message_listener.run()
    t_message_listener.join()
    print('xdd')

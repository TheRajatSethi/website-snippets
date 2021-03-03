"""
Generators.py contains code to consume streaming and processing data from Redis using generators.
"""

import redis
import json


QUEUE_NAME = 'stock_data'
r = redis.Redis()


def consume_redis_data():
    yield r.blpop(QUEUE_NAME)


def raw_data():
    yield from consume_redis_data()


def action(data):
    pass


def save(prediction):
    json.dumps(prediction)


def predictor():
    for data in raw_data():
        yield from action(data)


def driver():
    for prediction in predictor():
        save(prediction)


if __name__ == '__main__':
    driver()

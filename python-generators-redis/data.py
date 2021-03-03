"""
Generates random toy stock data in specific format and posts to Redis.
The prices or volatility listed are made up and not accurate and should
not be used for any purpose except playing around in this tutorial.
"""

import json
import redis
import sys
import random
import time

from collections import namedtuple
from datetime import date

StockData = namedtuple('StockData', ['base_price', 'description', 'volatility_std_dev', 'exchange'])

QUEUE_NAME = "stock_data"
r = redis.Redis()  # Default host="localhost", port=6379, db=0

STOCKS = {
    'APPL': StockData(1000, "Apple", .05, 'NYSE'),
    'GOOG': StockData(1500, "Alphabet Inc", .05, 'NYSE'),
    'ACN': StockData(250, "Accenture Inc", .07, 'NYSE'),
    'RBC': StockData(110, "RBC Royal Bank", .06, 'NYSE'),
    'TD': StockData(70, "TD Bank", .06, 'NYSE'),
    'TSLA': StockData(720, "Tesla", .15, 'NYSE'),
}


def structure_data(symbol: str, description, exchange, date: date, open_price: float, close_price: float, sentiment: list):
    data = {
        'symbol': symbol,
        'description': description,
        'exchange': exchange,
        'date': str(date),
        'open': open_price,
        'close': close_price,
        'sentiment': sentiment
    }
    return data


def price_generator(stock):
    base_mean_price = STOCKS[stock].base_price
    open_price = base_mean_price * random.gauss(1, STOCKS[stock].volatility_std_dev)
    close_price = base_mean_price * random.gauss(1, STOCKS[stock].volatility_std_dev)
    return round(open_price, 2), round(close_price, 2)


def sentiment_generator():
    with open("words.txt") as f:
        words = f.readlines()
        words = [word.strip() for word in words]
        sentiment = [[random.choice(words) for _ in range(random.randint(2, 5))], [random.choice(words) for _ in range(random.randint(2, 8))],
                     random.choice(words), random.choice(words)]
        return sentiment


def data_generator():
    stock = random.choice(list(STOCKS.keys()))
    open_price, close_price = price_generator(stock)
    sentiment = sentiment_generator()
    return stock, STOCKS[stock].description, STOCKS[stock].exchange,date(random.randint(2000, 2020), random.randint(1, 12), random.randint(1, 28)), open_price, close_price, sentiment


def redis_post(data):
    try:
        r.rpush(QUEUE_NAME, json.dumps(data))
    except Exception as e:
        print("Exception in redis post")


def main(data_count: int = 1):
    for _ in range(data_count):
        data = data_generator()
        structured_data = structure_data(*data)
        redis_post(structured_data)
        time.sleep(.5)  # To mimic stream of inbound data


if __name__ == '__main__':
    try:
        data_count = int(sys.argv[1])
        main(data_count)
    except Exception as e:
        main()

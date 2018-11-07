import argparse
import atexit
import json
import logging
import requests
import schedule
import time

from kafka.errors import KafkaError
from kafka import KafkaProducer

# Default kafka topic write to
topic_name = 'analyzer'
# Default kafka broker location
kafka_broker = '127.0.0.1:9092'

logger_format = "%(asctime)-15s %(message)s"
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

API_BASE = 'https://api.gdax.com'

def check_symbol(symbol):
	"""
	Helper method check if the symbol exists in coinbase
	"""
	logger.debug('Checking symbol...')
	try:
		response = requests.get(API_BASE +'/products')
		product_ids = [product['id'] for product in response.json()]

		if symbol not in product_ids:
			logger.warns('The symbol %s is not supported. The list of supported symbols: %s', symbol, product_ids)
			exit()
	except Exception as e:
		logger.warn('Failed to fetch products: %s', e)

def fetch_price(symbol, producer, topic_name):
	"""
	Helper fucntion to retrieve data and send it to kafka
	"""
	logger.debug('Start to fetch price for %s :',symbol)
	try:
		response = requests.get('%s/products/%s/ticker' %(API_BASE, symbol))
		price = response.json()['price']
		timestamp = time.time()
		payload = {
			'Symbol':str(symbol),
			'LastTradePrice':str(price),
			'Timestamp':str(timestamp)
		}

		logger.debug('Retrieved %s from %s', symbol, payload)
		producer.send(topic=topic_name, value=json.dumps(payload).encode('utf-8'))
		logger.debug('Sent price for %s to kafka', symbol)

	except Exception as e:
		logger.warn('Failed to fetch price: %s', e)

def shutdown_hook(producer):
	try:
		producer.flush(10)
	except KafkaError as kafka_error:
		logger.warn('Failed to flush pending message to kafka: %s', kafka_error)
	finally: # no matter flush is successful or not, run below
		try:
			producer.close(10)
		except Exception as e:
			logger.warn('Failed to close Kafka connection: %s', e.message)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol',help='the symbol you want to pull.')
	parser.add_argument('topic_name',help='the kafka topic push to.')
	parser.add_argument('kafka_broker', help='the location of kafka broker.')

	# Parser args
	args = parser.parse_args()
	symbol = args.symbol
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker

	#Check if the symbol is supported
	check_symbol(symbol)

	#Initiate a simple kafka producer
	producer = KafkaProducer(bootstrap_servers = kafka_broker)

	#Schedule and run the fetch_price function every second
	schedule.every(2).seconds.do(fetch_price, symbol, producer, topic_name)

	#Setup proper shutdown hook
	atexit.register(shutdown_hook, producer)

	#while
	while True:
		schedule.run_pending()
		time.sleep(2)

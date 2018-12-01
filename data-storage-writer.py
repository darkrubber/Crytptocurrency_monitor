import argparse
import atexit
import json
import happybase
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Default kafka topic write to
topic_name = 'analyzer'
# Default kafka broker location
kafka_broker = '127.0.0.1:9092'
#Default table in Hbase
data_table = 'crypto_data'

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage-writer')
logger.setLevel(logging.DEBUG)


def shutdown_hook(kafka_consumer, hbase_connection):
	"""
	A shutdown hook to be called before shutdown
	"""
	try:
		kafka_consumer.close()
		hbase_connection.close()
	except Exception as e:
		logger.warn('Failed to close kafka consumer or hbase connection: %s', str(e))


# 1)Row key: Symbol
#				family:price
#BTC_USD		6000 timestamp
#				6100 timestamp
#				6200 timestamp

#2)Row key - Symbol, column -timestamp
#				family:timestamp1	family:tmiestamp2
#BTC-USD			6000					6100

#3)Row key - Symbol + timestamp
#					family:price
#BTC-USD:timestamp1		6000
#	..	:tmiestamp2  	6100
# 	.. : timestamp3 	6200

def persist_data(data, hbase_connection, data_table):
	"""
	Persist data into hbase
	"""
	try:
		logger.debug('Start to persist dat to hbase: %s', data)
		parsed = json.loads(data)
		symbol = parsed.get('Symbol')
		price = parsed.get('LastTradePrice')
		timestamp = parsed.get('Timestamp')

		table =  hbase_connection.table(data_table)
		row_key = "%s-%s" % (symbol, timestamp)
		table.put(row_key, {'family:symbol':symbol,			# can do  'value':data ; throw everything in
							'family:trade_time':timestamp,
							'family:trade_price':price})

		logger.info('Persisted data to hbase for symbol:%s, \
			price: %s, timestamp: %s', symbol, price, timestamp)

	except Exception as e:
		logger.error('Failed to persist data to hbase for %s',str(e))

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name')
	parser.add_argument('kafka_broker')
	parser.add_argument('data_table')
	parser.add_argument('hbase_host')

	#Parse args
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	data_table = args.data_table
	hbase_host = args.hbase_host

	#Initiate a simple kafka consumer
	kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

	#Initiate a hbase connection
	hbase_connection = happybase.Connection(hbase_host)

	#Create table if not exists
	hbase_tables = [table.decode() for table in hbase_connection.tables()]
	if data_table not in hbase_tables:
		hbase_connection.create_table(data_table, {'family':dict()})

	#Step up proper shutdown hook

	atexit.register(shutdown_hook, kafka_consumer, hbase_connection)

	#Start consuming kafka and writing to hbase
	for msg in kafka_consumer:
		persist_data(msg.value ,hbase_connection, data_table)
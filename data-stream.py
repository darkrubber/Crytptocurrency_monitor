import argparse
import atexit
import logging
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

def shutdown_hook(producer):
	"""
	a shutdown hook
	"""
	try:
		logger.info('Flushing pending message to kafka... timeout in 10s')
		producer.flush(10)
		logger.info('Flushing messages completed')
	except KafkaError as kafka_error:
		logger.warn('Failed to flush pending message to kafka, caused by: %s', kafka_error.message)
	finally: # no matter flush is successful or not, run below
		try:
			logger.info('Closing kafka connection...')
			producer.close(10)
		except Exception as e:
			logger.warn('Failed to close Kafka connection, caused by: %s', e.message)

def process_stream(stream, kafka_producer, target_topic):
	
	def construct_pair(data):
		record = json.loads(data)
		return record.get('Symbol'), (float(record.get('LastTradePrice')), 1)

	def send_to_kafka(rdd):
		results = rdd.collect()
		for res in results:
			data = json.dumps({
				'Symbol': res[0],
				'Timestamp': time.time(),
				'Average': res[1]})
		try:
			logger.info("Sending average price %s to Kafka", data)
			kafka_producer.send(target_topic, value=data.encode('utf-8'))
		except KafkaError as err:
			logger.warn("Failed to send average price caused by %s", err.message)

	stream.map(construct_pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda kv: (kv[0], kv[1][0] / kv[1][1])).foreachRDD(send_to_kafka)

if __name__ == '__main__':

	# Setup command line args
	parser = argparse.ArgumentParser()
	parser.add_argument('source_topic')
	parser.add_argument('target_topic')
	parser.add_argument('kafka_broker')
	parser.add_argument('batch_duration', help='batch duration in secs')

	#Parse args
	args = parser.parse_args()
	source_topic = args.source_topic
	target_topic = args.target_topic
	kafka_broker = args.kafka_broker
	batch_duration = int(args.batch_duration)

	#Create spark context and streaming context
	sc = SparkContext("local[2]", "AveragePrice")
	sc.setLogLevel('INFO')
	ssc = StreamingContext(sc, batch_duration)

	# Instantiate a kafka stream for processing
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [source_topic], {'metadata.broker.list':kafka_broker})

	# Extract value from directKafkaStream (key, value) pair
	stream = directKafkaStream.map(lambda msg:msg[1])

	# Instantiate a simple kafka producer
	# write broker = read broker in this example
	kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)
	process_stream(stream, kafka_producer, target_topic)

	# Setup shutdown hook
	atexit.register(shutdown_hook, kafka_producer)

	ssc.start()
	ssc.awaitTermination()  # Exit at Crtl + c

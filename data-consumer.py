import argparse

from kafka import KafkaConsumer

# Default kafka topic write to
topic_name = 'analyzer'
# Default kafka broker location
kafka_broker = '127.0.0.1:9092'

def consumer(topic_name, kafka_broker):
	consumer = KafkaConsumer(topic_name, bootstrap_servers = kafka_broker)

	for message in consumer:
		print(message)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help='Kafka topic to pull from')
	parser.add_argument('kafka_broker', help ='Location of the kafka broker.')

	#Parse args
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker

	consumer(topic_name, kafka_broker)
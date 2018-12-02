# Cryptocurrency_Price_Chart

### With SMACK

---------------

Clean all docker containers:
```
docker rm -f $(docker ps -a -q)
```

Run Zookeeper:
```
docker run -d -p 2181:2181 -p 2888:2888 -p 3888:3888 --name zookeeper confluent/zookeeper
```
Run Confluent Kafka:
```
docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=localhost -e KAFKA_ADVERTISED_PORT=9092 --name kafka --link zookeeper:zookeeper confluent/kafka
```
---------------
Run Spark:
```
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 data-stream.py analyzer average-price 127.0.0.1:9092 5
```
---------------
Run 1)data-producer	2) redis-publisher

Start NodeJs on Port 3000:
```
node index.js --redis_host-localhost --redis_port=6379 --redis_channel=price --port=3000
```
---------------
Run HBase with extras:
```
docker run -d -h myhbase -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301 --name hbase harisekhon/hbase:1.2
```
(add 127.0.0.1 myhbase to /etc/hosts)

---------------
and Hbase with Kafka:
```
docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=localhost -e KAFKA_ADVERTISED_PORT=9092 --link hbase:zookeeper confluent/kafka
```
-----------------

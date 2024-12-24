wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.12-2.8.0.tgz <br>
tar -xzf kafka_2.12-2.8.0.tgz <br>
start_mysql
mysql --host=127.0.0.1 --port=3306 --user=root --password=Mjk0NDQtcnNhbm5h<br>

create database tolldata;<br>
use tolldata;<br>

create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);<br>

exit<br>
python3 -m pip install kafka-python<br>
python3 -m pip install mysql-connector-python==8.0.31<br>

## start ZooKeeper
cd kafka_2.12-2.8.0 <br>
bin/zookeeper-server-start.sh config/zookeeper.properties <br>

## Start the Kafka broker service
cd kafka_2.12-2.8.0
bin/kafka-server-start.sh config/server.properties

## Create a topic

cd kafka_2.12-2.8.0
bin/kafka-topics.sh --create --topic news --bootstrap-server localhost:9092

## Start Producer 
bin/kafka-console-producer.sh --topic news --bootstrap-server localhost:9092

## Start Consumer 
cd kafka_2.12-2.8.0
bin/kafka-console-consumer.sh --topic news --from-beginning --bootstrap-server localhost:9092


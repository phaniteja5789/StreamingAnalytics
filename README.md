# StreamingAnalytics
This project perform Analytics on Streaming Data.

**DataSimulator.py**

Python File generates JSON Messages that are appended to a File with name TemperatureRecorded.txt

Exceution Command:

python DataSimulator.py 100

Total command Line Arguments 2

Argv[0] = File name

Argv[1]=Total Number of JSON Messgaes that are to be generated.


Once the Exceution Command is executed it generates the text file with name TemperatureRecorded.txt in the current working directory.

**Data needs to be sent to Kafka**

**Lists the active topics in kafka cluster**
bin/kafka-topics.sh --list --zookeeper localhost:2181

Zookeeper Running on 2181 Port

**Create a Topic with Name "SensorAnalytics"**
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic SensorAnalytics

**TopicName-SensorAnalytics
Replication Factor-1 Every Partition is replicated by 1
Partitions-2 Topic has 2 partitions
**

**Produce the data into Topic by using below command**
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic SensorAnalytics < TemperatureRecorded.txt 

Now the Data is stored inside Kafka Cluster under Logical Storage(Topic)

**Submit the spark job using Spark-Submit use the below command**

spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.7.jar StreamingMetrics.py

Inside StreamingMetrics.py it connects to Kafka using KafkaUtils class and creates a DSTREAM by subscribing to Topic "SensorAnalytics".

Once the DSTREAM is recieved from the Kafka the RDD Operations are applied.


**Tech Stack used
1.Python 
2.PySpark
3.Kafka
**

#!/usr/bin/python

from __future__ import print_function
from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
import sys
import re
import json


# creation of SparkContext and StreamingContext
sc=SparkContext("local[2]","StreamingJOB")
ssc = StreamingContext(sc, 3)


#creation of Direct Stream in order to connect to Kafka and recieve the message in form topic,msg
kvs = KafkaUtils.createDirectStream(ssc, ["SensorAnalytics"], {"metadata.broker.list": "localhost:9092"})

#converts the Json Data into Python Dictionary using json.loads()
jsonRDD = kvs.map(lambda v: json.loads(v))

print("Started Computations")

print("FirstTask")
#Calculation of Average Temperature recorded by each state and displaying in Descending Order

def generationPairRDD(x):
	return (x["State"],tuple(x["Payload"]["data"]["temperature"],1))

def stateWise(x,y):
	return (x[0]+y[0],x[1]+y[1])
	
def avgTemperature(x):
	return (x[0]/x[1])
	

avgTemperatureRDD=jsonRDD.map(generationPairRDD).reduceByKey(stateWise).mapValues(avgTemperature).sortBy(lambda x:x,True)

avgTemperatureRDD.pprint()

print("SecondTask")
#calculation of Unique Sensors per each state

def generationPair_2(x):
	return (x["State"],x["GUID"])

def sensorWise(x,y):
	unique_items=set(x)
	unique_items.add(y)
	return list(unique_items)
UniqueSensors_State=jsonRDD.map(generationPair_2).reduceByKey(sensorWise).mapValues(lambda x:len(x))
UniqueSensors_State.pprint()

print("ThirdTask")
#calculation of Total Json Data
def total_Messages(a,b):
	return (a+b)
Messages_Count=jsonRDD.map(lambda a:1).reduce(total_Messages)

Messages_Count.pprint()

print("FourthTask")
#calculation of Total Sensors
def count_Sensors(x,y):
	return list(set(x).add(y))
jsonRDD.map(lambda x:x["GUID"]).reduce(count_Sensors).map(lambda a:1).reduce(lambda a,b:a+b)
jsonRDD.pprint()

print("Execution Completed")

#starting the Stream Job
ssc.start()

#Waiting for Termination
ssc.awaitTermination()

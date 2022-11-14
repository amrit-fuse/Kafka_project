from allsecrets import *
import json
import requests
from json import dumps, loads
# dumps converts python object to json string
# loads converts json string to python object

from kafka import KafkaProducer

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('OMDB')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
    .getOrCreate()


# http://www.omdbapi.com/?i=tt3896198&apikey=******* # api key for    OMBD API

parameters = {'t': 'top gun maverick', "apikey": ombd_api}
OMDB_response = requests.get("http://www.omdbapi.com/", params=parameters)


# zookeeper and kafka broker should be running in the background
producer = KafkaProducer(bootstrap_servers=[
                         'localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
# value_serializer=lambda x: dumps(x).encode('utf-8') is used to convert the data into json format

# send the OMDB_response to the topic named OMDB
producer.send('OMDB', value=OMDB_response.json())


# flush the messages to the topic
# flush() is a blocking function which ensures all the messages are sent to the topic
producer.flush()


#The first step is to initialize sparkContext

from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("Intro to Spark") \
      .getOrCreate()
sc=spark.sparkContext

#1 - using parallelize on local collection 

input1=[(36.5,36.5),(37,37),(39,39)]
inputRDD1=sc.parallelize(input1)

inputRDD1.collect()
#2 - from an external datasource 

#2.1 HDFS file system
#Sample Dataset is : resources/data/UCI_Credit_card_csv.zip
inputRDD2_1=sc.textFile("/tmp/credit_data_input/UCI_Credit_Card.csv")




inputRDD2_1.take(2)

#3 - from another rdd 
inputRDD3=inputRDD2_1.map(lambda x: x.split(',')[1])
inputRDD3.take(2)


#Day 3 - Putting together a piece of code to show how 
#Panda's DF can be converted to RDD DF/Spark DF
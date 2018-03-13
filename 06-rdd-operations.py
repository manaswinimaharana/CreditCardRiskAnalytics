
#The first step is to initialize sparkContext

from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("Intro to Spark") \
      .getOrCreate()
sc=spark.sparkContext
#2.1 HDFS file system
#Sample Dataset is : resources/data/UCI_Credit_card_csv.zip
inputRDD2_1=sc.textFile("/tmp/credit_data_input/UCI_Credit_Card.csv")
inputRDD2_1.take(2)

newRDD=inputRDD2_1.map(lambda x: x.split(',')). \
filter(lambda x: "ID" not in x[0])

newRDD.count()

#Now this is not very neat and makes it a bit cumbersum 
#to write code for each and every bit 

from pyspark.sql import Row
schemedRDD = newRDD.map(lambda p: Row(ID=p[0], LIMIT_BAL=int(p[1])))
schemedRDD.take(1)

#We have to explictly send schema, even when it's exists a part of the file
#Next let's explore Dataframes  and see how each this can become
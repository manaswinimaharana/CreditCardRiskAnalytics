
#The first step is to initialize sparkContext

from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("Intro to Spark") \
      .getOrCreate()

sc=spark.sparkContext


## PANDAS DF To Spark DF 

import pandas as pd

#ok for small dataset

df = pd.read_csv("./resources/data/UCI_Credit_Card.csv.zip")

df.head(n=5)
df.count()
# summarize the data
print(df.describe())
# take a look at the standard deviation of each column
print(df.std())

#Now if we want to run distributed operation, we will need to create RDD
#Since the data is local to driver, we will use parallelize 
#But in case of Pandas, we have an implicit conversion method available 

ucc_input=spark.createDataFrame(df)
ucc_input.count()
ucc_input.describe("ID").show()



## Creating DFs from RDDs
inputRDD2_1=sc.textFile("/tmp/credit_data_input/UCI_Credit_Card.csv")
inputRDD2_1.take(2)

inputRDD2_1.toDF()

#Fails as it cannot infer schema
#Explicit schema needs to be provided using a Struct type 
from pyspark.sql import Row
schemedRDD = inputRDD2_1.map(lambda p: Row(ID=p.split(',')[0], LIMIT_BAL=(p.split(',')[1])))
schemedRDD.toDF().take(10)
schemedRDD.toDF().select("ID").take(10)


#Creating DF from HDFS semi/structured data directly using Spark DF APIs
from __future__ import print_function
import sys

inputDf = spark   \
      .read   \
      .option("header", True) \
      .option("inferSchema", True) \
      .csv("/tmp/credit_data_input/UCI_Credit_Card.csv")

inputDf.select("LIMIt_BAL").take(10)



inputDf.describe("LIMIT_BAL").show()


#Next we will explore how to use Mathematical and statistical derivation
#https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html


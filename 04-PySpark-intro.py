
#The first step is to initialize sparkContext

#--Ignore the below 7 lines if you are running this in PySparkShell--
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("Intro to Spark") \
      .getOrCreate()

sc=spark.sparkContext
#-------------------------------------------------------------------
spark.version




# Let's pull a function from previous Python ex 
def mySquare(number):
    return  number*number

input=[36.5,37.5,39] 
F = map(mySquare,input)
for t in F:
  print (t)

#In order to call Spark operation we will create a RDD first 
# Let's convert local variable `input` to RDD

inputRDD=sc.parallelize(input)


#Next decide on what Spark operation we neeed to do what I want to do 
#Here we are just trying to get the squared value for eeach element in RDD 
#So we will need a map operation 

inputRDD.count()

mapRDD=inputRDD.map(mySquare)
mapRDD.take(5)

#make it a little more interesting 
input2=[(36.5,34),(37,33),(39,32)]
inputRDD2=sc.parallelize(input2)
mapRDD2=inputRDD.map(mySquare)
mapRDD2.take(5)

#how lambda is implemented and can help 
mapRDD2=inputRDD2.map(lambda x: mySquare(x[0]))
mapRDD2.take(5)


#let's re-define the function and see what is pickling is 
import numpy as np
def my_sqrt(x):
    return np.sqrt(x)
  


sc.parallelize(range(10)).map(my_sqrt).collect()
# Works because Spark takes care of serializing the closure function 

sc.parallelize([(my_sqrt, i) for i in range(10)]).map(lambda x : x[0](x[1])).collect()
# throws serialization error 
#we can't use broadcast 
#sc.broadcast(my_sqrt())

sc.parallelize([(np.sqrt, i) for i in range(10)]).map(lambda x : x[0](x[1])).collect()
# works as numpy is available on all nodes 

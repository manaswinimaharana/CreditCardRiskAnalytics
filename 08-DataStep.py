
#The first step is to initialize sparkContext

from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("Intro to Spark") \
      .getOrCreate()
sc=spark.sparkContext
spark.version


from __future__ import print_function
import sys

# We will look at how I can read data from two seperate tables and perform a join

# Table1: customer_info (/tmp/credit_data_raw/customer_info/)
# Table2: transaction_info (//tmp/credit_data_raw/transaction_info/)
# Both are in CSV format

customerDF = spark   \
      .read   \
      .format("csv") \
      .option("header", True) \
      .option("inferSchema", True) \
      .load("/tmp/credit_data_raw/customer_info")


transDF = spark   \
      .read   \
      .format("csv") \
      .option("header", True) \
      .option("inferSchema", True) \
      .load("/tmp/credit_data_raw/transaction_info")




# USING SQL statements
# We will need to register the DF as tables


customerDF.createOrReplaceTempView("customerDF")
transDF.createOrReplaceTempView("transDF")

#Sample SQL Commands 
spark.sql("Select count(*) from customerDF").show()
spark.sql("Select ID from transDF limit 10").show()
spark.sql("Select count(distinct(ID)) from transDF limit 10").show()
spark.sql("Select max(LIMIT_BAL) from transDF").show()
spark.sql("Select min(LIMIT_BAL) from transDF").show()

#Filter
spark.sql("Select count(*)  from transDF where PAY_0 < 0 ").show()

#Use this command to get the summary stats on each of these datasets
customerDF.describe().show()

# JOIN the datasets 
transformedDF=spark.sql("Select SEX,EDUCATION,MARRIAGE,AGE,transDF.* from customerDF inner join transDF where customerDF.ID=transDF.ID")
transformedDF.createOrReplaceTempView("transformedDF")


#For persistence
spark.sql("create table gra_demo4 stored as parquet as select case when ID=1 then 10 else EDUCATION end as  from customerDF")
spark.sql("insert into table gra_demo3  select case when ID=1 then 10 else EDUCATION end  from customerDF")




#TO DO
# Add a step to demo writing data directly to HDFS 
#------------------

#PERFORM the same JOIN as above 
#USING transformation operations 
joinedDF=customerDF.join(transDF,customerDF.ID == transDF.ID,"inner"). \
select(transDF.ID,transDF.PAY_AMT1)


#To find function/method details visit
#https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#module-pyspark.sql.functions
#Windowig functions 
#spark.sql("SELECT empno,deptno,sal,lag(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as pre_val FROM emp;
df=spark.read.parquet("/user/hive/warehouse/gra_demo4")
df.registerTempTable("df_sample")
spark.sql("select count(*) from df_sample").count()

spark.sql("select * from customers limit 10").show()




#DEMO STATISTICS using DF(s)


# We will use our original dataset 
inputDF1 = spark   \
      .read   \
      .format("csv") \
      .option("header", True) \
      .option("inferSchema", True) \
      .load("/tmp/credit_data_input/UCI_Credit_Card.csv")

inputDF1.createOrReplaceTempView("inputDF1")

inputDF=spark.sql("Select cast(ID as String) as ID, \
LIMIT_BAL,SEX,EDUCATION,MARRIAGE,AGE,PAY_0, \
PAY_2,PAY_3,PAY_4,PAY_5 ,\
PAY_6,BILL_AMT1,BILL_AMT2, \
BILL_AMT3,BILL_AMT4,BILL_AMT5,BILL_AMT6, \
PAY_AMT1,PAY_AMT2,PAY_AMT3,PAY_AMT4,PAY_AMT5,\
PAY_AMT6, `default.payment.next.month` as default from inputDF1")
inputDF.createOrReplaceTempView("inputDF")


## Next we will explore how to use Mathematical and statistical derivation


# 1.Summary and Descriptive Statistics 
#------------------------------------------------------
#   The function describe returns a DataFrame containing information 
#   such as number of non-null entries (count),mean, standard deviation, 
#   and minimum and maximum value for each numerical column
#If you have a DataFrame with a large number of columns,
#you can also run describe on a subset of the columns:

inputDF.select("LIMIT_BAL","PAY_AMT1","SEX","EDUCATION","MARRIAGE","BILL_AMT1",   \
               "BILL_AMT2","BILL_AMT3","BILL_AMT4","BILL_AMT5","BILL_AMT6","PAY_0","PAY_2").describe().show()


#you can also use SQL
spark.sql("select mean(LIMIT_BAL),max(LIMIT_BAL),min(LIMIT_BAL) from inputDF").show()
#---------------------------------------------------------


# 2.Sample covariance and correlation 
#------------------------------------------------------
#Covariance is a measure of how two variables change with respect to each other. 
#A positive number would mean that there is a tendency that as one variable increases, 
#the other increases as well. A negative number would mean that as one variable increases, 
#the other variable has a tendency to decrease.
#The sample covariance of two columns of a DataFrame can be calculated as follows:

inputDF.stat.cov('PAY_2', 'PAY_3')

# Correlation is a normalized measure of covariance that is easier to understand, as 
# it provides quantitative measurements of the statistical dependence 
# between two random variables.

inputDF.stat.corr('PAY_2', 'PAY_3')

#Cross Tabulation 
#Cross Tabulation provides a table of the frequency distribution
#for a set of variables.Cross-tabulation is a powerful tool 
#in statistics that is used to observe the statistical significance 
#(or independence) of variables
#from pyspark.sql.functions import col , column
inputDF.stat.crosstab("EDUCATION", "PAY_0").show()
inputDF.stat.crosstab("SEX", "default").show()
inputDF.stat.crosstab("MARRIAGE", "default").show()


#Frequent Items
#Figuring out which items are frequent in each 
#column can be very useful to understand a dataset
inputDF.stat.freqItems(["MARRIAGE", "default"],0.4).show()

#Mathematical Functions
#a suite of mathematical functions. 
#Users can apply these to their columns with ease
#The inputs need to be columns functions that take a single argument, 
#such as cos, sin, floor, ceil. For functions that take two arguments as input,
#such as pow,hypot, either two columns or a combination of a double and column can be supplied.



#https://spark.apache.org/docs/2.2.0/ml-statistics.html
#ML Basic Statistics 
#Calculating the correlation between two series of data 
#is a common operation in Statistics. 
#In spark.ml we provide the flexibility to calculate pairwise
#correlations among many series. 
#The supported correlation methods are currently Pearson’s and Spearman’s 
#correlation.


#unfortunately my demo environment is running on Spakr 2.1.0,
#hence cannot demonstarte these
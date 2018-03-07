# We can read this sample dataset sitting in the local file system using variouc methods 
# write a lot of nativ python code 


# Using the Pandas
import pandas as pd 
"""file = r'resources/data/UCI_Credit_Card.csv.zip'
df = pd.read_csv(file)
df1=df[['ID','LIMIT_BAL']]
print(df1.head(n=100)) """


import pylab as pl
import numpy as np
#Read data from Spark

import statsmodels.formula.api as sm


df = pd.read_csv("resources/data/UCI_Credit_Card.csv.zip")

# summarize the data
print(df.describe())
  

# take a look at the standard deviation of each column
print(df.std())


# frequency table cutting presitge and whether or not someone was admitted
#print pd.crosstab(df['admit'], df['prestige'], rownames=['admit'])
# prestige   1   2   3   4
# admit                   
# 0         28  97  93  55
# 1         33  54  28  12

# plot all of the columns
#df.hist()


####sklearn 


import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.cross_validation import train_test_split, cross_val_score
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import roc_curve, auc, classification_report, confusion_matrix, accuracy_score
from sklearn.feature_selection import RFECV
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline

df = pd.read_csv('resources/data/UCI_Credit_Card.csv.zip', index_col='ID')
df.head()
df.dtypes
df.shape
df.isnull().sum()


# rename column for easy reference

df.rename(columns={"default.payment.next.month": "default"}, inplace=True)
df.columns=df.columns.str.lower()

print(df.columns)


# check the benchmark

float(df.default.sum())/len(df.default)

# by looking at the column names, there might be multicollinearity issues here,
# so check the correlation matrix to confirm

df.corr()
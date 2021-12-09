import pysparkimport sys
import json
import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.setAppName("your project name")
sc = SparkContext(conf=conf)
stream = StreamingContext(sc, 2)

datastream = stream.socketTextStream("localhost", 6100)

x = datastream.flatMap(lambda datastream: json.loads(datastream))

x.pprint()

stream.start()
stream.awaitTermination()
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

conf = SparkConf()
conf.setAppName("your project name")
sc = SparkContext(conf=conf)
stream = StreamingContext(sc, 2)

datastream = stream.socketTextStream("localhost", 6100)

x = datastream.flatMap(lambda datastream: json.loads(datastream))

x.pprint()

stream.start()
stream.awaitTermination()
spark = pyspark.sql.SparkSession.builder.appName("clipper-pyspark").getOrCreate()

sc = spark.sparkContext

%matplotlib inline
np.random.seed(60)
%%sh
#Let see the first 5 rows
head -5 train.csv
from pyspark.sql.functions import col, lower
df = spark.read.format('csv')\
          .option('header','true')\
          .option('inferSchema', 'true')\
          .option('timestamp', 'true')\
          .load('train.csv')

data = df.select(lower(col('Category')),lower(col('Descript')))\
        .withColumnRenamed('lower(Category)','Category')\
        .withColumnRenamed('lower(Descript)', 'Description')
data.cache()
print('Dataframe Structure')
print('----------------------------------')
print(data.printSchema())
print(' ')
print('Dataframe preview')
print(data.show(5))
print(' ')
print('----------------------------------')
print('Total number of rows', df.count())
def top_n_list(df,var, N):
    '''
    This function determine the top N numbers of the list
    '''
    print("Total number of unique value of"+' '+var+''+':'+' '+str(df.select(var).distinct().count()))
    print(' ')
    print('Top'+' '+str(N)+' '+'Crime'+' '+var)
    df.groupBy(var).count().withColumnRenamed('count','totalValue')\
    .orderBy(col('totalValue').desc()).show(N)
    
    
top_n_list(data, 'Category',10)
print(' ')
print(' ')
top_n_list(data,'Description',10)
data.select('Category').distinct().count()

training, test = data.randomSplit([0.7,0.3], seed=60)
#trainingSet.cache()
print("Training Dataset Count:", training.count())
print("Test Dataset Count:", test.count())
stream.awaitTermination()
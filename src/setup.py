import kaggle
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import psycopg2

# Download the data
kaggle.api.authenticate()
kaggle.api.dataset_download_files(dataset='garystafford/environmental-sensor-data-132k', path='C:/kafka/data', unzip=True)

spark = (
    SparkSession
    .builder
    .appName('Setting Up')
    .master('local[*]')
    .getOrCreate()
)

df = spark.read.option('header','true').csv('data/iot_telemetry_data.csv')

# Timestamp Column
df = df.withColumn('ts', F.col('ts').cast('double'))
df = df.withColumn('ts', F.col('ts').cast('timestamp'))
# Numerical Columns
df = df.withColumn('humidity', F.col('humidity').cast('double'))
df = df.withColumn('lpg', F.col('lpg').cast('double'))
df = df.withColumn('smoke', F.col('smoke').cast('double'))
df = df.withColumn('temp', F.col('temp').cast('double'))
df = df.withColumn('co', F.col('co').cast('double'))
df = df.withColumn('humidity', F.col('humidity').cast('double'))
# Boolean Columns
df = df.withColumn('light', F.col('light').cast('boolean'))
df = df.withColumn('motion', F.col('motion').cast('boolean'))

# Filter depending on the 3 devices we have
machine_1 = df.filter(F.col('device')=='b8:27:eb:bf:9d:51') # stable conditions, warmer and dryer
machine_2 = df.filter(F.col('device')=='1c:bf:ce:15:ec:4d') # highly variable temperature and humidity
machine_3 = df.filter(F.col('device')=='00:0f:00:70:91:0a') # stable conditions, cooler and more humid

machine_1.write.json('C:/kafka/data/machine_1', mode='overwrite')
machine_2.write.json('C:/kafka/data/machine_2', mode='overwrite')
machine_3.write.json('C:/kafka/data/machine_3', mode='overwrite')

spark.stop()
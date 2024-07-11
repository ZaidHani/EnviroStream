# 
import threading
import subprocess

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DoubleType, BooleanType

spark = (
    SparkSession
    .builder
    .appName('EnviroStream')
    .config('spark.streaming.stopGracefullyOnShutdown', True)
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') # scala version 2.12, then pyspark version 3.5.0
    .config('spark.jars', 'config/postgresql-42.7.3.jar')
    .config('spark.sql.shuffle.partitions', 4)
    .master('local[*]')
    .getOrCreate()
)

schema = StructType([
    StructField('ts', TimestampType()),
    StructField('device', StringType()),
    StructField('co', DoubleType()),
    StructField('humidity', DoubleType()),
    StructField('light', BooleanType()),
    StructField('lpg', DoubleType()),
    StructField('motion', BooleanType()),
    StructField('smoke', DoubleType()),
    StructField('temp', DoubleType()),
])

df = (
    spark
    .readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', 'localhost:9092')
    .option('subscribe', 'machine-1,machine-2,machine-3')
    .option('startingOffsets', 'latest')
    .load()
)

# simple transformation for the values of the json string before being loaded
value_df = df.select(
    F.from_json(
        F.col('value').cast('string'),
        schema
    )
)

value_df = value_df.withColumnRenamed('from_json(CAST(value AS STRING))','value')

# writer query, edit this so the data could be inserted into sql server database, nvm let's try postgres first
user_name='postgres'
db_password='anon'
def _write_streaming(df, epoch_id) -> None:
    df.write \
    .mode('append') \
    .format("jdbc") \
    .option("url", 'jdbc:postgresql://localhost:5432/envirostream') \
    .option('dbtable', 'sensor') \
    .option("driver", "org.postgresql.Driver") \
    .option("user", user_name) \
    .option("password", db_password) \
    .save()

final = (
    value_df.select('value.ts', 'value.device', 'value.co', 'value.humidity', 'value.light', 'value.lpg', 'value.motion', 'value.smoke', 'value.temp')
    .writeStream
    .foreachBatch(_write_streaming)
    .option("checkpointLocation", "logs/checkpoint")
    #.trigger(processingTime='1 minute')
    .start()
    )

def run_script(script_name):
    subprocess.run(['python', script_name])

if __name__ == "__main__":
    producer1_thread = threading.Thread(target=run_script, args=('src/producers/machine-1.py',))
    producer2_thread = threading.Thread(target=run_script, args=('src/producers/machine-2.py',))
    producer3_thread = threading.Thread(target=run_script, args=('src/producers/machine-3.py',))
    consumer_thread =  threading.Thread(target=final.awaitTermination)
    
    producer1_thread.start()
    producer2_thread.start()
    producer3_thread.start()
    consumer_thread.start()
    
    producer1_thread.join()
    producer2_thread.join()
    producer3_thread.join()
    consumer_thread.join()
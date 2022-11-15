import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import statistics

spark = SparkSession.builder\
            .master("local")\
            .appName("HDFSToMongoDB")\
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

  

#df_withouAvg = spark.read.option("multiline","true").json("/myTest/fitness_data_500.json")
#df_withouAvg = spark.read.option("multiline","true").json("/fitnessJsonData_s.json")
df_withouAvg = spark.read.option("multiline","true").json("fitness_data_500_data_speed.json")
array_mean = udf(lambda x: float(statistics.mean(x)), FloatType())
df2 = df_withouAvg.withColumn('mean_heart_rate', array_mean('heart_rate'))
df2=df2.drop('heart_rate')
df2=df2.withColumn("heart_rate_min", df2.mean_heart_rate/10)
df2=df2.withColumn("heart_rate_min", df2.heart_rate_min.cast('int'))
df2=df2.withColumn("heart_rate_min", df2.heart_rate_min*10)
df2=df2.withColumn("heart_rate_max", df2.heart_rate_min+10)
df3=df2.select("gender","heart_rate_min","heart_rate_max","sport")
df3=df3.groupBy("gender","heart_rate_min","heart_rate_max","sport").agg(count("sport").alias("sportCount"))

df3.select("gender","heart_rate_min","heart_rate_max","sport","sportCount").write\
    .format('com.mongodb.spark.sql.DefaultSource')\
    .mode('overwrite')\
    .option( "uri", "mongodb://localhost:27017/local.Heart_Rate") \
    .save()

df3.show()
df_speed = df_withouAvg.withColumn('mean_speed', array_mean('speed'))
df_speed=df_speed.drop('speed')
df_speed=df_speed.withColumn("min_speed_range", df_speed.mean_speed/10)
df_speed = df_speed.withColumn("min_speed_range", df_speed.min_speed_range.cast('int'))
df_speed = df_speed.withColumn("min_speed_range", df_speed.min_speed_range*10)
df_speed = df_speed.withColumn("max_speed_range", df_speed.min_speed_range+10)
df_speed = df_speed.select("gender","min_speed_range","max_speed_range","sport")
df_speed=df_speed.groupBy("gender","min_speed_range","max_speed_range","sport").agg(count("sport").alias("sportCount"))


df_speed.select("gender","min_speed_range","max_speed_range","sport","sportCount").write\
    .format('com.mongodb.spark.sql.DefaultSource')\
    .mode('overwrite')\
    .option("uri","mongodb://localhost:27017/local.Speed") \
    .save()
df_speed.show()





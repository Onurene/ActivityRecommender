import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql import SQLContext
import json
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.functions import count
import statistics

from flask import Flask,render_template, request
import json
import subprocess

#initializing flask application
app  = Flask(__name__)

#route to load recommendations homepage
@app.route('/')
def ui():
    return render_template('UI.html')

#route to receive data from frontend and process it in spark 
@app.route('/ProcessUserInfo/<string:userinfo>',methods = ['POST','GET'])
def getValue(userinfo):
    #userinfo - data received from frontend
    userinfo = json.loads(userinfo)
    print(userinfo) 
    
    # Create a SparkSession
    spark = SparkSession.builder\
            .master("local")\
            .appName("SparkJobToMongoDB")\
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    
    print(spark.version) #3.3.1
    global result
    #------------------------------user input-------------------------------

    heartInfo,speedInfo,genderInfo = userinfo['avgHrate'], userinfo['avgSpeed'], userinfo['selectedGender']
        
    #step 1 - create dF for input received
    userInputdf = spark.createDataFrame([(heartInfo,speedInfo,genderInfo)],["heartInfo","speedInfo","genderInfo"])
    print("------------------------User input received--------------------------")
    userInputdf.show()
    
    #step 2 - find minimum and maximum range for given avg 
    userInputdf=userInputdf.withColumn("heart_rate_min", userInputdf.heartInfo/10)
    userInputdf=userInputdf.withColumn("heart_rate_min", userInputdf.heart_rate_min.cast('int'))
    userInputdf=userInputdf.withColumn("heart_rate_min", userInputdf.heart_rate_min*10)
    userInputdf=userInputdf.withColumn("heart_rate_max", userInputdf.heart_rate_min+10)

    userInputdf=userInputdf.withColumn("speed_min", userInputdf.speedInfo/10)
    userInputdf=userInputdf.withColumn("speed_min", userInputdf.speed_min.cast('int'))
    userInputdf=userInputdf.withColumn("speed_min", userInputdf.speed_min*10)
    userInputdf=userInputdf.withColumn("speed_max", userInputdf.speed_min+10)

    print("------------------------Range found--------------------------")

    userInputdf.show()

    heart_rate_min = userInputdf.select("heart_rate_min").collect()[0].asDict()["heart_rate_min"]
    heart_rate_max = userInputdf.select("heart_rate_max").collect()[0].asDict()["heart_rate_max"] 
    speed_min = userInputdf.select("speed_min").collect()[0].asDict()["speed_min"] 
    speed_max = userInputdf.select("speed_max").collect()[0].asDict()["speed_max"] 
    
    #step 4 - check if activity recommendation data is available in intermediate mongo table
    #read data from mongodb 
    cacheData = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                        .option("uri", "mongodb://127.0.0.1/local.cacheRangeActivity")\
                        .load()
    
    sqlC = SQLContext(spark)
    
    cacheData.createOrReplaceTempView("cacheData")
    print("------------------------Read data available in cacheData--------------------------")
    cacheData.show()

    query = "SELECT sport from cacheData where minHeart = {} and maxHeart = {}  and minSpeed = {} and maxSpeed = {} and gender = '{}'"\
          .format(heart_rate_min,heart_rate_max,speed_min,speed_max,genderInfo)
        
    cacheResult = sqlC.sql(query)
    print("------------------------Query output to check if data available in cacheResult------------ ")

    cacheResult.show()

    
    if cacheResult.count()>0:  #data available hence return to user 
      print("------------------------Requested data available in cache------------------------- ")
      cacheResult.show()
      final_data = cacheResult.select('sport').distinct().collect()
      
      result = ""
      for row in final_data:
        result = row['sport']+","+result
      result = result.strip(",")
      return result
    else:
      #no data available in intermediate mongo tables - find in base mongo table
      print("------------------------Requested activity not avaialable--------------------------\n")
      print("------------------------Proceeding to check Base mongo tables----------------------")

      heartRatebaseTableData = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                        .option("uri", "mongodb://127.0.0.1/local.Heart_Rate")\
                        .load()
      heartRatebaseTableData.createOrReplaceTempView("heartRatebaseTableData")
      
      queryHeartInfo = "SELECT heart_rate_min,heart_rate_max,gender,sport,sportCount \
                from heartRatebaseTableData where heart_rate_min = {} \
                and heart_rate_max = {} \
                and gender = '{}' \
                order by sportCount desc"\
              .format(heart_rate_min,heart_rate_max,genderInfo)

      heartResult = sqlC.sql(queryHeartInfo)

      speedbaseTableData = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                        .option("uri", "mongodb://127.0.0.1/local.Speed")\
                        .load()
      speedbaseTableData.createOrReplaceTempView("speedbaseTableData")

      querySpeedInfo = "SELECT min_speed_range,max_speed_range,gender,sport,sportCount\
                  from speedbaseTableData where min_speed_range =  {} and max_speed_range = {} \
                  and gender = '{}' \
                  order by sportCount desc"\
              .format(speed_min,speed_max,genderInfo)
      
      speedResult = sqlC.sql(querySpeedInfo)

      print("------------------------Activities recommended for given heartRate----------------------")

      heartResult.show()

      print("------------------------Activities recommended for given Speed--------------------------")

      speedResult.show()
      
      finalResult = heartResult.unionByName(speedResult,allowMissingColumns=True).distinct()

      print("------------------------Union of the Recommendations-----------------------------------")

      finalResult.show()
      
      final_data = finalResult.select('sport').distinct().collect()
      
      result = ""
      for row in final_data:
        result = row['sport']+","+result
      result = result.strip(",")
      if result == "":
        print('No result found for the given set of values in the dataset')
        result =  'No result found for the given set of values in the dataset'
        return result
      else:
        print("------------------------Final unique list of activities--------------------------------")
        print('\n\n')
        print(result)
        print('\n\n')
        
        
        #store this info in cache table
        df = spark.createDataFrame([(speed_min,speed_max,heart_rate_min,heart_rate_max,genderInfo,result)],
                                  ["minSpeed","maxSpeed","minHeart","maxHeart","gender","sport"])

        print("------------------------Data stored in Intermediate mongo collections for future reference-----------------------")

        df.show()
        df.write\
          .format('com.mongodb.spark.sql.DefaultSource')\
          .mode('append')\
          .option("uri","mongodb://localhost:27017/local.cacheRangeActivity") \
          .save()
        
      
      return result

#route to return activity recommendations to the frontend
@app.route('/result')
def output():
    return render_template('result.html', data=result)

#run flask job
if __name__=="__main__":
    app.run(debug=True)
    
      
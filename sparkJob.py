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

app  = Flask(__name__)

@app.route('/')
def ui():
    return render_template('UI.html')


@app.route('/ProcessUserInfo/<string:userinfo>',methods = ['POST','GET'])
def getValue(userinfo):
    userinfo = json.loads(userinfo)
    print(userinfo) 
    
    # Create a SparkSession
    spark = SparkSession.builder\
            .master("local")\
            .appName("SparkJobToMongoDB")\
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    
    print(spark.version)
    global result
    #------------------------------user input-------------------------------

    heartInfo,speedInfo,genderInfo = userinfo['avgHrate'], userinfo['avgSpeed'], userinfo['selectedGender']
    
    '''
    heartInfo = [130,131,134,135,136,132,130,138,130,138]
    speedInfo = [20,20,21,24,23,25,26,21,23,21]
    genderInfo = "male"
    '''
    '''heartInfo = [110,111,124,115,116,112,110,118,110,118] 
    speedInfo = [25,26,23,24,23,27,26,25,23,23]  
    genderInfo = "male"'''

    
    #step 1 - create dF for given input
    userInputdf = spark.createDataFrame([(heartInfo,speedInfo,genderInfo)],["heartInfo","speedInfo","genderInfo"])
    print("------------------------User input received--------------------------")
    userInputdf.show()
    
    #step 2 - find range for given avg 
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
    
    #step 4 - check if data available in intermediate table
    #read data from mongodb 
    cacheData = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                        .option("uri", "mongodb://127.0.0.1/local.cacheRangeActivity")\
                        .load()
    
    sqlC = SQLContext(spark)
    
    cacheData.createOrReplaceTempView("cacheData")
    print("------------------------Read data available in Cache--------------------------")
    cacheData.show()

    query = "SELECT sport from cacheData where minHeart = {} and maxHeart = {}  and minSpeed = {} and maxSpeed = {} and gender = '{}'"\
          .format(heart_rate_min,heart_rate_max,speed_min,speed_max,genderInfo)
        
    cacheResult = sqlC.sql(query)
    print("------------------------Query output to check if data available in cache------------ ")

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
      #no data available in cache - find in base table
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

        print("------------------------Data stored in Cache table for future reference-----------------------")

        df.show()
        df.write\
          .format('com.mongodb.spark.sql.DefaultSource')\
          .mode('append')\
          .option("uri","mongodb://localhost:27017/local.cacheRangeActivity") \
          .save()
        
      
      
      return result

@app.route('/result')
def output():
    return render_template('result.html', data=result)


if __name__=="__main__":
    app.run(debug=True)
    
      
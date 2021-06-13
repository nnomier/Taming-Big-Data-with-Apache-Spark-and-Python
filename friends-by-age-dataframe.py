from pyspark.sql import SparkSession
from pyspark.sql import functions as func
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()


friends_age= people.select("age","friends")

print("avg age")
friends_age.groupBy("age").agg(func.round(func.avg("friends"),2).alias('avg friends')).sort('age').show()

spark.stop()

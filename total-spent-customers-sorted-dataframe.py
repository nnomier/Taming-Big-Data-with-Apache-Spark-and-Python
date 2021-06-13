from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("SpendByCustomerSortedDataFrame").getOrCreate()

schema = StructType([ \
                     StructField("customerID", StringType(), True), \
                     StructField("itemID", IntegerType(), True),  \
                     StructField("amountSpent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()

customerSpending = df.select("customerID", "amountSpent")

total_by_customer= customerSpending.groupBy('customerID').agg(func.round(func.sum('amountSpent'),2).alias('total-spent'))

total_by_customer.show()


total_by_customer.sort('total-spent').show()

spark.stop()




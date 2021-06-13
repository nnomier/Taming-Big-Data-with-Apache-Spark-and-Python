from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpent")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")

def parseLine(line):
    fields = line.split(',')
    customer = int(fields[0])
    price = float(fields[2])
    return (customer, price)

rdd = lines.map(parseLine)

totals = rdd.reduceByKey(lambda x, y: x+y)
totals_sorted = totals.map(lambda x:(x[1],x[0])).sortByKey()

results = totals_sorted.collect()
for result in results:
    print(result[1],result[0])



from operator import add
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

data = sc.parallelize(list("Hello World"))

count = data.map(lambda x:(x,1)).reduceByKey(add).sortBy(lambda x:x[1],ascending=False).collect()

for word,count in count:
    print(f"{word},{count}")


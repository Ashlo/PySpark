import pandas as pd

data = [['Ashu',25],['Tejal',26],['abc',89]]

#creating the pandas dataframe
pandasDF = pd.DataFrame(data,columns=['Name','Age'])

from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

sparkDF = spark.createDataFrame(pandasDF)
sparkDF.printSchema()
sparkDF.show()

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

mySchema = StructType([StructField("First Name",StringType(),True),StructField("Age",IntegerType(),True)])


sparkDF2 = spark.createDataFrame(pandasDF,schema=mySchema)
sparkDF2.printSchema()
sparkDF2.show()


spark.conf.set("spark.sql.execution.arrow.enabled","true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")

pandasDF2=sparkDF2.select("*").toPandas
print(pandasDF2)


test=spark.conf.get("spark.sql.execution.arrow.enabled")
print(test)

test123=spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled")
print(test123)

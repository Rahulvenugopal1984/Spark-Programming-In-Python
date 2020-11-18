
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType
if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("readfiles") \
        .master("local[2]") \
        .getOrCreate()

## Sampling from big files
    logDF = spark.read \
        .option("header", True) \
        .csv("/Users/rahulvenugopalan/Documents/bigDBFS.csv") \
        .sample(withReplacement=False, fraction=0.3, seed=3)

    logDF.show(10)

## Reading files  - Different ways
    # Type 1

    smartphoneDF = spark.read.json("/Users/rahulvenugopalan/Downloads/sampledb2.txt")
    smartphoneDF.show(10)
    smartphoneDF.printSchema()
    smartphoneDFschema = smartphoneDF.schema
    # Type 2
    smartphoneDF2 = spark.read \
        .format("json") \
        .load("/Users/rahulvenugopalan/Downloads/sampledb2.txt")

    smartphoneDF2.show(10)


schemaSMS = StructType([
    StructField("SMS", StringType(), True)
])


# Here is the full schema as well
fullSchema =  StructType([
    StructField("SMS", StructType([
    StructField("Address",StringType(),True),
    StructField("date",StringType(),True),
    StructField("metadata", StructType([
    StructField("name",StringType(), True)
    ]), True),
  ]), True)
])
# Type 3  - Read using Schema and filter
## Filter using SQL expression
SMSDF = (spark.read
  .schema(schemaSMS)
  .json("/Users/rahulvenugopalan/Downloads/sampledb2.txt")
  .filter("SMS is not null")
)
# Filter using column
SMSDFullschema = (spark.read
  .schema(fullSchema)
  .json("/Users/rahulvenugopalan/Downloads/sampledb2.txt")
  .filter(f.col("SMS").isNotNull())
)

SMSDF.show()
SMSDFullschema.show()

SMSDFullschema.select('SMS.Address','SMS.date','SMS.metadata.name').show(truncate=False)


spark.stop()

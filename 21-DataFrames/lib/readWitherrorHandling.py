
from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType
if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("readfiles") \
        .master("local[2]") \
        .getOrCreate()
# Types of error handling while reading.

#   PERMISSIVE	    -   Includes corrupt records in a "_corrupt_record" column (by default)
#   DROPMALFORMED	-   Ignores all corrupted records
#   FAILFAST	    -   Throws an exception when it meets corrupted records

data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

smartphoneDF = spark.read.json("/Users/rahulvenugopalan/Downloads/samplecorrupt.txt")
smartphoneDF.show(10)

sc=spark.sparkContext
#   PERMISSIVE	    -   Includes corrupt records in a "_corrupt_record" column (by default)
corruptDF = (spark.read
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record2")
  .json("/Users/rahulvenugopalan/Downloads/samplecorrupt.txt")
)

(corruptDF).show()
#   DROPMALFORMED	-   Ignores all corrupted records
corruptDF2 = (spark.read
  .option("mode", "DROPMALFORMED")
  .json(sc.parallelize(data))
)

(corruptDF2).show()

#   FAILFAST	    -   Throws an exception when it meets corrupted records

try:
    data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

    corruptDF3 = (spark.read
                 .option("mode", "FAILFAST")
                 .json(sc.parallelize(data))
                 )
    (corruptDF3).show()

except Exception as e:
    print(e)
##


spark.stop()

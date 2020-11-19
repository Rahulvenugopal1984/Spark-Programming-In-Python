
from pyspark.sql import *
from pyspark.sql.functions import to_timestamp, unix_timestamp, hour, from_utc_timestamp
from pyspark.sql.types import *
from pyspark.sql import functions as f

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("dataframeswithDateformat") \
        .master("local[2]") \
        .getOrCreate()

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDatestr", StringType()),
        StructField("EventTimestampstr", StringType())
    ])

    my_rows_date = [
                    Row("123", "04/05/2020","2020-02-28 12:30:00"),
                    Row("124", "3/05/2020","2020-02-28 12:30:00" ),
                    Row("125", "04/05/2020","2020-02-28 12:30:00"),
                    Row("126", "04/05/2020","2020-02-28 12:30:00")
                    ]

    my_rdd_list = spark.sparkContext.parallelize(my_rows_date)
    print(type(my_rdd_list))
    my_df_list= spark.createDataFrame(my_rdd_list, my_schema)
    my_df_list.show()

## Scenarios  : -
    # 1) Convert string to Date
    # 2) Convert string to Timestamp
    # 3) Convert string to Unixtimestamp with and without casting to timestamptype()
    # 4) Convert string to get hours/minutes
    # 5) convert into a different time zone - GMT/UTC to EST


    dfwithDate = my_df_list.withColumn("EventDateStringToDateType", f.to_date(f.col("EventDatestr"), 'M/d/yyyy'))\
                           .withColumn("EventDateStringToTimestampType",to_timestamp("EventTimestampstr", 'yyyy-MM-dd HH:mm:ss')) \
                           .withColumn("EventDateStringToUnixtimestampTypeWithCast", unix_timestamp("EventTimestampstr", 'yyyy-MM-dd HH:mm:ss').cast(TimestampType())) \
                           .withColumn("EventDateStringToUnixtimestampType",unix_timestamp("EventTimestampstr", 'yyyy-MM-dd HH:mm:ss')) \
                           .withColumn("EventDatehoursESTformat", hour(from_utc_timestamp("EventTimestampstr", 'EST')))\
                           .withColumn("EventDatehoursUTCformat", hour(to_timestamp("EventTimestampstr", 'yyyy-MM-dd HH:mm:ss')))

    dfwithDate.printSchema()
    dfwithDate.show()

spark.stop()

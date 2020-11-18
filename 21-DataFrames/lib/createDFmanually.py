
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("createDataframes") \
        .master("local[2]") \
        .getOrCreate()

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDatestr", StringType())
    ])

 ## Method 1 : - Creating dataframes using Dataset of row objects with createDataFrame.

    my_rows_date = [Row("123", "04/05/2020"),
               Row("124", "3/05/2020" ),
               Row("125", "04/05/2020"),
               Row("126", "04/05/2020")
             ]

    my_rdd_list = spark.sparkContext.parallelize(my_rows_date)
    print(type(my_rdd_list))
    my_df_list= spark.createDataFrame(my_rdd_list, my_schema)
    my_df_list.show()

 ## Method 2 : - Creating dataframes using Dataset of row objects with toDF.
    columnName=["id","EventDate"]
    dfnew_toDF=my_rdd_list.toDF(columnName)
    dfnew_toDF.show()

spark.stop()
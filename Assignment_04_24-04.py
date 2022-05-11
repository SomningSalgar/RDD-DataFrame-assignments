from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Assignment_04").master("local[*]").getOrCreate()

    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]
    columns = ["firstname", "lastname", "country", "state"]
    # df = spark.createDataFrame(data=data, schema=columns)
    # df.printSchema()
    # df.show(truncate=False)  # shows all columns data
    from pyspark.sql.functions import col,lit
    # df.withColumn("state",col("state").cast("Integer")).printSchema()

    ## (1) Create df and write df values in csv,json,parquet,orc format in various ways

    # df.write.mode('overwrite').csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Output\datacsv2")
    # df.write.format("csv").mode('overwrite').save(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Output\datacsv")

    # df.write.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Output\datacsv")
    # df.write.json(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Output\datajson")
    # df.write.orc(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Output\dataorc")
    # df.write.parquet(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Output\dataparquet")

    ## (2) apply select() on df in various ways
    # df.select("*").show()
    # df.select('firstname','state').show()
    # df.select(['firstname','state']).show()
    # df.select(col('firstname'),col('state')).show()
    # df.select(col('firstname').alias("fname"),col('state').alias("st")).show()

    ## (3) apply withColumn() on df in various ways
    # df.withColumn("salary",col("salary")*10).show()   # we can change value
    # df.withColumn("deptno",col("deptno").cast("String")).show()  # we can change datatype
    # df.withColumn("Designation",lit("Engineer")).show()   # add new column
    # df.withColumn("CopiedColumn", col("state") * -1).show()

    # data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
    #         ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
    #         ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
    #         ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
    #         ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
    #         ]
    #
    # columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    # df2 = spark.createDataFrame(data=data, schema=columns)
    #
    # df2.withColumn("CopiedColumn", col("salary") *100).show()

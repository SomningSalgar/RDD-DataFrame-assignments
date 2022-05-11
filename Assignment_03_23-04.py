from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':

    spark=SparkSession.builder\
            .appName("Assignment_02")\
            .config("spark.driver.bindAddress","localhost")\
            .config("spark.ui.port","4050")\
            .master("local[*]")\
            .getOrCreate()
    # print(spark)

    ## (1) avg salary per department

    # First we have to create RDD
    # rdd=spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.txt")
    # rdd1=rdd.map(lambda x:x.split(","))
    # print(rdd1.collect())

    # Create DataFrame   --->default it will take column names(simple DF)
    # df=spark.createDataFrame(rdd1)

    # Create DF with column name   --> it will take column names as mentioned
    # df=spark.createDataFrame(data=rdd1).toDF('id','fname','lname','age','gender','deptno','salary')
    # df.printSchema()
    # df.show()

    # df1=df.withColumn("salary",col("salary").cast("Integer")).withColumn("deptno",col("deptno").cast("Integer"))
    # df1.groupBy("deptno").avg("salary").sort("deptno").show()
    # df1.groupBy("deptno").max("salary").sort("deptno").show()
    # df1.agg(min("salary")).show()

    #     ## by spark sql
    # df1.createOrReplaceTempView("emp")
    # # spark.sql("select deptno,avg(salary) from emp group by deptno").show()
    # spark.sql("select deptno,salary from emp where salary=(select max(salary) from emp where salary<(select max(salary) from emp))").show()




                ## for HR.Employee.csv file
    # df=spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\0_Input\HR_Employe.csv",inferSchema=True,header=True)
    #df.groupBy("DEPARTMENT_ID").avg("SALARY").sort("DEPARTMENT_ID",ascending=True).show()
    # df.groupBy("DEPARTMENT_ID").max("SALARY").sort("DEPARTMENT_ID",ascending=True).show()
    # df.agg(max('SALARY')).show()

         ##----------------OR-----------

    # df2 = spark.read.text(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.txt")
    # df2.printSchema()
    # df2.show()

    ## (2) employee details who have second highest salary
    # df=spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv",
    #                   inferSchema=True,header=True)
    # df.printSchema()
    # df.show()
    # df1=df.orderBy(col("salary").desc()).show(2,truncate=True)
    # df1.orderBy(col("salary").asc()).show(1,truncate=True)
    # df2.show()

                ##---------OR-----------------

    # df=spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv",
    #                   inferSchema=True,header=True)
    # from pyspark.sql.window import Window
    # from pyspark.sql.functions import row_number, rank, dense_rank

    # df.printSchema()
    # df.show()

    # windowSpec = Window.orderBy(col("salary").desc())

    # df.withColumn("row_number", row_number().over(windowSpec)).where(col("row_number") == 2).show(truncate=False)
    # df.withColumn("rank", rank().over(windowSpec)).where(col("rank") == 2).show(truncate=False)
    # df.withColumn("dense_rank", dense_rank().over(windowSpec)).where(col("dense_rank") == 2).show(truncate=False)

            ##-------------OR
    ## By spark sql

    # df.createOrReplaceTempView("emp")
    # spark.sql(
    #     "select deptno,salary from emp where salary=(select max(salary) from emp where salary<(select max(salary) from emp))").show()

    ## (3) find unique records from two DataFrame
    # df1=spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv",inferSchema=True,header=True)
    # df1.show()
    # df2=spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employees_details1.csv",inferSchema=True,header=True)
    # df2.show()
    #
    # df1.union(df2).distinct().sort("id").show()






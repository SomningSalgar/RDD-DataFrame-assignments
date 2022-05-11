from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark=SparkSession.builder.appName("Assignment_05").master("local[*]").getOrCreate()
    # print(spark)

    # data=[(1,"xyz",200,datetime.strptime("2021-01-01", "%Y-%m-%d")),
    #       (2,"pqr",201,datetime.strptime("2021-01-01", "%Y-%m-%d")),
    #       (2,"pqr",202,datetime.strptime("2021-01-02", "%Y-%m-%d")),
    #       (2,"pqr",203,datetime.strptime("2021-01-03", "%Y-%m-%d")),
    #       (2,"pqr",204,datetime.strptime("2021-01-04", "%Y-%m-%d")),
    #       (3,"nmp",205,datetime.strptime("2021-01-01", "%Y-%m-%d")),
    #       (3,"nmp",206,datetime.strptime("2021-01-03", "%Y-%m-%d")),
    #       (3,"nmp",207,datetime.strptime("2021-01-06", "%Y-%m-%d"))]
    # column=["id","name","price","date"]
    # df=spark.createDataFrame(data=data,schema=column)
    # df.printSchema()
    # df.show()

    ## (1) Retrive record which is having maximum date with respective of id, name column

    # df.withColumn("date", col("date")).groupBy("id", "name").agg(max("date")).show()
             #------OR
    # df.select("id","name","date").groupBy("id", "name").agg(max("date")).show()
            #-------OR
    # df.groupBy("id", "name").agg(max("date")).show()



    ## (2) Expected output as below

    # df1=df.withColumn("to_date",lead("date",1).over(Window.partitionBy("id","name").orderBy("date")))\
    # .select("*")

    # df1.show()     ## It returns null string


    ## Returns null if the input is a string that can not be cast to Date or Timestamp.

    # df2=df1.withColumn("to_date",col("to_date").cast("string"))

    ##Creating Temp view for sql
    # df.show()
    # df.createOrReplaceTempView("xyz")
    # spark.sql("select id,name,price,date,nvl(to_date,'2999-12-31') as to_date from xyz").show()


    ## For emp_details csv file questions--try below question with RDD, Spark Dataframe, Spark SQL

    ##(1)  get employee details, who has salary more than avg salary of all the employees

    schema="id int,fname string,lname string,age int,gender string,deptno int,salary long,joining_date date"
    df=spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\empdetails_date.csv",schema=schema)
    # df.printSchema()
    # df.show()

    # In DataFrame
    # avg_sal=df.agg(avg("salary")).take(1)[0][0]
    # df.select("*").where(col("salary")>avg_sal).show()

    ## In Spark SQL
    # df.createOrReplaceTempView("Employees")
    # spark.sql("select * from Employees where salary>(select avg(salary) from Employees )").show()


    ## (2) get count of an employee based on joining month

     ## In DataFrame
    # df.groupBy(month("joining_date").alias("Joining_month")).count().show()

    ## In Spark SQL
    # df.createOrReplaceTempView("Employees")
    # spark.sql("select extract(month from joining_date) Joining_month,count(id) count_emp from Employees group by Joining_month").show()


    ## (3) get a count of an employee based on joining year

    ## In DataFrame
    # df.groupBy(year("joining_date")).count().show()
    ## In spark sql
    # df.createOrReplaceTempView("Employees")
    # spark.sql("select count(id),extract(year from joining_date) year from Employees group by year").show()


    ## (4) get employee details with depart name who has joined in a leap year

    ## In DataFrame
    # df.printSchema()
    # df.select("*").where(year("joining_date")%4==0).show()

    # ## In spark sql
    # df.createOrReplaceTempView("Employees")
    # spark.sql("select * from Employees where extract(year from joining_date)%4=0").show()

    # df.show()
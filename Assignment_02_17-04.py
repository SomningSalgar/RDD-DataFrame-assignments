from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':

    conf=SparkConf().setAppName("Assignment_02").setMaster("local[*]")
    sc=SparkContext(conf=conf)

    spark=SparkSession.builder.appName("Assignment_02").master("local[*]")\
        .config("spark.driver.bindaddres","localhost")\
        .config("spark.ui.port","4050").getOrCreate()

    # print(sc)
    # print(spark)


    ## replace department name in employee table
    # emp_rdd=spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\0_Input\employee_details.txt")
    # emp_rdd1=emp_rdd.map(lambda x: x.split(",")).map(lambda x: (x[5],x))
    # dept_rdd=spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\0_Input\dept.txt")

    ##skip header
    # header=dept_rdd.first()
    # dept_rdd1=dept_rdd.filter(lambda x:x!=header).map(lambda x:x.split(",")).map(lambda x:(x[0],x[1]))
    #
    # join_rdd=emp_rdd1.join(dept_rdd1).cache()
    # for row in join_rdd.collect():
    #     print(row)

    # def replacefun(element):
    #     src=element[0]
    #     src[5]=element[1]
    #     return src

    # join_rdd.map(lambda x:x[1]).map(replacefun)\
    #     .saveAsTextFile(r"C:\\Users\\Vishalya\\PycharmProjects\\PySpark_Session\\Output\\replacefun")


    ## 1. average salary per department
    # avg_rdd=spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.txt")

    # def avg_cal(*args):
    #     avg=sum(args)/len(args)
    #     return(avg)
    #
    # print(avg_rdd.map(lambda x:x.split(",")).map(lambda x:(x[5],int(x[6]))).reduceByKey(avg_cal).collect())

         ## OR

    # avg_rdd1=(avg_rdd.map(lambda x: x.split(","))\
    #           .map(lambda x: (x[5], int(x[6])))\
    #           .groupByKey().mapValues(list))\
    #           .map(lambda x:(x[0],sum(x[1])/len(x[1])))
    # print(avg_rdd1.collect())


    ## 2. provide employee details who has second highest salary

    # rdd=spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.txt")
    # sec_sal_rdd=rdd.sortBy(lambda x: x[6], ascending=False).take(2)
    # print(spark.sparkContext.parallelize(sec_sal_rdd).sortBy(lambda x:x[6],ascending=True).first())

    ##---------OR-----
    # header=spark.sparkContext.parallelize(sec_sal_rdd).first()
    # print(spark.sparkContext.parallelize(sec_sal_rdd).filter(lambda x: x!=header).collect())

    ##---------OR-----
    # print(spark.sparkContext.parallelize(sec_sal_rdd).zipWithIndex().filter(lambda x:x[1]==1).collect())

    ##------OR-----
    # secondhighestsal=min(rdd.map(lambda x: x.split(","))\
    #                     .map(lambda x:int(x[6])).distinct()\
    #                     .takeOrdered(2,key=lambda x: -x))
    # print(rdd.map(lambda x:x.split(",")).filter(lambda x:int(x[6])==secondhighestsal).collect())


    ## 3. Retrieve employee_details with unique records from employee_detail.txt
            # and employee_details1.txt and store them in different file.

    # emp_rdd=spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.txt")
    # empd_rdd=spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details1.txt")

    # union_rdd=emp_rdd.union(empd_rdd)
    # for i in union_rdd.collect():
    #     print(i)
    # f_rdd=union_rdd.distinct()
    # for i in f_rdd.collect():
    #     print(i)
    # f_rdd.saveAsTextFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Output\union_rdd")





    ## 4. Create a dataframe from existing rdd/existing collection using various ways
    # from pyspark.sql import Row
    # e=[Row("max","doctor","usa"),
    #    Row("Nike","Enterprenuer","UK")]
    # df=spark.createDataFrame(e)
    # df.show()
    # df.printSchema()


    ## 5. Create a dataframe from csv file using various ways

    ## (i) PySpark Read CSV File into DataFrame

    # df = spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv")
    # df.printSchema()

            #------OR_____

    # df = spark.read.format("csv").load(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv")
    # df.printSchema()

    ## (ii) Using Header Record For Column Names

    # df = spark.read.option("header", True) \
    #     .csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv")
    # df.printSchema()

    ## (iii) inferSchema
    # df=spark.read.option("inferSchema",'True').csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv")
    # df.printSchema()

    ## (iv) header
    # df = spark.read.options(inferSchema='True',header="True").csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv")
    # df.printSchema()

        #----OR
    # df=spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv",inferSchema=True,header=True)
    # df.printSchema()

    ## (v) with user defined schema

    from pyspark.sql.types import *
    # dataschema=StructType([StructField(name="id",dataType=IntegerType()),
    #                        StructField('fname',StringType()),
    #                        StructField("lname",StringType()),
    #                        StructField("age",IntegerType()),
    #                        StructField("gender",StringType()),
    #                        StructField("deptno",IntegerType()),
    #                        StructField("salary",IntegerType())])
    # df=spark.read.csv(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\Input\employee_details.csv",schema=dataschema,header=True)
    # df.printSchema()
    # df.show()






    # input("enter any number: ")
    # spark.stop()
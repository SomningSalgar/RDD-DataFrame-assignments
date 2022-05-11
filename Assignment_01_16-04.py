from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("Assignment_01").master("local[*]").getOrCreate()


    ## (1)Read txt,CSV disk file:-

    # txt_rdd = spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\0_Input\input_file.txt")
    # print(txt_rdd.collect())

    # csv_rdd=spark.sparkContext.textFile(r"C:\Users\Vishalya\PycharmProjects\PySpark_Session\0_Input\HR_Employe.csv")
    # print(csv_rdd.collect())
    # for line in csv_rdd.collect():
    #     print(line)

    ## (2) count words in the file

    # wordcount_txt=txt_rdd.flatMap(lambda x: x.lower().split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)
    # for word in wordcount_txt.collect():
    #     print(word)

    # wordcount_csv=csv_rdd.flatMap(lambda x: x.lower().split(","))\
    #                .map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
    # for word in wordcount_csv.collect():
    #     print(word)

    ## (3) create rdd using parallelize collection
    lst=[{"a":1},{"b":2},{"c":1},{"d":4}]
    lst_rdd=spark.sparkContext.parallelize(lst)
    # for i in lst_rdd.collect():
    #     print(i)

    ## (4) retrive element from above rdd, having value as 1 and 2
    # def func(dict1):
    #     return{k:v for k,v in dict1.items() if v==1 or v==2}
    #
    # print(lst_rdd.filter(func).collect())

    ## (5) create an a new rdd based on above one by multiplying 25 in value section
    # def func(dict1):
    #     return{k:v*25 for k,v in dict1.items()}
    #
    # print(lst_rdd.map(func).collect())


    ## (6) create a new rdd from point 3 and retrive even number values only
    # def func(dict):
    #     return{k:v for k,v in dict.items() if v%2==0}
    # print(lst_rdd.filter(func).collect())

    # input("Enter any number: ")
    # spark.stop()
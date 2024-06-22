# coding:utf8

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # 读取CSV文件
    df = spark.read.format("csv").\
        option("sep", ";").\
        option("header", True).\
        option("encoding", "utf-8").\
        schema("name STRING, age INT, job STRING").\
        load(r"C:\Users\xu\Pictures\hadoop\spark_learning\测试数据\sql\people.csv")

    df.printSchema()
    df.show()

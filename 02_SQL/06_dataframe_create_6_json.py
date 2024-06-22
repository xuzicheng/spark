# coding:utf8

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # JSON类型自带有Schema信息
    df = spark.read.format("json").load(r"C:\Users\xu\Pictures\hadoop\spark_learning\测试数据\sql\people.json")
    df.printSchema()
    df.show()

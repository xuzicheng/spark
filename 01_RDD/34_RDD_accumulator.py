# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 创建SparkConf和SparkContext
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 创建一个包含10个元素(1到10)的RDD，并分成2个分区
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    # Spark提供的累加器变量, 参数是初始值
    acmlt = sc.accumulator(0)


    # 定义一个映射函数，每处理一个元素时累加器加1
    def map_func(data):
        global acmlt
        acmlt += 1
        # print(acmlt)


    # 对RDD应用map函数，但不改变其元素
    rdd2 = rdd.map(map_func)

    # 将rdd2缓存起来
    rdd2.cache()

    # 触发action操作，收集结果
    rdd2.collect()

    # 对RDD应用另一个map函数，不做任何改变（只是一个占位操作，实际生产环境中可能会有其他操作）
    rdd3 = rdd2.map(lambda x: x)
    rdd3.collect()

    # 打印累加器的值，输出的是map_func调用的次数
    print(acmlt)

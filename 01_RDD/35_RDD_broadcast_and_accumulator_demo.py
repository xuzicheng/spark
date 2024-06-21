import re

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 1. 配置Spark上下文对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 2. 读取数据文件
    file_rdd = sc.textFile(r"C:\Users\xu\Pictures\hadoop\spark_learning\accumulator_broadcast_data.txt")

    # 3. 特殊字符的list定义
    abnormal_char = [",", ".", "!", "#", "$", "%"]

    # 4. 将特殊字符list包装成广播变量
    broadcast = sc.broadcast(abnormal_char)

    # 5. 对特殊字符出现次数做累加, 累加使用累加器最好
    acmlt = sc.accumulator(0)

    # 6. 数据处理, 先处理数据的空行, 在Python中有内容就是True, 没有内容就是False
    lines_rdd = file_rdd.filter(lambda line: line.strip())

    # 7. 去除前后的空格
    data_rdd = lines_rdd.map(lambda line: line.strip())

    # 8. 对数据进行切分, 按照正则表达式切分, 因为空格分隔符某些单词之间是两个或多个空格
    # 正则表达式 \s+ 表示 不确定多少个空格, 最少一个空格
    words_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))

    # 9. 过滤数据, 保留正常单词用于做单词计数, 在过滤的过程中对特殊符号做计数
    def filter_func(data):
        """过滤数据, 保留正常单词用于做单词计数, 在过滤的过程中对特殊符号做计数"""
        global acmlt
        # 取出广播变量中存储的特殊符号list
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            # 表示这个是特殊字符
            acmlt += 1
            return False
        else:
            return True


    normal_words_rdd = words_rdd.filter(filter_func)

    # 10. 正常单词的单词计数逻辑
    result_rdd = normal_words_rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

    print("正常单词计数结果: ", result_rdd.collect())
    print("特殊字符数量: ", acmlt)

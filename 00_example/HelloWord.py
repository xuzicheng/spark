from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("WordCount").setMaster("local")
sc = SparkContext(conf=conf)
inputFile = r"C:\Users\xu\Pictures\hadoop\spark_learning\words.txt"
textFile = sc.textFile(inputFile)
wordCount = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
wordCount.foreach(print)

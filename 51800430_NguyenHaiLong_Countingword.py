from pyspark  import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('Word counting')
sc = SparkContext.getOrCreate(conf = conf)
my_RDD = (sc.textFile('/content/sample_data/Text.txt', minPartitions = 1, use_unicode = True).map(lambda each: each.split(' ')))
count = my_RDD.reduce(lambda x:1)
word = len(count)
print(word)

my_rdd = sc.parallelize(count)
t = my_rdd.map(lambda x:(x,1))
num_word = t.reduceByKey(lambda x,y: x+y)
print(num_word.collect())

k = int(input('Enter k: '))
resl = num_word.takeOrdered(k, key = lambda x: -x[1])
for i in resl:
  print(i[0])
max_i = num_word.reduce(lambda x,y: max(x,y))
print(max_i)
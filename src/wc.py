from pyspark.sql import SparkSession
import sys
if __name__='__main__':
    conf=sparkconf()
    sc=sparkContext('local','wcapp',conf=conf)
    r1=sc.textfile(sys.argv[1])
    r2=r1.flatmap(lambda x:x.split(' ')).map(lambda x:(x,1)).
                            reduceByKey(lambda x,y:x+y)
    r2.saveAsTextFile(sys.argv[2])
    sc.stop()

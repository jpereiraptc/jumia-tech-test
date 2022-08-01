from pyspark.sql import SparkSession
import sys

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[*]") \
      .appName("Most Connected User") \
      .getOrCreate() 

def main() -> int:
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    print("Input: {} Output: {}".format(inputFile,outputFile))
    inputRdd = spark.sparkContext.textFile(inputFile)
    mapRdd = inputRdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
    sortedOutput = mapRdd.reduceByKey(lambda a,b: a+b).sortBy(lambda tmp: tmp[1], False)
    #Adding coalesce to output into a single file
    sortedOutput.coalesce(1).saveAsTextFile(outputFile)
    return 0

#Execution: python most-connected-user.py higgs-social_network.edgelist output.txt
if __name__ == '__main__':
    sys.exit(main())
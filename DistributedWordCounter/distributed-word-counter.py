from pyspark.sql import SparkSession
import sys

# Create SparkSession with half of the available cores
spark = SparkSession.builder \
      .master("local[4]") \
      .appName("Distributed Word Counter") \
      .getOrCreate() 

def main() -> int:
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    print("Input: {} Output: {}".format(inputFile,outputFile))
    inputRdd = spark.sparkContext.textFile(inputFile)
    mapRdd = inputRdd.flatMap(lambda line: line.split(" ")).filter(lambda word: word != "").map(lambda word: (word.lower(), 1))
    sortedOutput = mapRdd.reduceByKey(lambda a,b: a+b).sortBy(lambda tmp: tmp[1], False).map(lambda entry: "{}:{}".format(entry[0],entry[1]))
    #Adding coalesce to output into a single file
    sortedOutput.coalesce(1).saveAsTextFile(outputFile)
    return 0

#Execution: python distributed-word-counter.py input.txt output.txt
if __name__ == '__main__':
    sys.exit(main())
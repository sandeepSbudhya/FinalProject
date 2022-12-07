from pyspark.sql import SparkSession
import glob

spark = SparkSession.builder.getOrCreate()
def cleanFile(fileName):
    df = spark.read.csv(fileName, sep=',', inferSchema=True, header=True).dropna()\
        .drop('Fwd Header Length40').withColumnRenamed('Fwd Header Length61','Fwd Header Length')
    df.coalesce(1).write.option("header", True).csv("cleaned"+fileName.split('/')[-1:][0])

if __name__ == "__main__":
    path = '/home/sandeep/Uni/ISP_Project/CIC2017/*.csv'
    for fname in glob.glob(path):
        cleanFile(fname)
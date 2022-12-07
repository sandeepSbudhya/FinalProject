from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('/home/sandeep/Uni/ISP_Project/CIC2017/2017Data.csv', sep=',', inferSchema=True, header=True)
df.groupBy("Label").count().show()

benign = df.filter(df.Label == 'BENIGN').limit(25000)
ddos = df.filter(df.Label == 'DDoS').limit(25000)
portscan = df.filter(df.Label == 'PortScan').limit(25000)
doshulk = df.filter(df.Label == 'DoS Hulk').limit(25000)
bruteforce = df.filter((df.Label == 'SSH-Patator') | (df.Label == 'FTP-Patator')).withColumn('Label', lit("BruteForce"))

combined = benign.union(ddos).union(portscan).union(doshulk).union(bruteforce)
combined.coalesce(1).write.option("header", True).csv("resampled")
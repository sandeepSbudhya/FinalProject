#!/usr/bin/env python3

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer
from matplotlib.pyplot import figure
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from matplotlib import pyplot as plt
import json

class TrainRandomForest:
    def __init__(self,):
        # self.spark = SparkSession.builder.config("spark.executor.memory", "5g").config("spark.driver.memory", "15g").getOrCreate()
        self.spark = SparkSession.builder.getOrCreate()

    def stringIndexerDf(self, training_file, ):
        df = self.spark.read.csv('hdfs://master-node:9000/user/hadoop/data/'+ training_file, sep=',', inferSchema=True, header=True)
        Indexer = StringIndexer(inputCol='Label', outputCol="Class Index").setHandleInvalid("skip")
        classIndexedDf = Indexer.fit(df).transform(df)
        classMap = [x for x in classIndexedDf.select("Label", "Class Index").distinct().collect()]
        dict = {}
        for x in classMap:
            dict[x[1]] = x[0]
        with open('classMap.json', 'w') as file:
            json.dump(dict, file)
        return classIndexedDf

    def train(self, trainingDf, ):
        numcols = [x for (x, dataType) in trainingDf.dtypes if ((dataType =="int") | (dataType == "double") & (x != "Class Index"))]
        vectorAssembledDf = VectorAssembler(inputCols = numcols, outputCol = "Features").transform(trainingDf)
        train, test = vectorAssembledDf.randomSplit([0.7, 0.3], seed = 2022)
        rf = RandomForestClassifier(featuresCol = 'Features', labelCol = 'Class Index').fit(train)
        predictions = rf.transform(test)
        evaluator = MulticlassClassificationEvaluator( labelCol="Class Index", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        evaluator = MulticlassClassificationEvaluator( labelCol="Class Index", predictionCol="prediction", metricName="f1")
        f1score = evaluator.evaluate(predictions)
        evaluator = MulticlassClassificationEvaluator( labelCol="Class Index", predictionCol="prediction", metricName="falsePositiveRateByLabel")
        fprbl = evaluator.evaluate(predictions)
        with open('metrics.txt', 'w') as f:
            f.write('Accuracy: '+str(accuracy)+'\n')
            f.write('F1 Score: '+str(f1score)+'\n')
            f.write('False Positive Rate by Label: '+str(fprbl)+'\n')
            f.close()
        rf.write().overwrite().save('hdfs://master-node:9000/user/hadoop/RfModel')

if __name__ == "__main__":
    """ This is executed when run from the command line """
    trainer = TrainRandomForest()
    df = trainer.stringIndexerDf('resampled2017Data.csv')
    trainer.train(df)

    
   
    

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer
import json
from pyspark.sql import functions as F
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading


class IncomingFileHandler(FileSystemEventHandler):
    def __init__(self):
        self.events=[]
        self.tester = RfModel()

    def on_created(self, event):
        self.events.append(event.src_path)
        threading.Thread(target=self.tester.predictions, args=(event.src_path,)).start()


class Watch:
    def __init__(self):
        event_handler = IncomingFileHandler()
        self.observer = Observer()
        self.observer.schedule(event_handler, path='/home/hadoop/incomingData', recursive=False)

    def start_observing(self):
        self.observer.start()
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

class RfModel:
    
    def __init__(self):
        self.spark = SparkSession.builder.config('spark.scheduler.mode', 'FAIR').config("spark.scheduler.allocation.file", "/home/hadoop/Train/fairscheduler.xml").getOrCreate()
        self.rf = RandomForestClassificationModel.load('hdfs://master-node:9000/user/hadoop/RfModel')
        with open('classMap.json') as f:
            data = f.read()
        self.classMap = json.loads(data)
        self.map_col = F.create_map([F.lit(x) for i in self.classMap.items() for x in i])

    def predictions(self, fileName):
        df = self.spark.read.csv(fileName, sep=',', inferSchema=True, header=True)
        numcols = [x for (x, dataType) in df.dtypes if ((dataType =="int") | (dataType == "double"))]
        vectorAssembledDf = VectorAssembler(inputCols = numcols, outputCol = "Features").transform(df)
        predictions = self.rf.transform(vectorAssembledDf).select('probability', 'prediction')
        predictions = predictions.withColumn('class', self.map_col[F.col('prediction')])
        predictions.show()
        
if __name__ == "__main__":
    """ This is executed when run from the command line """
    obs = Watch()
    obs.start_observing()

# Network Intrusion Detection using Machine Learning in Big Data Environment


## Instructions to setup

 1. Setup Apache Hadoop and Apache Spark on all the nodes.
 ***All machines must have the same version of the OS java, scala, Apache Hadoop, Apache Spark and python***
The master-node must have passwordless ssh access to the data-node(s).
 2. create user called 'hadoop' and clone the repo to the master node.
 `git clone https://github.com/sandeepSbudhya/FinalProject.git`
 3. The folder structure in the master node should like as follows:
	 /home/hadoop/Train - contains train.py, test.py and fairscheduler.xml
	 /home/hadoop/incomingData - This is where to send the samples for testing live.
 4. create the virtual environment.
`pipenv shell`
 5. The 2017 training data must be resampled and combined. The code to do so is provided in the misc folder.
 6. The hadoop file structure should look as follows:
 
	 /user/hadoop/data - contains the training file '2017data.csv'
	 Once the model is trained it will be stored in HDFS.
	 /user/hadoop/rfmodel
	 The following command will copy the file to HDFS
`hadoop fs -copyFromLocal /path/to/file/in/local/device /path/in/hadoop`
 7. run the train script with the environment activated.
	 `python3 train.py`
 8. The evaluation metrics should be available in the metrics.txt file once the training is complete.
 9. run the test script with the environment activated.
	 `python3 test.py`
 10. Send test data to the master nodes directory as follows:
	 scp /path/to/sample.csv user@master-node-ip:/home/hadoop/incomingData




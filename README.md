# Intrusion-Detection-System-Using-Spark

Keywords: Spark, Resilient Distributed Dataset (RDD), Parallel Computing

We use PySpark to implement the algorithms for detecting intrusive behaviors inside a network log. 
The data set is a 4 GB compressed TCP data dump provided by the International Knowledge Discovery and Data Mining Tools Competition (KDD cup 99).
Data dictionary and important notes on the data set can be found on http://kdd.ics.uci.edu/databases/kddcup99/task.html

The classification of the KDD cup 99 mainly targets four types of attacks:
1) DOS: denial-of-service, e.g. syn flood;
2) R2L: unauthorized access from a remote machine, e.g. guessing password;
3) U2R: unauthorized access to local superuser (root) privileges, e.g., various "buffer overflow" attacks;
4) probing: surveillance and other probing, e.g., port scanning.

In this project we use pyspark to explore some well-known rules to extract some useful information from the KDD 99 data set. By implementing the Spark interfaces in python (PySpark) to perform big-data analysis on statistical data using Jupyter notebook, we were able to evaluate 
the difficulty of brute force login attack & distribution of DDOS attack by correlating SYN flooding in 0.7s

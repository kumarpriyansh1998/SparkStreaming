# SparkStreaming

This code reads in streaming text data from a directory and filters out IP addresses that have appeared more than 30 times in the last 5 seconds of data. The filtered results are printed to the console in real-time.

To accomplish this, the code first initializes a Spark context and a Spark streaming context with a batch interval of 5 seconds. It also sets a checkpoint directory, which is required for stateful operations like reduceByKey().

The mapIPValue() function is defined to map each line of text data to an IP address, count the occurrences of each IP address, and filter out IP addresses that have occurred more than 30 times. This function is applied to the streaming data using the transform() method.

The resulting DStream, transformed_access_log, is printed to the console using the pprint() method, which is a method of DStream that displays the first n elements of each RDD in the DStream.

Overall, this code demonstrates how to use PySpark to process streaming data and perform real-time filtering operations.

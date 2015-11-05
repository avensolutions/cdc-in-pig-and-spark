**cdc-in-pig-and-spark**
==============
Source change detection (CDC) in Hadoop implemented using:  
- [Apache Spark](http://spark.apache.org/) using [PySpark](http://spark.apache.org/docs/latest/api/python/index.html); or  
- [Apache Pig](https://pig.apache.org/) 

generate-cdc-script.py is a Python module which reads dataset config from a YAML document (see config.yaml in this project)
, and then creates a Spark program in either PigLatin or PySpark which compares incoming text based delimited datasets with current versions of the 
same object stored as Parquet file(s).  Changes are detected by comparing an MD5 signature of the natural key and non key columns 
respectively with the signatures from existing records.  The resultant detected changes are written to a new Parquet formatted object 
with additional meta columns:

    rectype, effective_start_date, effective_end_date, key_hash, value_hash
    e.g.
    .., 'INSERT', 1446162419, 0, 'cd507348bd94c167b', '826a43b1cf6f975d0' 
    .., 'UPDATE', 1446162419, 0, 'cd507348bd94c167b', '826a43b1cf6f975d0' 
    .., 'DELETE', 1446162419, 1446162500, 'cd507348bd94c167b', '826a43b1cf6f975d0' 	
    
Current records are identified by `effective_end_date == 0`

A generalisation of the approach is:  

*Compare INCOMING to CURRENT using DATASETCONFIG at EFFECTIVETIME save as OUTPUT*  

This approach does not depend on Hive schemas or even SQL for that matter, meaning it can easily handle non SQL patterns.  The other advantage to this approach
 is that it can not only react to changes in the data but can handle changes in the structure of incoming data with respect to the pre-existing object
, eg the addition of a new column would be handled seamlessly as an update to (all) records in the dataset.

**So this is effectively CDC for NoSQL sources as well as conventional SQL sources**
	
Dependencies
--------------
- Python 2.7.x
- jinja2
- Apache Spark 1.3.x or above; or
- Apache Pig 0.12 or above; including
	-  [Apache DataFu](https://datafu.incubator.apache.org/)
	-  [Apache Parquet library for Pig](https://github.com/Parquet/parquet-mr/tree/master/parquet-pig)

generate-cdc-script.py Usage
--------------
    runtime_method (str): run time execution method for the CDC process, accepted values are: 'pyspark', 'pig'
	incoming_dataset_path (str): fully qualified path to INCOMING dataset file(s) or directory, globs supported
    current_dataset_path (str): fully qualified path to CURRENT dataset file(s) or directory, globs supported		
    config_file (str): full path and file name for config YAML document, describing the dataset.
    timestamp (int):  long timestamp used for versioning of templates, data files and effective timestamp for changed records
    output_format (str): format for the output, accepted values are: 'screen', 'file'
    output_path (Optional[str]): mandatory if output_format is set to 'file', fully qualified path for output file(s)

	$ # PySpark Examples
    $ python generate-cdc-script.py pyspark hdfs://mydataset/incoming/ hdfs://mydataset/current/ config.yaml 1446162419 screen
    $ python generate-cdc-script.py pyspark hdfs://mydataset/incoming/ hdfs://mydataset/current/ config.yaml 1446162419 file hdfs://mydataset/latest
	$ # Pig Examples
    $ python generate-cdc-script.py pig incoming current config.yaml 1446162419 screen
    $ python generate-cdc-script.py pig incoming current config.yaml 1446162419 file latest
		
Test Example
--------------	
The included test example illustrates the CDC generation and execution process (using PySpark in this example), follow the steps below:  

1. create the following directories (in your user directory) in HDFS

    `hadoop fs -mkdir incoming`  
    `hadoop fs -mkdir current`  
2. run the following command from the current project directory:

	`hadoop fs -put testdata/testdata.day1 incoming/`


3. run the following commands to compare the incoming data to an empty set of existing records:

	`EFFTS=$(date +%s)`  	
	`PYSPARK_CDC_SCRIPT=$(python generate-cdc-script.py pyspark hdfs:///user/root/incoming/ hdfs:///user/root/current/current.parquet config.yaml $EFFTS file hdfs:///user/root/current/current.parquet)`  
	`export SPARK_HOME=/usr/hdp/2.3.0.0-2557/spark`  
	`export YARN_CONF_DIR=/etc/hadoop/conf`  
	`${SPARK_HOME}/bin/spark-submit \`  
	`--master yarn-client \`  
	`--num-executors 1 \`  
	`--driver-memory 512m \`  
	`--executor-memory 512m \`  
	`$PYSPARK_CDC_SCRIPT`
 
	since there were no existing records, all incoming records should be classified as INSERTs (note you wont see this output as it is written to the current.parquet directory  

	`_1     _2      _3      _4      _5 _6     _7         _8 _9                   _10`  
	`'key3' 'r3_v1' 'r3_v2' 'r2_v3' 3  INSERT 1446244715 0  63389d83771bfb2d6... 4171995af83ae2cfc...`  
	`'key2' 'r2_v1' 'r2_v2' 'r2_v3' 2  INSERT 1446244715 0  e1ec108e9675fa1a2... d35f684dd118b1a67...`  
	`'key1' 'r1_v1' 'r1_v2' 'r1_v3' 1  INSERT 1446244715 0  cd507348bd94c167b... 826a43b1cf6f975d0...`

4. now run the following command to refresh the incoming directory with the next set of incoming data:

	`hadoop fs -rm incoming/testdata.day1`  
	`hadoop fs -put testdata/testdata.day2 incoming/` 

  
5. run the CDC process again, sending the output to the console:

	`EFFTS=$(date +%s)`  	
	`PYSPARK_CDC_SCRIPT=$(python generate-cdc-script.py pyspark hdfs:///user/root/incoming/ hdfs:///user/root/current/current.parquet config.yaml $EFFTS screen hdfs:///user/root/current/current.parquet)`  
	`export SPARK_HOME=/usr/hdp/2.3.0.0-2557/spark`  
	`export YARN_CONF_DIR=/etc/hadoop/conf`  
	`${SPARK_HOME}/bin/spark-submit \`  
	`--master yarn-client \`  
	`--num-executors 1 \`  
	`--driver-memory 512m \`  
	`--executor-memory 512m \`  
	`$PYSPARK_CDC_SCRIPT` 
 
	you should see the following output reflecting the changes in the incoming data set:

	`_1     _2              _3      _4      _5 _6     _7         _8         _9                   _10`  
	`'key4' 'r4_v1'         'r4_v2' 'r4_v3' 4  INSERT 1446245044 0          3105a2a16fb856c60... fc28f96fb3c5f0ce4...`  
	`'key1' 'r1_v1'         'r1_v2' 'r1_v3' 1  DELETE 1446244715 1446245044 cd507348bd94c167b... 826a43b1cf6f975d0...`  
	`'key2' 'r2_v1_UPDATED' 'r2_v2' 'r2_v3' 2  UPDATE 1446245044 0          e1ec108e9675fa1a2... 1d745316b2c1f367a...`  
	`'key2' 'r2_v1'         'r2_v2' 'r2_v3' 2  UPDATE 1446244715 1446245044 e1ec108e9675fa1a2... d35f684dd118b1a67...`  
	`'key3' 'r3_v1'         'r3_v2' 'r2_v3' 3  INSERT 1446244715 0          63389d83771bfb2d6... 4171995af83ae2cfc...`  

	*key4 was a new record, key1 was deleted between day1 and day2, key2 was updated, and there were no changes to key3*
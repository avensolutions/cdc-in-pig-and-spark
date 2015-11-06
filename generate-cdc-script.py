"""
generate-cdc-pyspark.py

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

Compare INCOMING to CURRENT using DATASETCONFIG at EFFECTIVETIME save as OUTPUT*  

This approach does not depend on Hive schemas or even SQL for that matter, meaning it can easily handle non SQL patterns.  The other advantage to this approach
 is that it can not only react to changes in the data but can handle changes in the structure of incoming data with respect to the pre-existing object
, eg the addition of a new column would be handled seamlessly as an update to (all) records in the dataset.

Args:
		runtime_method (str): run time execution method for the CDC process, accepted values are: 'pyspark', 'pig'
		incoming_dataset_path (str): fully qualified path to INCOMING dataset file(s) or directory, globs supported
		current_dataset_path (str): fully qualified path to CURRENT dataset file(s) or directory, globs supported		
		config_file (str): full path and file name for config YAML document, describing the dataset.
        timestamp (int):  long timestamp used for versioning of templates, data files and effective timestamp for changed records
		output_format (str): format for the output, accepted values are: 'screen', 'file'
		output_path (Optional[str]): mandatory if output_format is set to 'file', fully qualified path for output file(s)

Example:

		$ # PySpark Examples
		$ python generate-cdc-script.py pyspark hdfs://mydataset/incoming/ hdfs://mydataset/current/ config.yaml 1446162419 screen
		$ python generate-cdc-script.py pyspark hdfs://mydataset/incoming/ hdfs://mydataset/current/ config.yaml 1446162419 file hdfs://mydataset/latest

		$ # Pig Examples
		$ python generate-cdc-script.py pig incoming current config.yaml 1446162419 screen
		$ python generate-cdc-script.py pig incoming current config.yaml 1446162419 file latest
		
.. _cdc-in-pig-and-spark Project in GitHub:
   https://github.com/avensolutions/pyspark-cdc		
		
"""

import jinja2, yaml, sys
# validate args
if len(sys.argv) < 6:
	print __doc__
	print "ERROR: Insuffient number of arguments"
	sys.exit(1)
runtime_method = str(sys.argv[1])	
if (runtime_method != 'pyspark') and (runtime_method != 'pig'):
	print __doc__
	print "ERROR: Illegal argument for runtime_method"
	sys.exit(1)
incoming_path = str(sys.argv[2])
current_path = str(sys.argv[3])
config_yaml = str(sys.argv[4])
ts = int(sys.argv[5])
output_format = str(sys.argv[6])
if output_format == 'file':
	if len(sys.argv) == 8:
		output_path = str(sys.argv[7])
	else:
		print __doc__
		print "ERROR: Insuffient number of arguments (output_path not supplied)"
		sys.exit(1)
elif output_format == 'screen':
		output_path = ''
else:
	print __doc__
	print "ERROR: Illegal argument for output_format"
	sys.exit(1)
#
# Main program
#

# read config.yaml
with open(config_yaml, 'r') as f:
	doc = yaml.load(f)
datasource_name = doc["datasource_name"]
delimited_by = doc["delimited_by"]
cols = doc["cols"]
metacols = doc["metacols"]
no_src_cols = len(cols)
if runtime_method == 'pyspark':
	templated_filename = datasource_name + "_cdc." + str(ts) + ".py"
	TEMPLATE_FILE = "spark-cdc.py.template"
else:
	templated_filename = datasource_name + "_cdc." + str(ts) + ".pig"
	TEMPLATE_FILE = "cdc.pig.template"
	
templateLoader = jinja2.FileSystemLoader( searchpath="templates" )
templateEnv = jinja2.Environment( loader=templateLoader )
template = templateEnv.get_template( TEMPLATE_FILE )

templateVars = { "incoming_path" : incoming_path,
				"current_path" : current_path,
				"delimited_by" : delimited_by,
				"ts" : ts,
				"no_src_cols" : no_src_cols,	
                "cols" : cols,
				"metacols" : metacols,
				"output_format"	: output_format,
				"output_path" : output_path}

# render template
outputText = template.render( templateVars )

with open(templated_filename, 'w') as f:
	f.write(outputText)
	
sys.stdout.write(templated_filename)
sys.stdout.flush()
sys.exit(0)	
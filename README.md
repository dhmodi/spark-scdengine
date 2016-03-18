# spark-scdengine

This application is plug-n-play application developed on Spark 1.6.0 using DataFrame feature.

It is easily configurable and converts few datatypes automatically.


#Usage:
1. Download jar from target directory
2. Execute below command

$SPARK_HOME/bin/spark-submit --class com.scd.engine.SparkSCDEngine --master <master> --deploy-mode <mode> --properties-file <path to properties file> <path to jar> <local/master> <path to CSV file>

# Description:
1. Properties file contains the application level configurable properties.
2. CSV file contains the table level SCD properties.

e.g. 
Source_DB   Source_Table  Target_DB Target_Table Primary_Key SCD_Type Incoming_Data Format

default src_Table default tgt_Table col1  Type2 Incremental col3="yyyy-MM-dd' 'HH:mm:ss"|col5="dd-MMM-yy"



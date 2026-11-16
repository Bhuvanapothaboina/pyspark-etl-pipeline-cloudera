ETL Data Pipeline Using PySpark and Hadoop (Cloudera)

1. Project Overview

This project demonstrates the implementation of an ETL (Extract, Transform, Load) pipeline using PySpark in a Hadoop environment (Cloudera).

The pipeline reads input data, performs data transformations using PySpark, and stores the processed data in Parquet format in HDFS.

2. Technologies Used

- Python
- PySpark
- Apache Spark
- Hadoop Distributed File System (HDFS)
- Cloudera
- Linux
- PuTTY
- WinSCP
- Parquet

3. Project Workflow

The following steps were performed during the practical:

1. Prepared the input dataset and PySpark logic file.
2. Created the configuration file and shell script.
3. Transferred the required project files from the local Windows system to the Cloudera virtual machine using WinSCP.
4. Verified the input and output paths in the configuration file.
5. Used PuTTY to access the Cloudera Linux environment.
6. Transferred the input dataset into HDFS using HDFS commands.
7. Executed the Spark job using the shell script.
8. Verified the processed output files in HDFS.

4. Files in the Project

"etl_file.py"            ---> Contains the PySpark ETL logic

"config.conf"               ---> Contains input path, output path and log file configuration

"Spark_Submit_shell_conf_script.sh"    ----> Shell script used to execute the Spark job

5. Linux and HDFS Operations

During this practical, I used PuTTY to execute Linux and HDFS commands.

The operations included:

- Navigating Linux directories and managing files.
- Preparing configuration and shell script files.
- Creating directories in HDFS.
- Uploading input text files to HDFS.
- Verifying files and directories in HDFS.
- Executing the Spark job through the shell script.
- Checking the final output in HDFS.

6. Challenges Faced and Solutions

Challenge 1: Incorrect Input File Name

Problem: The input file name specified in the configuration file did not match the actual dataset file name.

Solution: Corrected the input file name in the configuration file to match the actual dataset file name and executed the job again.

Challenge 2: HDFS NameNode Safe Mode

Problem: The Spark job failed because HDFS was in Safe Mode, which prevented writing output data.

Solution: Exited Safe Mode using the following command:

hdfs dfsadmin -safemode leave

After exiting Safe Mode, I executed the Spark job again successfully.

7. Final Output

- Successfully executed the PySpark ETL pipeline.
- Processed the input dataset using PySpark.
- Stored the transformed data in HDFS in Parquet format.
- Verified the output directory containing 200 Parquet files.

8. Key Learnings

Through this practical, I gained hands-on experience in:

- Understanding ETL pipeline execution.
- Working with PySpark and Apache Spark.
- Performing file operations in Linux.
- Managing files in HDFS.
- Using configuration files and shell scripts.
- Troubleshooting HDFS Safe Mode issues.
- Storing processed data in Parquet format.



Project Type: Hands-on Data Engineering Practical

Focus Areas: ETL, PySpark, Hadoop, HDFS and Linux

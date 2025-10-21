#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution Analysis
Analyzes distribution of log levels (INFO, WARN, ERROR, DEBUG) across all Spark logs.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count
import sys
import os

def create_spark_session(master_url):
    """Create Spark session with appropriate configuration."""
    builder = SparkSession.builder.appName("Problem1-LogLevelDistribution")
    
    if master_url:
        builder = builder.master(master_url).config("spark.executor.memory", "4g")
    
    return builder.getOrCreate()

def main():
    parser = argparse.ArgumentParser(description='Analyze log level distribution')
    parser.add_argument('master_url', nargs='?', default=None, 
                       help='Spark master URL (e.g., spark://ip:7077)')
    parser.add_argument('--net-id', required=True, 
                       help='Your net ID for S3 bucket access')
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session(args.master_url)
    sc = spark.sparkContext
    
    try:
        # Determine data source
        if args.master_url:
            # Running on cluster - use S3, all log files
            data_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*.log"
            print(f"Reading from S3: {data_path}")
        else:
            # Running locally - use sample data
            # Try multiple patterns to handle different directory structures
            data_path = "data/sample/application_*/*.log"
            
            # Check if sample directory exists and has the right structure
            if not os.path.exists("data/sample"):
                print("ERROR: data/sample/ directory not found!")
                print("Please create sample data first:")
                print("  mkdir -p data/sample")
                print("  cp -r data/raw/application_1485248649253_0052 data/sample/")
                sys.exit(1)
            
            # Check if we have any log files
            import glob
            log_files = glob.glob(data_path)
            if not log_files:
                # Try alternate pattern (maybe logs are directly in sample/)
                data_path = "data/sample/*.log"
                log_files = glob.glob(data_path)
                
                if not log_files:
                    print("ERROR: No log files found in data/sample/")
                    print("Current directory:", os.getcwd())
                    print("Looking for:", data_path)
                    print("\nPlease create sample data:")
                    print("  cp -r data/raw/application_1485248649253_0052 data/sample/")
                    sys.exit(1)
            
            print(f"Reading from local: {data_path}")
            print(f"Found {len(log_files)} log files")
        
        # Read all log files
        logs_rdd = sc.textFile(data_path)
        logs_df = spark.createDataFrame(logs_rdd.map(lambda x: (x,)), ["value"])
        
        total_lines = logs_df.count()
        print(f"Total lines read: {total_lines:,}")
        
        # Extract log levels using regex
        # Pattern based on dataset format: YY/MM/DD HH:MM:SS LEVEL Component: message
        logs_parsed = logs_df.withColumn(
            'log_level',
            regexp_extract('value', r'\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\s+(INFO|WARN|ERROR|DEBUG)\s+', 1)
        )
        
        # Filter to only rows with valid log levels
        logs_with_levels = logs_parsed.filter(col('log_level') != '')
        logs_with_levels.cache()
        
        total_with_levels = logs_with_levels.count()
        print(f"Lines with log levels: {total_with_levels:,}")
        
        # Count by log level
        level_counts = (logs_with_levels
                       .groupBy('log_level')
                       .agg(count('*').alias('count'))
                       .orderBy(col('count').desc()))
        
        # Collect results
        counts_list = level_counts.collect()
        
        # Ensure output directory exists
        os.makedirs('data/output', exist_ok=True)
        
        # Output 1: Counts CSV
        print("Writing problem1_counts.csv...")
        level_counts.toPandas().to_csv('data/output/problem1_counts.csv', index=False)
        
        # Output 2: Sample entries (10 random samples)
        print("Writing problem1_sample.csv...")
        sample_df = (logs_with_levels
                    .select(col('value').alias('log_entry'), 'log_level')
                    .sample(False, 0.001)  # Sample for diversity
                    .limit(10))
        sample_df.toPandas().to_csv('data/output/problem1_sample.csv', index=False)
        
        # Output 3: Summary statistics
        print("Writing problem1_summary.txt...")
        with open('data/output/problem1_summary.txt', 'w') as f:
            f.write(f"Total log lines processed: {total_lines:,}\n")
            f.write(f"Total lines with log levels: {total_with_levels:,}\n")
            f.write(f"Unique log levels found: {len(counts_list)}\n\n")
            f.write("Log level distribution:\n")
            
            for row in counts_list:
                level = row['log_level']
                count_val = row['count']
                pct = (count_val / total_with_levels) * 100
                f.write(f"  {level:5s} : {count_val:10,} ({pct:5.2f}%)\n")
        
        print("\n=== Analysis Complete ===")
        print(f"Total log lines: {total_lines:,}")
        print(f"Lines with log levels: {total_with_levels:,}")
        print("\nLog level distribution:")
        for row in counts_list:
            print(f"  {row['log_level']}: {row['count']:,}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
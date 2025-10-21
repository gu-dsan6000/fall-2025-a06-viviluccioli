#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis
Analyzes cluster usage patterns across 6 YARN clusters (2015-2017).
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, count, min as spark_min, max as spark_max,
    input_file_name, to_timestamp
)
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import os
import sys

def create_spark_session(master_url):
    """Create Spark session with appropriate configuration."""
    builder = SparkSession.builder.appName("Problem2-ClusterUsageAnalysis")
    
    if master_url:
        builder = builder.master(master_url).config("spark.executor.memory", "4g")
    
    return builder.getOrCreate()

def process_with_spark(spark, data_path):
    """Process logs with Spark to extract cluster and application data."""
    sc = spark.sparkContext
    
    print(f"Reading from: {data_path}")
    print("Note: Reading only ApplicationMaster logs (container_*_01_000001) for efficiency")
    
    # Read all log files with file paths
    logs_rdd = sc.textFile(data_path)
    logs_df = spark.createDataFrame(logs_rdd.map(lambda x: (x,)), ["value"])
    logs_df = logs_df.withColumn('file_path', input_file_name())
    
    print(f"Total lines read: {logs_df.count():,}")
    
    # Extract timestamps - format: YY/MM/DD HH:MM:SS
    logs_with_time = logs_df.withColumn(
        'timestamp_str',
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1)
    ).filter(col('timestamp_str') != '')
    
    # Parse timestamp to proper datetime
    logs_with_time = logs_with_time.withColumn(
        'timestamp',
        to_timestamp('timestamp_str', 'yy/MM/dd HH:mm:ss')
    )
    
    # Extract application ID from file path
    # File pattern: container_<cluster_id>_<app_number>_01_000001.log
    # Or directory pattern: application_<cluster_id>_<app_number>
    logs_with_app = logs_with_time.withColumn(
        'application_id',
        regexp_extract('file_path', r'application_(\d+_\d+)', 1)
    ).filter(col('application_id') != '')
    
    # Extract cluster ID (timestamp) and app number
    logs_with_app = logs_with_app.withColumn(
        'cluster_id',
        regexp_extract('application_id', r'^(\d+)_', 1)
    ).withColumn(
        'app_number',
        regexp_extract('application_id', r'_(\d+)$', 1)
    )
    
    logs_with_app.cache()
    
    print(f"Lines with parsed data: {logs_with_app.count():,}")
    
    # Aggregate by application: get start and end times
    app_timeline = (logs_with_app
                   .groupBy('cluster_id', 'application_id', 'app_number')
                   .agg(
                       spark_min('timestamp').alias('start_time'),
                       spark_max('timestamp').alias('end_time')
                   )
                   .orderBy('cluster_id', 'app_number'))
    
    # Aggregate by cluster
    cluster_summary = (app_timeline
                      .groupBy('cluster_id')
                      .agg(
                          count('*').alias('num_applications'),
                          spark_min('start_time').alias('cluster_first_app'),
                          spark_max('end_time').alias('cluster_last_app')
                      )
                      .orderBy(col('num_applications').desc()))
    
    return app_timeline, cluster_summary

def create_visualizations(timeline_df, cluster_df):
    """Create visualizations using Seaborn."""
    
    # Set style
    sns.set_style("whitegrid")
    plt.rcParams['figure.dpi'] = 100
    
    # Visualization 1: Bar chart of applications per cluster
    print("Creating bar chart...")
    fig, ax = plt.subplots(figsize=(12, 6))
    
    cluster_sorted = cluster_df.sort_values('num_applications', ascending=False)
    
    # Create color palette
    colors = sns.color_palette("husl", len(cluster_sorted))
    
    bars = ax.bar(
        range(len(cluster_sorted)),
        cluster_sorted['num_applications'],
        color=colors
    )
    
    # Add value labels on bars
    for i, (bar, count) in enumerate(zip(bars, cluster_sorted['num_applications'])):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
               f'{int(count)}', ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    ax.set_xlabel('Cluster ID (Unix Timestamp)', fontsize=12)
    ax.set_ylabel('Number of Applications', fontsize=12)
    ax.set_title('Applications per Cluster', 
                 fontsize=14, fontweight='bold')
    ax.set_xticks(range(len(cluster_sorted)))
    ax.set_xticklabels(cluster_sorted['cluster_id'], rotation=45, ha='right')
    
    # Add grid for readability
    ax.yaxis.grid(True, alpha=0.3)
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    plt.savefig('data/output/problem2_bar_chart.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # Visualization 2: Density plot for largest cluster
    print("Creating density plot for largest cluster...")
    
    # Find largest cluster
    largest_cluster_id = cluster_sorted.iloc[0]['cluster_id']
    num_apps = cluster_sorted.iloc[0]['num_applications']
    
    # Calculate durations for largest cluster
    cluster_apps = timeline_df[timeline_df['cluster_id'] == largest_cluster_id].copy()
    cluster_apps['duration_seconds'] = (
        cluster_apps['end_time'] - cluster_apps['start_time']
    ).dt.total_seconds()
    
    # Remove any invalid durations
    cluster_apps = cluster_apps[cluster_apps['duration_seconds'] > 0]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Create histogram with KDE
    sns.histplot(
        data=cluster_apps,
        x='duration_seconds',
        kde=True,
        bins=40,
        ax=ax,
        color='steelblue',
        alpha=0.7,
        edgecolor='black',
        linewidth=0.5
    )
    
    # Use log scale to handle skewed data
    ax.set_xscale('log')
    ax.set_xlabel('Job Duration (seconds, log scale)', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title(
        f'Job Duration Distribution - Cluster {largest_cluster_id}\n'
        f'(n={len(cluster_apps)} applications)',
        fontsize=14,
        fontweight='bold'
    )
    
    # Add vertical line for median
    median_duration = cluster_apps['duration_seconds'].median()
    ax.axvline(median_duration, color='red', linestyle='--', linewidth=2, 
               label=f'Median: {median_duration:.0f}s')
    ax.legend()
    
    plt.tight_layout()
    plt.savefig('data/output/problem2_density_plot.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    print("Visualizations created successfully!")

def main():
    parser = argparse.ArgumentParser(description='Analyze cluster usage patterns')
    parser.add_argument('master_url', nargs='?', default=None,
                       help='Spark master URL (e.g., spark://ip:7077)')
    parser.add_argument('--net-id', required=False,
                       help='Your net ID for S3 bucket access')
    parser.add_argument('--skip-spark', action='store_true',
                       help='Skip Spark processing and only regenerate visualizations')
    args = parser.parse_args()
    
    # Ensure output directory exists
    Path('data/output').mkdir(parents=True, exist_ok=True)
    
    if args.skip_spark:
        # Load existing CSVs and regenerate visualizations
        print("Skipping Spark processing, loading existing data...")
        timeline_df = pd.read_csv('data/output/problem2_timeline.csv', 
                                  parse_dates=['start_time', 'end_time'])
        cluster_df = pd.read_csv('data/output/problem2_cluster_summary.csv', 
                                parse_dates=['cluster_first_app', 'cluster_last_app'])
    else:
        # Run Spark processing
        if not args.net_id:
            print("Error: --net-id required when not using --skip-spark")
            return 1
        
        spark = create_spark_session(args.master_url)
        
        try:
            # Determine data path - focus on ApplicationMaster logs only
            if args.master_url:
                data_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/container_*_01_000001.log"
                print("Running on cluster with full dataset (this takes 10-20 minutes)...")
            else:
                data_path = "data/sample/application_*/container_*_01_000001.log"
                
                # Check if files exist
                import glob
                log_files = glob.glob(data_path)
                if not log_files:
                    print("ERROR: No ApplicationMaster logs found in data/sample/")
                    print(f"Looking for: {data_path}")
                    print("Files in sample directory:")
                    os.system("ls -la data/sample/application_*/*.log 2>/dev/null | head -5")
                    sys.exit(1)
                
                print("Running locally with sample data...")
            
            # Process with Spark
            app_timeline, cluster_summary = process_with_spark(spark, data_path)
            
            # Convert to Pandas
            timeline_df = app_timeline.toPandas()
            cluster_df = cluster_summary.toPandas()
            
            # Save CSVs
            print("Saving CSV files...")
            timeline_df.to_csv('data/output/problem2_timeline.csv', index=False)
            cluster_df.to_csv('data/output/problem2_cluster_summary.csv', index=False)
            
        finally:
            spark.stop()
    
    # Generate summary statistics
    print("Writing summary statistics...")
    with open('data/output/problem2_stats.txt', 'w') as f:
        total_clusters = len(cluster_df)
        total_apps = cluster_df['num_applications'].sum()
        avg_apps = total_apps / total_clusters if total_clusters > 0 else 0
        
        f.write("Cluster Usage Analysis\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {int(total_apps)}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        
        for idx, row in cluster_df.iterrows():
            f.write(f"  Cluster {row['cluster_id']}: "
                   f"{int(row['num_applications'])} applications\n")
    
    # Create visualizations
    create_visualizations(timeline_df, cluster_df)
    
    print("\n=== Analysis Complete ===")
    print(f"Total clusters: {len(cluster_df)}")
    print(f"Total applications: {int(cluster_df['num_applications'].sum())}")
    print("\nTop clusters by usage:")
    print(cluster_df.head())

if __name__ == "__main__":
    main()
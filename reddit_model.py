from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

    # Task 1 load data
    # comments = context.read.json("comments-minimal.json.bz2")
    # submissions = context.read.json("submissions.json.bz2")
    # label = context.read.csv("labeled_data.csv", header = 'true')

    # comments.write.parquet("comments.parquet")
    # submissions.write.parquet("submissions.parquet")
    # label.write.parquet("label.parquet")

    comments = context.read.parquet("comments.parquet")
    submissions = context.read.parquet("submissions.parquet")
    label = context.read.parquet("label.parquet")
    
    # Task 2 functional dependencies join two table
    data = label.join(comments, label.Input_id == comments.id,'inner').select(label.Input_id,comments.body,label.labeldjt)
 
    




if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)

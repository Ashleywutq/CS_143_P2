from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, expr,count,to_date,col,from_unixtime, avg
from pyspark.sql.types import ArrayType, StringType, DoubleType,DateType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder,CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import DenseVector
import cleantext
import logging

def convert(text):
    final =[]
    for i in range(len(text)):
        final += text[i].split()
    return final  

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
 
    #Task 4
    sanitize = udf(cleantext.sanitize, ArrayType(StringType()))
    # context.udf.register("sanitize", sanitize_udf)
    # context.registerDataFrameAsTable(data, "data")
    data = data.withColumn('cleaned_body', sanitize(data.body))

    #Task 5
    convert_udf = udf(convert,ArrayType(StringType()))
    data = data.withColumn('cleaned_body', convert_udf(data.cleaned_body))

    #Task 6A
    cv = CountVectorizer(inputCol="cleaned_body", outputCol="features", binary = True, minDF=10.0)
    model = cv.fit(data)
    data = model.transform(data)
        
    #Task 6B
    pos = data.withColumn("label", expr("case when labeldjt = '1' then 1 else 0 end"))
    neg = data.withColumn("label", expr("case when labeldjt = '-1' then 1 else 0 end"))

#    # Task 7
#    # Initialize two logistic regression models.
#    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
#    poslr = LogisticRegression(labelCol='label', featuresCol="features", maxIter=10).setThreshold(0.2)
#    neglr = LogisticRegression(labelCol='label', featuresCol="features", maxIter=10).setThreshold(0.25)
#     # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
#    posEvaluator = BinaryClassificationEvaluator()
#    negEvaluator = BinaryClassificationEvaluator()
#    # There are a few parameters associated with logistic regression. We do not know what they are a priori.
#    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
#    # We will assume the parameter is 1.0. Grid search takes forever.
#    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
#    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
#    # We initialize a 5 fold cross-validation pipeline.
#    posCrossval = CrossValidator(
#         estimator=poslr,
#         evaluator=posEvaluator,
#         estimatorParamMaps=posParamGrid,
#         numFolds=5)
#    negCrossval = CrossValidator(
#         estimator=neglr,
#         evaluator=negEvaluator,
#         estimatorParamMaps=negParamGrid,
#         numFolds=5)
#    # Although crossvalidation creates its own train/test sets for
#    # tuning, we still need a labeled test set, because it is not
#    # accessible from the crossvalidator (argh!)
#    # Split the data 50/50
#    posTrain, posTest = pos.randomSplit([0.5, 0.5])
#    negTrain, negTest = neg.randomSplit([0.5, 0.5])
#    # Train the models
#    print("Training positive classifier...")
#    posModel = posCrossval.fit(posTrain)
#    print("Training negative classifier...")
#    negModel = negCrossval.fit(negTrain)
#    # Once we train the models, we don't want to do it again. We can save the models and load them again later.
#    posModel.save("project2/pos.model")
#    negModel.save("project2/neg.model")

    #task 8
    comments_truc = comments.select(comments.id, comments.body, comments.created_utc, comments.link_id.substr(4,12).alias('link_id'), comments.author_flair_text,comments.score.alias('cscore'))
    submissions_truc = submissions.select(submissions.id.alias('sub_id'),  submissions.title,submissions.score.alias('sscore'))
    data2 = comments_truc.join(submissions_truc, comments_truc.link_id == submissions_truc.sub_id,'inner')
    # data2.explain()

    #task 9
    data2 = data2.sample(False, 0.2, None)
    #remove some data
    data2 = data2.filter("body not like '%/s%'").filter("body not like '&gt%'")
    #Task 4
    data2 = data2.withColumn('cleaned_body', sanitize(data2.body))
    #Task 5
    data2 = data2.withColumn('cleaned_body', convert_udf(data2.cleaned_body))
    #Task 6A
    data2 = model.transform(data2)
    #inferece
    posModel2 = CrossValidatorModel.load("project2/pos.model")
    negModel2 = CrossValidatorModel.load("project2/neg.model")
    posResult = posModel2.transform(data2)
    posResult = posResult.withColumnRenamed('prediction', 'pos').drop('rawPrediction','probability')
    negResult = negModel2.transform(posResult)
    results = negResult.withColumnRenamed('prediction', 'neg').drop('rawPrediction','probability')
#   results.write.parquet("project2/results.parquet")

    # # Task 10
    # # results = context.read.parquet("project2/results.parquet")
    # #    1. Compute the percentage of comments that were positive and the percentage of comments that were negative across all submissions/posts. You will want to do this in Spark.
    # q1 = results.select('pos','neg')
    # q1 = q1.groupBy().avg('pos', 'neg')
    # q1.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save('q1.csv')

    # # #   2. Compute the percentage of comments that were positive and
    # # # the percentage of comments that were negative across all days.
    # # # Check out from from_unixtime function.
    # q2 = results.select(to_date(results.created_utc.cast('timestamp')).alias('date'),results.pos,results.neg).groupBy('date').avg('pos','neg')
    # q2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save('q2.csv')

    # # # 3.Compute the percentage of comments that were positive
    # # # and the percentage of comments that were negative across all states.
    # # # There is a Python list of US States here. Just copy and paste it.
    # states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California',
    #            'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida',
    #            'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas',
    #            'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
    #            'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
    #            'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina',
    #            'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
    #            'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont',
    #            'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
    # q3 = results[results.author_flair_text.isin(states)].groupBy('author_flair_text').avg('pos','neg')
    # q3.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save('q3.csv')
    
    # # #   4. Compute the percentage of comments that were positive
    # # # and the percentage of comments that were negative by comment and story score, independently.
    # # # You will want to be careful about quotes. Check out the quoteAll option.
    # q4_c = results.groupBy('cscore').avg('pos','neg')
    # q4_s = results.groupBy('sscore').avg('pos','neg')
    # q4_c.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save('q4_c.csv')
    # q4_s.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save('q4_s.csv')

    # # #5. Any other dimensions you compute will receive extra credit
    # # # if they make sense based on the datayou have.

    # final 4: Give a list of the top 10 positive stories (have the highest percentage of positive comments) and the top 10 negative stories (have the highest percentage of negative comments).
    final4 = results.groupBy('title').agg(avg('pos').alias('avgpos'), avg('neg').alias('avgneg'))
    final4.orderBy('avgpos', ascending=0).limit(10).show(truncate=False)
    final4.orderBy('avgneg', ascending=0).limit(10).show(truncate=False)


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)

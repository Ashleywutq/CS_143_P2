from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf,expr
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator


import cleantext

def convert(text):
    final =[]
    for i in range(len(text)):
        final += text[i].split()
    return final  # list

def stripthree(text):
    return text[3:]

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
    
    # # Task 2 functional dependencies join two table
    # data = label.join(comments, label.Input_id == comments.id,'inner').select(label.Input_id,comments.body,label.labeldjt)
 
    # #Task 4 
    # sanitize = udf(cleantext.sanitize, ArrayType(StringType()))
    # # context.udf.register("sanitize", sanitize_udf)
    # # context.registerDataFrameAsTable(data, "data")
    # data = data.withColumn('cleaned_body', sanitize(data.body))

    # #Task 5
    # convert_udf = udf(convert,ArrayType(StringType()))
    # data = data.withColumn('cleaned_body', convert_udf(data.cleaned_body))

    # #Task 6A
    # cv = CountVectorizer(inputCol="cleaned_body", outputCol="features", binary = True, minDF=10.0)
    # model = cv.fit(data)
    # data = model.transform(data)

    # #Task 6B
    # pos = data.withColumn("label", expr("case when labeldjt = '1' then 1 else 0 end"))
    # neg = data.withColumn("label", expr("case when labeldjt = '-1' then 1 else 0 end"))

    # #Task 7
    # # Initialize two logistic regression models.
    # # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    # poslr = LogisticRegression(labelCol='label', featuresCol="features", maxIter=10)
    # neglr = LogisticRegression(labelCol='label', featuresCol="features", maxIter=10)
    # # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    # posEvaluator = BinaryClassificationEvaluator()
    # negEvaluator = BinaryClassificationEvaluator()
    # # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # # We will assume the parameter is 1.0. Grid search takes forever.
    # posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    # negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # # We initialize a 5 fold cross-validation pipeline.
    # posCrossval = CrossValidator(
    #     estimator=poslr,
    #     evaluator=posEvaluator,
    #     estimatorParamMaps=posParamGrid,
    #     numFolds=5)
    # negCrossval = CrossValidator(
    #     estimator=neglr,
    #     evaluator=negEvaluator,
    #     estimatorParamMaps=negParamGrid,
    #     numFolds=5)
    # # Although crossvalidation creates its own train/test sets for
    # # tuning, we still need a labeled test set, because it is not
    # # accessible from the crossvalidator (argh!)
    # # Split the data 50/50
    # posTrain, posTest = pos.randomSplit([0.5, 0.5])
    # negTrain, negTest = neg.randomSplit([0.5, 0.5])
    # # Train the models
    # print("Training positive classifier...")
    # posModel = posCrossval.fit(posTrain)
    # print("Training negative classifier...")
    # negModel = negCrossval.fit(negTrain)
    # # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    # posModel.save("project2/pos.model")
    # negModel.save("project2/neg.model")

    #task 8
    stripthree_udf = udf(stripthree, StringType())
    comments_truc = comments.select(comments.created_utc, comments.link_id.alias('link_id'), comments.author_flair_text)
    submissions_truc = submissions.select(submissions.id,  submissions.title)
    # data2 = comments_truc.join(submissions_truc, comments_truc.link_id == submissions_truc.id,'inner')
    print(comments_truc.limit(1).collect())


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)

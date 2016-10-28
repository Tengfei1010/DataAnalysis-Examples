#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
 * Created by kevin on 10/27/16.
"""
import os
import sys

# Set the path for spark installation
# this is the path where you have built spark using sbt/sbt assembly
os.environ['SPARK_HOME'] = "/home/kevin/Tools/spark/spark-2.0.1-bin-hadoop2.7"
# Append to PYTHONPATH so that pyspark could be found
sys.path.append("/home/kevin/Tools/spark/spark-2.0.1-bin-hadoop2.7/python")

# Now we are ready to import Spark Modules
try:
    from pyspark import SparkContext, SparkConf
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.tree import GradientBoostedTrees
    from pyspark.mllib.tree import GradientBoostedTreesModel
except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

oheMap = None


def map_rain_rdd(line):
    tokens = line.split(',', -1)
    cat_key = tokens[0] + "::" + tokens[1]
    cat_features = tokens[5: 15]
    numerical_features = tokens[15:]
    return (cat_key, cat_features, numerical_features)


def parse_cat_features(x):
    catfeatureList = []
    for i, item in enumerate(x):
        catfeatureList.append((i, item))
    return catfeatureList


def parse_numerical_features(items):
    new_items = []
    for item in items:
        if int(item) < 0:
            new_items.append(0.0)
        else:
            new_items.append(float(item))
    return new_items


def map_label(x):
    return x.label


def create_ohe_train_data(x_1, x_2, x_3):
    cat_features_indexed = parse_cat_features(x_2)
    cat_feature_ohe = []
    for k in cat_features_indexed:
        if k in oheMap:
            cat_feature_ohe.append(oheMap[k])
        else:
            cat_feature_ohe.append(0.0)

    numerical_features_dbl = parse_numerical_features(x_3)

    features = numerical_features_dbl + cat_feature_ohe
    return LabeledPoint(int(x_1.split("::")[1]), Vectors.dense(features))


def create_gbt_model(sc):
    txtrdd = sc.textFile('sample.txt')
    train_raw_rdd, _ = txtrdd.randomSplit([8, 2], 37L)

    # cache train, test
    train_raw_rdd.cache()
    # test_raw_rdd.cache()

    train_rdd = train_raw_rdd.map(map_rain_rdd)
    print(train_rdd.take(1))

    # 估计有异常
    train_cat_rdd = train_rdd.map(lambda (x_1, x_2, x_3): parse_cat_features(x_2))
    print(train_cat_rdd.take(1))

    global oheMap
    oheMap = train_cat_rdd.flatMap(lambda x: x).distinct().zipWithIndex().collectAsMap()
    #
    ohe_train_rdd = train_rdd.map(lambda (x_1, x_2, x_3): create_ohe_train_data(x_1, x_2, x_3))
    print(ohe_train_rdd.take(1))

    gbt_model = GradientBoostedTrees.trainClassifier(ohe_train_rdd, {})
    gbt_model.save(sc, "myGradientBoostingClassificationModel")


def test_gbt_model(sc):
    txtrdd = sc.textFile('sample.txt')
    train_raw_rdd, test_raw_rdd = txtrdd.randomSplit([8, 2], 17)

    # cache train, test
    train_raw_rdd.cache()
    test_raw_rdd.cache()

    print(test_raw_rdd.count())
    test_rdd = test_raw_rdd.map(map_rain_rdd)
    print(test_rdd.take(1))

    # this is for
    train_rdd = train_raw_rdd.map(map_rain_rdd)
    print(train_rdd.take(1))

    train_cat_rdd = train_rdd.map(lambda (x_1, x_2, x_3): parse_cat_features(x_2))
    print(train_cat_rdd.take(1))

    global oheMap
    oheMap = train_cat_rdd.flatMap(lambda x: x).distinct().zipWithIndex().collectAsMap()
    print(oheMap)
    # end

    ohe_test_rdd = test_rdd.map(lambda (x_1, x_2, x_3): create_ohe_train_data(x_1, x_2, x_3))
    print(ohe_test_rdd.take(1))

    sameModel = GradientBoostedTreesModel.load(sc, "myGradientBoostingClassificationModel")

    predictions = sameModel.predict(ohe_test_rdd.map(lambda x: x.features))
    predictionAndLabel = ohe_test_rdd.map(lambda lp: lp.label).zip(predictions)
    print(predictionAndLabel.take(1))

    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / ohe_test_rdd.count()
    print("GBTR accuracy:{}".format(accuracy))


if __name__ == '__main__':
    # conf = SparkConf().setAppName('test')
    sc = SparkContext()
    # create_gbt_model(sc)
    test_gbt_model(sc)

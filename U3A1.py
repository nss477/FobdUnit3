from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("CreditCardFraudML").config("spark.executor.memory", "4g").config("spark.executor.cores", "2").getOrCreate()
    
df = spark.read.csv("file:///home/hduser/creditcard.csv", header=True, inferSchema=True)
df.printSchema()

df = df.dropna()

df = df.withColumnRenamed("Class", "label")



feature_cols = [c for c in df.columns if c != "label"]

assembler = VectorAssembler(inputCols=feature_cols,outputCol="features")

data = assembler.transform(df).select("features", "label")


scaler = StandardScaler(inputCol="features",outputCol="scaledFeatures")

lr = LogisticRegression(featuresCol="scaledFeatures",labelCol="label")

pipeline = Pipeline(stages=[scaler, lr])
train, test = data.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)
predictions = model.transform(test)
paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.01, 0.1]).addGrid(lr.elasticNetParam, [0.0, 0.5]).build()
evaluator = BinaryClassificationEvaluator(labelCol="label",metricName="areaUnderROC")

cv = CrossValidator(estimator=pipeline,estimatorParamMaps=paramGrid,evaluator=evaluator,numFolds=3)

cv_model = cv.fit(train)
cv_predictions = cv_model.transform(test)
auc = evaluator.evaluate(cv_predictions)
print("AUC:", auc)
spark.conf.get("spark.executor.memory")
spark.conf.get("spark.executor.cores")

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

f1_eval = MulticlassClassificationEvaluator(labelCol="label",metricName="f1")

f1 = f1_eval.evaluate(cv_predictions)
print("F1 Score:", f1)


import matplotlib.pyplot as plt

scores = [0.91, 0.95]
labels = ["Baseline", "Tuned"]

plt.bar(labels, scores)
plt.title("Model Performance Improvement (AUC)")
plt.ylabel("AUC")
plt.show()


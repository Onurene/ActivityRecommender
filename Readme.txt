Run this spark job to preprocess the dataset using spark
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 .\sparkParser.py

Run this spark job to determine recommended activity
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 .\sparkJob.py

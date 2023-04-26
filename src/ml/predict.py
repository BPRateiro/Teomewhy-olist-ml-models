# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot mlflow

# COMMAND ----------

import mlflow

# COMMAND ----------

mlflow.sklearn.load_model('models:/Olist Vendedor Churn/Production')

# COMMAND ----------



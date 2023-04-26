# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot mlflow

# COMMAND ----------

from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
from sklearn import metrics
from sklearn import ensemble

import scikitplot as skplt

import mlflow

import pandas as pd

from feature_engine import imputation

pd.set_option('display.max_rows', 1000)

# COMMAND ----------

# Sample
df = spark.table('silver.analytics.abt_olist_churn').toPandas()

# Base out-of-time
oot = df.dtReference == '2018-01-01'
df_oot = df[oot]

# Base de treino
df_train = df[~oot]
df_train.shape

# COMMAND ----------

# DBTITLE 1,Definindo variáveis
df_train.head()

var_identity = ['dtReference',	'idVendedor']
target = 'flChurn'
to_remove = ['qtdRecencia', target] + var_identity

features = df.columns.tolist()
features = list(set(features) - set(to_remove))
features

# COMMAND ----------

X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features], df_train[target], train_size=0.8, random_state=42)

print('Proporção de churn no treino:', y_train.mean())
print('Proporção de churn no teste:', y_test.mean())

# COMMAND ----------

# DBTITLE 1,Explore
X_train.isna().sum().sort_values(ascending=False)

missing_minus_100 = ['avgIntervaloVendas',
                     'maxNota',
                     'avgNota',
                     'medianNota',
                     'minNota',
                     'minVolumeProduto',
                     'avgVolumeProduto',
                     'medianVolumeProduto',
                     'maxVolumeProduto']

missing_0 = ['maxQtdeParcelas',
             'minQtdeParcelas',
             'medianQtdeParcelas',
             'avgQtdeParcelas']

# COMMAND ----------

# DBTITLE 1,Define experiment
mlflow.set_experiment('/Users/bruno.rateiro@gmail.com/olist-churn-billito')

# COMMAND ----------

# DBTITLE 1,Model
with mlflow.start_run():
    mlflow.sklearn.autolog()

    imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number=-100, variables=missing_minus_100)
    imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0, variables=missing_0)

#     model = tree.DecisionTreeClassifier(min_samples_leaf=25)
    model = ensemble.RandomForestClassifier(min_samples_leaf=50,
                                            n_jobs=-1,
                                            random_state=42,
                                            n_estimators=300)
    
    params = {'min_samples_leaf': [5,10,20],
              'n_estimators': [300,400,450,500]}
    
    grid = model_selection.GridSearchCV(model, params, cv=3, verbose=3, scoring='roc_auc')

    model_pipeline = pipeline.Pipeline([('Imputer -100', imputer_minus_100),
                                        ('Imputer 0', imputer_0),
                                        ('Grid Search', grid)])

    model_pipeline.fit(X_train, y_train)

    auc_train = metrics.roc_auc_score(y_train, model_pipeline.predict_proba(X_train)[:,1])
    auc_test = metrics.roc_auc_score(y_test, model_pipeline.predict_proba(X_test)[:,1])
    auc_oot = metrics.roc_auc_score(df_oot[target], model_pipeline.predict_proba(df_oot[features])[:,1])
    
    metrics_model = {'auc_train': auc_train,
                     'auc_test': auc_test,
                     'auc_oot': auc_oot}
    
    mlflow.log_metrics(metrics_model)

# COMMAND ----------

pd.DataFrame(grid.cv_results_).sort_values(by='rank_test_score')

# COMMAND ----------

# DBTITLE 1,Performance evaluation on training set
predict = model_pipeline.predict(X_train)
proba = model_pipeline.predict_proba(X_train)

# COMMAND ----------

skplt.metrics.plot_roc(y_train, proba)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_train, proba)

# COMMAND ----------

# DBTITLE 1,Performance evaluation on test set
proba_test = model_pipeline.predict_proba(X_test)
skplt.metrics.plot_roc_curve(y_test, proba_test)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_test, proba_test)

# COMMAND ----------

# DBTITLE 1,Performance evaluation on OOT
probas_oot = model_pipeline.predict_proba(df_oot[features])
skplt.metrics.plot_roc_curve(df_oot[target], probas_oot)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(df_oot[target], probas_oot)

# COMMAND ----------

# DBTITLE 1,Feature importance
fs_importance = model_pipeline[-1].feature_importances_
fs_cols = model_pipeline[:-1].transform(X_train.head(1)).columns.tolist()

pd.Series(fs_importance, index=fs_cols).sort_values(ascending = False)

# COMMAND ----------

skplt.metrics.plot_lift_curve(y_train, proba)

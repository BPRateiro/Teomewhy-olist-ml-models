# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import datetime

def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()
    
def table_exists(database, table):
    count = (spark.sql(f'SHOW TABLES FROM {database}')
                  .filter(f"tableName = '{table_name}'")
                  .count())
    return  bool(count)

def date_range(dt_start, dt_stop, period='daily'):
    datetime_start = datetime.datetime.strptime(dt_start, '%Y-%m-%d')
    datetime_stop = datetime.datetime.strptime(dt_stop, '%Y-%m-%d')

    dates = list()
    while datetime_start <= datetime_stop:
        dates.append(datetime_start.strftime('%Y-%m-%d'))
        datetime_start += datetime.timedelta(days=1)
    
    if period == 'daily':
        return dates
    if period == 'monthly':
        return [i for i in dates if i.endswith('01')]
    
table = dbutils.widgets.get('table')
database = 'silver.analytics'
table_name = f'fs_vendedor_{table}'

query = import_query(f"{table}.sql")

date_start, date_stop = dbutils.widgets.get('date_start'), dbutils.widgets.get('date_stop')
period = dbutils.widgets.get('period')
dates = date_range(date_start, date_stop, period)

print(table_name)
print(dates[:5])

# COMMAND ----------

# Não tenho permissão pra alterar o database
if not table_exists(database, table_name):
    # Criando a tabela
    for i in dates:
        (spark.sql(query.format(date=dates.pop(0)))
              .coalesce(1)
              .write
              .format('delta')
              .mode('overwrite')
              .option('overwriteSchema', 'true')
              .partitionBy('dtReference')
              .saveAsTable(f'{database}.{table_name}')
        )
else:
    # Realizando update
    for i in dates:
        spark.sql(f"DELETE FROM {database}.{table_name} WHERE dtReference = '{dates[0]}'")
        (spark.sql(query.format(date=dates.pop(0)))
              .coalesce(1)
              .write
              .format('delta')
              .mode('append')
              .saveAsTable(f'{database}.{table_name}')
        )

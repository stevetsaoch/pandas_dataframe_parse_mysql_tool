# dependency package
import mysql.connector
import pandas as pd
import datetime
import numpy as np

# personal mysql data for login mysql server with python
from mysql_inf import mysql_information as myinf

# import tool
from pandas_dataframe_parse_mysql_tool import pandas_dataframe_parse_mysql_tool as pdsql

# import train raw, you can find this data at: 
# https://www.kaggle.com/c/tabular-playground-series-jul-2021
train_raw = pd.read_csv('~/tabular_playground_June/train.csv', 
                    parse_dates = ['date_time'],
                    infer_datetime_format = True)
                    
train_raw.convert_dtypes()

# create a mysql connector
inf = myinf()
engine = mysql.connector.connect(auth_plugin='mysql_native_password', **inf.inf)

# create a tool object
tool = pdsql(train_raw, engine)

# data parse
## accuracy mode
# tool.dtype_parse(decimal_type_mode = 'accuracy', digit_num=4)
## space_save mode
# tool.dtype_parse(decimal_type_mode = 'space_save')
## all_include mode
tool.dtype_parse(decimal_type_mode = 'all_include')
tool.mysql_create_table_syntax('train_raw', unique_key=False)

# check if the syntax is correct
print(tool.creat_table_syntax)

# create db and table in the db
tool.mysql_create_db_table(db_name='test_db')

# insert all data
tool.insert_data_multi()
engine.close()

## new subclass example
# create engine
engine = mysql.connector.connect(auth_plugin='mysql_native_password', **inf.inf)

# read data from target table
rdf = pd.read_sql('SELECT * FROM train_raw', con = engine)

# test sample
np_test = np.random.random_sample((rdf.shape[0],))
series_test = pd.Series( np.random.random_sample((rdf.shape[0],)))
df_test = pd.DataFrame(np.random.random_sample((rdf.shape[0],2)), columns = ['col1', 'col2'])

# import subclass
from pandas_dataframe_parse_mysql_tool import insert_column_tool as incol
tool2 = incol() # call object

# insert new column from 1d nparray
tool2.insert_new_col(engine= engine, db_name='test_db', table_name='train_raw', data = np_test, data_col_name='trail_np', decimal_type_mode = 'all_include')

# insert new column from pd.series
tool2.insert_new_col(engine= engine, db_name='test_db', table_name='train_raw', data = series_test, data_col_name='trail_series', decimal_type_mode = 'all_include')

# insert new column from pd.Dataframe
tool2.insert_new_col(engine= engine, db_name='test_db', table_name='train_raw', data = df_test, data_col_name=['test_1', 'test2'], decimal_type_mode = 'all_include')




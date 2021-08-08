# dependency package
import mysql.connector
import pandas as pd
import datetime

# personal mysql data for login mysql server with python
from mysql_inf import mysql_information as myinf

# tool import
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
# accuracy mode
# tool.dtype_parse(decimal_type_mode = 'accuracy', digit_num=4)
# space_save mode
tool.dtype_parse(decimal_type_mode = 'space_save')
tool.mysql_create_table_syntax('train_raw', unique_key=False)

# check if the syntax is correct
print(tool.creat_table_syntax)

# create db and table in the db
tool.mysql_create_db_table(db_name='test_db')

# insert all data
tool.insert_data_multi()
engine.close()




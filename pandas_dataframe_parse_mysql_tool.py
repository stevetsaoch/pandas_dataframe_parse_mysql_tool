# dependency package
import mysql.connector
import pandas as pd
import numpy as np
import datetime

# type hint
from typing import Union, Callable
from pandas.core.algorithms import isin, unique1d
from pandas.core.frame import DataFrame
from pandas.core.series import Series

# personal mysql data
from mysql_inf import mysql_information as myinf

class pandas_dataframe_parse_mysql_tool(object):
    # range of unsigned int
    dtype_int= \
    {
        'TINYINT': (-128, 127),
        'SMALLINT': (-32768, 32767),
        'MEDIUMINT': (-8388608, 8388607),
        'INT': (-2147483648, 2147483647),
        'BIGINT': (-9223372036854775808, 9223372036854775807)
    }
    
    # bytes use of approximate types
    dtype_decimal = \
    {
        'DECIMAL': {'len': (0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 'bytes': (0, 1, 1, 2, 2, 3, 3, 4, 4, 4)},
        'FLOAT': {'len': 6, 'bytes': 4},
        'DOUBLE': {'len': 16, 'bytes': 8}
    }

    def __init__(self, df: DataFrame, engine: 'mysql.connector', unique_col: str = None):

        # class attributes
        self.df = df
        self.engine = engine
        self.columns_dtype = {} # recording columns name and dtype
        self.unique_col = unique_col

        # create unique column if it was not assigned
        if self.unique_col == None:
            self.df['unique_key'] = np.arange(self.df.shape[0])
            self.unique_col = 'unique_key'

    # return bytes usage of decimal type in mysql
    def bytes_of_decimal(self, value: Union[float, str], len_dig: tuple, bytes_use: tuple) -> int:
        '''
        Parameters:
        ==============================================================
        This function will calculate bytes of use when you assign value as DECIMAL in mysql.
        value: value you want to calculate.
        len_dig: Digit length tuple.
        bytes_use : Bytes use of different length in len_dig.
        '''

        # change dtype of value if it's not str
        if 'str' not in str(type(value)):
            value = str(value)
        
        # get length of int and digit part
        int_part, dig_part = value.split('.')
        int_len, dig_len = len(int_part), len(dig_part)
        int_bytes = 0
        dig_bytes = 0

        while int_len > 0:
            for dig in len_dig[::-1]:
                if int_len - dig >= 0:
                    int_len -= dig
                    int_bytes += bytes_use[len_dig.index(dig)]

        while dig_len > 0:
            for dig in len_dig[::-1]:
                if dig_len - dig >= 0:
                    dig_len -= dig
                    dig_bytes += bytes_use[len_dig.index(dig)]
                    
        return int_bytes + dig_bytes


    # dict with pair of columns and dtype then used in creating mysql table syntax
    def dtype_parse(self, data: Union[DataFrame, Series, np.ndarray] = None,
                    data_col_name: Union[str, list] = None,
                    decimal_type_mode: str = 'space_save', digit_num: int = 2,
                    decimal_parse_func: Callable = bytes_of_decimal) -> 'pandas_dataframe_parse_mysql_tool':

        '''
        Parameters:
        ==============================================================
        data: DataFrame, Series and 1D numpy array are list acceptable entry, default is None.  
              data = None is for creating new table. Purposes of other entries are for update 
              table and insert new column/s.
        data_col_name: customize column name. It is necessary for ndarray and pd.Series. 
                       For pd.DataFrame, you have to entry a list which is same columns number as you entry data, 
                       otherwies it will use orginal column name.
        digit_num: digit number behind digit point.
        decimal_parse_func: function for analysing bytes use for approximate data type if assigned it as decimal
        decimal_type_mode: Three ways to define which dtype will be assigned to column
        * accuracy: Using DECIMAL(M,D), digit_num is changeable
        * space_save: Calculate how many memory sapce need before assigning decimal 
                      to max value of that column. If it is higher than 4, then will
                      consider using float or double base on integer part length. This mode will 
                      keep all integer part and giving rest space to digit part accroding to mysql 
                      default.
        * all_include: Include all orginal data digit as possible.

        '''
        try:
            # convert data for different instance
            if isinstance(data, DataFrame):
                temp_df = data
                # check if custom column name was used
                if isinstance(data_col_name, list):
                    temp_df.columns = data_col_name
                    dtype_series = temp_df.dtypes
                else:
                    dtype_series = temp_df.dtypes

            elif isinstance(data, Series):
                temp_df = pd.DataFrame(data = data, columns = [f'{data_col_name}'])
                dtype_series = temp_df.dtypes
                
            elif isinstance(data, np.ndarray):
                temp_df = pd.DataFrame(data = data, columns = [f'{data_col_name}'])
                dtype_series = temp_df.dtypes
            
            elif data == None:
                temp_df = self.df
                dtype_series = temp_df.dtypes

            else:
                raise
            for col in list(dtype_series.index):

                if 'int' in str(dtype_series[col]):
                    # find most space fit int type base on value
                    max_int = temp_df[col].max(skipna=True)
                    for key in self.dtype_int.keys():
                        if max_int <= max(self.dtype_int[key]):
                            self.columns_dtype.update({f'{col}': f'{key}'})
                            break
                
                elif 'float' in str(dtype_series[col]):
                    max_float = temp_df[col].max(skipna=True)
                    max_float_str_int_len = len(str(max_float).split('.')[0]) # length of int part of max value in column
                    max_float_str_dig_len = len(str(max_float).split('.')[1]) # length of digit part of max value in column

                    if decimal_type_mode == 'accuracy':
                        self.columns_dtype.update({f'{col}':f'DECIMAL({max_float_str_int_len + digit_num}, {digit_num})'})

                    elif decimal_type_mode == 'space_save':
                        # calculate bytes use if assign data as decimal
                        decimal_bytes_use = decimal_parse_func(max_float, self.dtype_decimal['DECIMAL']['len'],
                                                               self.dtype_decimal['DECIMAL']['bytes'])
                        
                        # use decimal if max value of bytes use when assigning deciaml type is less than float type 
                        if decimal_bytes_use < 4:
                            self.columns_dtype.update({f'{col}':f'DECIMAL({max_float_str_int_len + max_float_str_dig_len},{max_float_str_dig_len})'})
                        
                        # use decimal if length of integer part is larger than 16 
                        # which will cause uncertainty when assign data type as double. 
                        # Digit part will be forced as two digit.
                        elif max_float_str_int_len > 16:
                            self.columns_dtype.update({f'{col}':f'DECIMAL({max_float_str_int_len + 2},{2})'})

                        # using float or double base on length of integer part
                        else:
                            if max_float_str_int_len <= 6:
                                self.columns_dtype.update({f'{col}':f'FLOAT'})
                            else:
                                self.columns_dtype.update({f'{col}':f'DOUBLE'})
                    
                    # all_include mode will include all orginal data digit as possible
                    elif decimal_type_mode == 'all_include':
                        if max_float_str_int_len + max_float_str_dig_len > 65:
                            dig_len = 65 - max_float_str_int_len
                            self.columns_dtype.update({f'{col}':f'DECIMAL({max_float_str_int_len + dig_len},{dig_len})'}) 
                        elif max_float_str_dig_len > 30:
                            self.columns_dtype.update({f'{col}':f'DECIMAL({max_float_str_int_len + 30},{30})'})
                        else:
                            self.columns_dtype.update({f'{col}':f'DECIMAL({max_float_str_int_len + max_float_str_dig_len},{max_float_str_dig_len})'})

                elif 'object' in str(dtype_series[col]):
                    # using max length as length of VARCHAR
                    max_length = max(temp_df[col].str.len())
                    self.columns_dtype.update({f'{col}':f'VARCHAR({max_length})'})

                elif any([True if i in str(dtype_series[col]) else None for i in ['date', 'datetime']]):
                    t_series = temp_df[col].dt.time # get time from datetime column
                    x = sum(t_series != datetime.time(0,0,0)) # check if all time is 00:00:00
                    # if all time equal to 00:00:00, use DATE rather than DATETIME/TIMESTAMP
                    if x > 0:
                        self.columns_dtype.update({f'{col}':'TIMESTAMP'})
                        # change dtype timestamp to str for later inserting into mysql databases
                        temp_df[col] = temp_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        self.columns_dtype.update({f'{col}':'DATE'})
                        # change dtype timestamp to str for later inserting into mysql databases
                        temp_df[col] = temp_df[col].dt.strftime('%Y-%m-%d')

            return self
        except RuntimeError:
            print('Please entry a numpy array, pandas series')
        except:
            raise
 
    def mysql_create_table_syntax(self, table_name: str) -> 'pandas_dataframe_parse_mysql_tool':
        # check if table exist
        self.table_name = table_name
        self.create_table_syntax = f'CREATE TABLE {self.table_name}('
        
        for i, key in enumerate(self.columns_dtype.keys()):
            try: 

                if i < len(self.columns_dtype.keys()) - 1:
                    
                    if key == self.unique_col:
                        self.create_table_syntax = self.create_table_syntax + f'{key} {self.columns_dtype[key]} UNIQUE, '
                    
                    else:    
                        self.create_table_syntax = self.create_table_syntax + f'{key} {self.columns_dtype[key]}, '

                else:

                    if key == self.unique_col:
                        self.create_table_syntax = self.create_table_syntax + f'{key} {self.columns_dtype[key]} UNIQUE)'
                    else:
                        self.create_table_syntax = self.create_table_syntax + f'{key} {self.columns_dtype[key]})'

            except KeyError as e:
                print('Please assign a column or a list of columns.')
                break

        return self

    def mysql_create_db_table(self, db_name: str):
        # check if database exist
        try:
            cursor_db_table = self.engine.cursor(buffered=True)
            # if syntax is not completed, raise error
            if self.create_table_syntax == f'CREATE TABLE {self.table_name}( ':
                raise SyntaxWarning

            else:
            # check if db exist, then create table
                cursor_db_table.execute(f'USE {db_name}')
                cursor_db_table.execute(f'SELECT * FROM {self.table_name}') # check if table exist
                print(f'Table {self.table_name} exist.')

        except mysql.connector.errors.ProgrammingError as e:

            # if db not exist, create a db
            if 'Unknown database' in str(e):
                cursor_db_table.execute(f'CREATE DATABASE {db_name}')
                cursor_db_table.execute(f'USE {db_name}')
                cursor_db_table.execute(f'{self.create_table_syntax}')
                print(f'Database {db_name} and table {self.table_name} are created.')

            # if database exist but table not exist
            elif 'Unknown table' in str(e):
                cursor_db_table.execute(f'{self.create_table_syntax}')
                print(f'Table {self.table_name} is created.')

            # if database exist but table not exist
            elif f'Table \'{db_name}.{self.table_name}\' doesn\'t exist' in str(e):
                cursor_db_table.execute(f'{self.create_table_syntax}')
                print(f'Table {self.table_name} is created.')
        except SyntaxWarning:
            print('Create table syntax is not completed, please entry param correctly.')

        finally:
            cursor_db_table.close()
    
    def insert_data_multi(self) -> 'pandas_dataframe_parse_mysql_tool':
        # change NaN to None because mysql can't parse np.nan type 
        self.df = self.df.where((pd.notnull(self.df)), None)

        # Insert multiple record to db
        # establish cursor
        cursor_insert = self.engine.cursor(buffered = True)

        # column and values
        col_names = self.df.keys()
        values = self.df.values.tolist()

        # preparing sql syntax
        sql_col_names = ','.join(col_names)
        value_sql = ','.join(['%s'] * self.df.shape[1])
        insert_syntax = f'INSERT INTO {self.table_name} ({sql_col_names}) VALUES ({value_sql})'

        try:
        # insert
            cursor_insert.executemany(insert_syntax, values)
        
        except:
            self.engine.rollback()        

        # commit
        finally:
            self.engine.commit()
            cursor_insert.close()
        
        return self

class insert_column_tool(pandas_dataframe_parse_mysql_tool):

    def __init__(self):
        # dtype dict for dtype sparse
        self.columns_dtype = {}

    def insert_new_col(self, engine: 'mysql.connector', db_name: str, table_name: str,
                      data: Union[DataFrame, Series, np.ndarray] = None,
                      data_col_name: Union[str, list] = None,
                      decimal_type_mode: str = 'space_save', digit_num: int = 2,
                      decimal_parse_func: Callable = pandas_dataframe_parse_mysql_tool.bytes_of_decimal):
        try:
            # check if database and table exist
            cursor_insert_col = engine.cursor(buffered=True)
            cursor_insert_col.execute(f'USE {db_name}') # check if database exist
            cursor_insert_col.execute(f'SELECT * FROM {table_name}') # check if table exist

            # find unique key in the exist table
            unique_key = pd.read_sql(f'SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC WHERE TC.TABLE_NAME = \
                                       \'{table_name}\' AND TC.CONSTRAINT_TYPE = \'UNIQUE\';', con = engine)
            unique_key = unique_key['CONSTRAINT_NAME'][0]

            # check length of new column is equal to table
            table = pd.read_sql(f'SELECT * FROM {table_name}', con = engine)
            table_row_num = len(table[unique_key])
            
            if isinstance(data, Series):
                if len(data) != table_row_num:
                    print('Please entry Series with equal row number as your table.')
                else:
                    super().dtype_parse(data, data_col_name, decimal_type_mode, digit_num, decimal_parse_func)
                    # add new column
                    cursor_insert_col.execute(f'ALTER TABLE {table_name} ADD({data_col_name} {self.columns_dtype[data_col_name]});')
                    # update value
                    # convert values to list of tuples 
                    data = data.tolist()
                    values = [(data[i], table[unique_key].tolist()[i]) for i in range(len(data))]

                    # update syntax
                    update_syntax = f'UPDATE {table_name} SET {data_col_name} = (%s) WHERE {unique_key} = (%s)'

                    try:
                    # update
                        cursor_insert_col.executemany(update_syntax, values)
                    
                    except IndexError:
                        engine.rollback()

                    # commit
                    finally:
                        engine.commit()
                        cursor_insert_col.close()
                        print('Update completed.')                     
                
            elif isinstance(data, np.ndarray):
                if len(data) != table_row_num:
                    print('Please entry ndarray with equal row number as your table.')
                else:
                    # parse the dtype
                    super().dtype_parse(data, data_col_name, decimal_type_mode, digit_num, decimal_parse_func)
                    # add new column
                    cursor_insert_col.execute(f'ALTER TABLE {table_name} ADD({data_col_name} {self.columns_dtype[data_col_name]});')
                    # insert value
                    # column and values
                    data = data.tolist()
                    values = [(data[i], table[unique_key].tolist()[i]) for i in range(len(data))]
                    # update syntax
                    update_syntax = f'UPDATE {table_name} SET {data_col_name} = (%s) WHERE {unique_key} = (%s)'

                    try:
                    # update
                        cursor_insert_col.executemany(update_syntax, values)
                    
                    except IndexError:
                        engine.rollback()

                    # commit
                    finally:
                        engine.commit()
                        cursor_insert_col.close()
                        print('Update completed.')              
                        
            elif isinstance(data, DataFrame):
                if data.shape[0] != table_row_num:
                    print('Please entry Dataframe with equal row number as your table.')
                else:
                    # parse the dtype
                    super().dtype_parse(data, data_col_name, decimal_type_mode, digit_num, decimal_parse_func)
                    # add new column, check if you specified custom column name
                    if isinstance(data_col_name, list):
                        data_cols = data_col_name
                    else:
                        data_cols = data.keys() # use original column name
                    
                    for col in data_cols:
                        # add new column
                        cursor_insert_col.execute(f'ALTER TABLE {table_name} ADD({col} {self.columns_dtype[col]});')
                        # update value
                        values = [(data[col].tolist()[i], table[unique_key].tolist()[i]) for i in range(data.shape[0])]
                        update_syntax = f'UPDATE {table_name} SET {col} = (%s) WHERE {unique_key} = (%s)'
 
                        try:
                        # update
                            cursor_insert_col.executemany(update_syntax, values)
                        
                        except:
                            engine.rollback()

                        # commit
                        finally:
                            engine.commit()
                    
                    cursor_insert_col.close()
                    print('Update completed.')              
        

        except mysql.connector.errors.ProgrammingError as e:

            # if db not exist, create a db
            if 'Unknown database' in str(e):
                print('Please entry a exist database.')

            elif f'Table \'{db_name}.{self.table_name}\' doesn\'t exist' in str(e):
                print('Table not exist, please entry correct table name.')
            
            else:
                raise
        
    




  
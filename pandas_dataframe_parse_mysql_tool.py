# dependency package
import mysql.connector
import pandas as pd
import numpy as np
import datetime

# for function annotation
from pandas.core.frame import DataFrame

# personal mysql data
from mysql_inf import mysql_information as myinf

class pandas_dataframe_parse_mysql_tool():
    def __init__(self, df: DataFrame, engine: 'mysql.connector'):
        # range of unsigned int
        self.dtype_int= {
            'TINYINT': (-128, 127),
            'SMALLINT': (-32768, 32767),
            'MEDIUMINT': (-8388608, 8388607),
            'INT': (-2147483648, 2147483647),
            'BIGINT': (-9223372036854775808, 9223372036854775807)
        }
        # range of approximate types
        self.dtype_decimal = {
            'DECIMAL': {'len': (0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 'bytes': (0, 1, 1, 2, 2, 3, 3, 4, 4, 4)},
            'FLOAT': {'len': 6, 'bytes': 4},
            'DOUBLE': {'len': 16, 'bytes': 8}
        }

        self.df = df
        self.engine = engine
        self.columns_dtype = {} # recording columns name and dtype
    
    # return bytes usage of decimal type in mysql
    def bytes_of_decimal(value: str or float, len_dig: tuple, bytes_use: tuple) -> int:
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

    # update dict with columns and dtype pair that will be used in creating mysql table syntax
    def dtype_parse(self, decimal_type_mode: str = 'space_save', digit_num: int = 2, decimal_parse_func = bytes_of_decimal) -> 'pandas_dataframe_parse_mysql_tool':
        try:
            dtype_series = self.df.dtypes # dataframe dtypes
            for col in list(dtype_series.index):

                if 'int' in str(dtype_series[col]):
                    # find most space fit int type base on value
                    max_int = self.df[col].max(skipna=True)
                    for key in self.dtype_int.keys():
                        if max_int <= max(self.dtype_int[key]):
                            self.columns_dtype.update({f'{col}': f'{key}'})
                            break
                
                elif 'float' in str(dtype_series[col]):
                    max_float = self.df[col].max(skipna=True)
                    max_float_str_int_len = len(str(max_float).split('.')[0]) # length of int part of max value in column
                    max_float_str_dig_len = len(str(max_float).split('.')[1]) # length of digit part of max value in column
                    '''
                    Three ways to define which dtype will be assigned to column:
                    accuracy: Using DECIMAL(M,D), digit_num is changeable
                    space_save: Calculate how many memory sapce need before assigning decimal 
                                to max value of that column. If it is higher than 4, then will
                                consider using float or double base on integer part length. This mode will 
                                keep all integer part and giving rest space to digit part accroding to mysql 
                                default.
                    all_include: Include all orginal data digit as possible.
                    '''
                    if decimal_type_mode == 'accuracy':
                        self.columns_dtype.update({f'{col}':f'DECIMAL({max_float_str_int_len + digit_num}, {digit_num})'})

                    elif decimal_type_mode == 'space_save':
                        # calculate bytes use if assign data as decimal
                        decimal_bytes_use = decimal_parse_func(max_float, self.dtype_decimal['DECIMAL']['len'],
                                                       self.dtype_decimal['DECIMAL']['bytes'])
                        
                        # use decimal if max value of bytes use if assign deciaml type is less 
                        # than float type or length of integer part is larger than 16 which will
                        # cause uncertainty when assign data type as double
                        if (decimal_bytes_use < 4) or (max_float_str_int_len > 16):
                            self.columns_dtype.update({f'{col}':f'DECIMAL({max_float_str_int_len + max_float_str_dig_len},{max_float_str_dig_len})'})
                        
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
                    max_length = max(self.df[col].str.len())
                    self.columns_dtype.update({f'{col}':f'VARCHAR({max_length})'})

                elif any([True if i in str(dtype_series[col]) else None for i in ['date', 'datetime']]):
                    t_series = self.df[col].dt.time # get time from datetime column
                    x = sum(t_series != datetime.time(0,0,0)) # check if all time is 00:00:00
                    # if all time equal to 00:00:00, use DATE rather than DATETIME/TIMESTAMP
                    if x > 0:
                        self.columns_dtype.update({f'{col}':'TIMESTAMP'})
                        # change dtype timestamp to str for later inserting into mysql databases
                        self.df[col] = self.df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        self.columns_dtype.update({f'{col}':'DATE'})
                        # change dtype timestamp to str for later inserting into mysql databases
                        self.df[col] = self.df[col].dt.strftime('%Y-%m-%d')

            return self
        except:
            raise

    def mysql_create_table_syntax(self, table_name: str, unique_key: bool = False,
                                  unique_col: str or list = None) -> 'pandas_dataframe_parse_mysql_tool':
        # check if table exist
        self.table_name = table_name
        self.creat_table_syntax = f'CREATE TABLE {self.table_name}( '

        for i, key in enumerate(self.columns_dtype.keys()):
            try: 
                if unique_key == True:

                    if (unique_col == key or key in unique_col):

                        if i < len(self.columns_dtype.keys()) - 1:
                            self.creat_table_syntax = self.creat_table_syntax + f'{key} {self.columns_dtype[key]} UNIQUE, '

                        else:
                            self.creat_table_syntax = self.creat_table_syntax + f'{key} {self.columns_dtype[key]} UNIQUE)'
                
                    elif (unique_col == None):
                        raise KeyError
                    
                    else:

                        if i < len(self.columns_dtype.keys()) - 1:
                            self.creat_table_syntax = self.creat_table_syntax + f'{key} {self.columns_dtype[key]}, '

                        else:
                            self.creat_table_syntax = self.creat_table_syntax + f'{key} {self.columns_dtype[key]})'
                    
                else:

                    if i < len(self.columns_dtype.keys()) - 1:
                        self.creat_table_syntax = self.creat_table_syntax + f'{key} {self.columns_dtype[key]}, '

                    else:
                        self.creat_table_syntax = self.creat_table_syntax + f'{key} {self.columns_dtype[key]})'

            except KeyError as e:
                print('Please assign a column or a list of columns.')
                break

        return self

    def mysql_create_db_table(self, db_name: str):
        # check if database exist
        try:
            cursor_db_table = self.engine.cursor(buffered=True)
            # if syntax is not completed, raise error
            if self.creat_table_syntax == f'CREATE TABLE {self.table_name}( ':
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
                cursor_db_table.execute(f'{self.creat_table_syntax}')
                print(f'Database {db_name} and table {self.table_name} are created.')

            # if database exist but table not exist
            elif 'Unknown table' in str(e):
                cursor_db_table.execute(f'{self.creat_table_syntax}')
                print(f'Table {self.table_name} is created.')

            # if database exist but table not exist
            elif f'Table \'{db_name}.{self.table_name}\' doesn\'t exist' in str(e):
                cursor_db_table.execute(f'{self.creat_table_syntax}')
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

        # commit
        finally:
            self.engine.commit()
            cursor_insert.close()
        
        return self



  
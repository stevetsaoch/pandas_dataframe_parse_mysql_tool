Pandas Dataframe 寫入 mysql 工具  
![](https://img.shields.io/badge/python-3.8-blue) ![](https://img.shields.io/badge/MySQL-8.0.25--0%20LTS-orange) ![](https://img.shields.io/badge/Ubuntu-20.04.2%20LTS-orange)
===============================
# 簡介
此專案主要目標是將Pandas的Dataframe中的資料解析過後並以儲存空間最佳的方式建立MySQL語句，且支援數據批量插入。目前支援解析格式如下表：  
|Pandas dtype|MySQL dtype|
|:-----------|:----------|
|Integer|TINYINT, SMALLINT, MEDIUMINT, INT or BIGINT|
|Approximate type|FLOAT or DOUBLE|
|Object|VARCHAR|
|Timestamp|DATE, TIMESTAMP|

# Installation - 安裝
## Requirement - 開發平台與套件需求

* Windows WSL2 (Ubuntu 20.04.2 LTS)
* Mysql 8.0.25-0 for Linux
* Python 3.8.10
  * datetime 
  * pandas 1.2.2
  * mysql-connector 2.2.9

## Process - 安裝流程
* 在Window 10上安裝wsl，並將其轉換為wsl2: https://docs.microsoft.com/zh-tw/windows/wsl/install-win10
* 安裝mysql-server：
````
# Upgrade the Repositories
sudo apt update 
sudo apt upgrade

# Install MySQL
sudo apt install mysql-server

# Start service
sudo service mysql start

# Check all user and their authentication 
SELECT user, authentication_string, plugin, host FROM mysql.user

# Alter authentication and set password for root
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password'

# re-login to mysql-server with password 'password'
mysql -u username -p
````
* 所需套件
  * pandas 1.2.2
  * mysql-connector 2.2.9
  * numpy 1.19.5


# Main Functions - 主要功能

### **class pandas_dataframe_parse_mysql_tool(df: DataFrame, engine: 'mysql.connector', unique_col: str = None)**  

**此類主要目的是將pandas dataframe寫入mysql server，便於資料管理。**

|Parameters: |**df: DataFrame**|
|------------|-----------------|
||要解析的表格|
||**engine: 'mysql.connector'**|
||mysql資料庫連接器|
||**unique_col: str = None**|
||指定表格中哪一個欄位為唯一鍵，若沒有指定則會自動生成**unique_key**作為唯一鍵。|
  
========================================================  

**此類包含四個主要步驟:** 
* dtype_parse(data: Union[DataFrame, Series, np.ndarray]=None,  
              data_col_name: Union[str, list] = None,  
              decimal_type_mode = 'space_save', digit_num = 2):  
解析資料，並將資料解析成果紀錄成字典，只適用於此類與其子類實例。  

|Parameters:|**data: Union[DataFrame, Series, np.ndarray]=None**|
|:---------|:------------------|
||輸入資料，可以是pandas Dataframe, Series或是一維numpy array。預設為None。<br>若為None則會解析呼叫類輸入的表格，其他輸入格式用作子類插入新欄位所需。|
||**data_col_name: Union[str, list] = None**|
||當輸入為Series或是一維numpy array時，指定其表格名稱。|
| |**decimal_type_mode: str, Default = 'space_save'**|
|          |解析小數使用的模式，預設為儲存最佳化模式(space_save)<br><br>__**accuracy**__: 精準模式，使用DECIMAL並可以指定小數點後位數，預設位數為2 <br>__**space_save**__: 儲存最佳化模式，會以該行最大數為基準，分析其若為DECIMAL時所需空間大小，若大於FLOAT所需大小會選擇使用FLOAT，其他則根據整數長度決定用FLOAT或是DOUBLE<br>__**all_include**__: 保存原始數據所有位數，可能會消耗大量空間。<br><br>*在**space_save**模式下，若整數長度超過DOUBLE有效位數(16)時會強制使用DECIMAL，並設定為小數個數為**2**。*|
|          |**digit_num: int, Default = 2**|
|          |小數個數，作用於精準模式，詳見上述個模式解釋。|  
  
* mysql_create_table_syntax(table_name):  
根據dtype_parse結果，生成創建mysql table之語句。 

|Parameters: |**table_name：str**|
|:---------|:------------------|
|          |  表格名稱|


* mysql_create_db_table: 創建新資料庫並建立新table。

|Parameters: |**db_name：str**|
|:---------  |:---------------|
|          |資料庫名稱，會檢查資料庫是否已經存在，若無則以此名稱創建資料庫，並創建table。|

* insert_data_multi: 將padas dataframe導入mysql table。  
  
  
=======================================================
### **class insert_column_tool(pandas_dataframe_parse_mysql_tool):**  
  
**此新增類為pandas_dataframe_parse_mysql_tool的子類，繼承其解析資料格式的功能並轉換成新增欄位的資訊。**  

=======================================================  
**此類包含一個方法:**  
* insert_new_col(self, engine: 'mysql.connector', db_name: str, table_name: str,  
                      data: Union[DataFrame, Series, np.ndarray] = None,  
                      data_col_name: Union[str, list] = None,  
                      decimal_type_mode: str = 'space_save', digit_num: int = 2,  
                      decimal_parse_func: Callable = pandas_dataframe_parse_mysql_tool.bytes_of_decimal):  
此方法繼承父類dtype_parse方法，並需要輸入資料庫名稱、表格名稱等參數以利插入新欄位並更新數據，參數描述請參照父類描述。


# Example : 詳見example.py

# Latest Updated - 最近更新
* 2021.08.08: 使用者能主動選擇使用Decimal或是由系統決定是否要根據資料位數長度選用Decimal。
* 2021.08.12: 新增insert_column_tool類，可以就已存在之table擴充新欄位，支援1d nparray, pd.Dataframe 與 pd.Series輸入。

# Future Plans - 更新計畫
* ~~新增Decimal選項，使用者能主動選擇使用Decimal或是由系統決定是否要根據資料位數長度選用Decimal。~~ **2021.08.08更新完成**
* 解析純日期格式(date)。
* 支援TEXT相關格式。
* 單筆、多筆資料更新。
* 小數位數過多處理
* 改變save_space 分析方式，用中間值代替極大值，以減少極大值影響。
* 資料庫正規化。
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
* 安裝所需套件
  * pandas 1.2.2
  * mysql-connector 2.2.9


# Main Functions - 主要功能
````
class pandas_dataframe_parse_mysql_tool()
````

此工具主要分成四個部分: 
* dtype_parse(decimal_type_mode = 'space_save', digit_num = 2): 解析資料，並將資料解析成果紀錄成字典，只適用於此類實例。

|Parameters: |**decimal_type_mode: str, Default = 'space_save'**|
|:---------|:------------------|
|          |解析小數使用的模式，預設為儲存最佳化模式(space_save)<br><br>__**accuracy**__: 精準模式，使用DECIMAL並可以指定小數點後位數，預設位數為2 <br>__**space_save**__: 儲存最佳化模式，會以該行最大數為基準，分析其若為DECIMAL時所需空間大小，若大於FLOAT所需大小會選擇使用FLOAT，其他則根據整數長度決定用FLOAT或是DOUBLE<br>__**all_include**__: 保存原始數據所有位數，可能會消耗大量空間。<br><br>*整數長度超過DOUBLE有效位數(16)時會強制使用DECIMAL，並以 **digit_num**為小數個數*|
|          |**digit_num: int, Default = 2**|
|          |小數個數，作用於精準模式，詳見上述個模式解釋。|

* mysql_create_table_syntax(table_name, unique_key = False, unique_col = None):  
根據dtype_parse結果，生成創建mysql table之語句。  

|Parameters: |**table_name：str**|
|:---------|:------------------|
|          |  表格名稱|
|          |**unique_key：bool, default=False**|
|          |  **False**: 沒有欄位會被指定為UNIQUE KEY<br>**True**: 將unique_col中str/list皆指定為UNIQUE KEY|
|**Attributes:**|**creat_table_syntax：str**|
|            |  創建Table的MySQL syntax。|

* mysql_create_db_table: 創建新資料庫並建立新table。

|Parameters: |**db_name：str**|
|:---------  |:---------------|
|          |資料庫名稱，會檢查資料庫是否已經存在，若無則以此名稱創建資料庫，並創建table。|

* insert_data_multi: 將padas dataframe導入mysql table。

# Example : 詳見example.py

# Future Plans - 更新計畫
* ~~新增Decimal選項，使用者能主動選擇使用Decimal或是由系統決定是否要根據資料大小用Decimal。~~ **2021.08.08更新完成**
* 解析純日期格式(date)。
* 支援TEXT相關格式。
* 單筆、多筆資料更新。
* 小數位數過多處理
* 改變save_space 分析方式，用中間值代替極大值，以減少極大值影響。
* 資料庫正規化。
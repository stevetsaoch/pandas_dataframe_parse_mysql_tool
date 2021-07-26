Pandas Dataframe 寫入 mysql 工具  
![](https://img.shields.io/badge/python-3.8-blue) ![](https://img.shields.io/badge/MySQL-8.0.25--0%20LTS-orange) ![](https://img.shields.io/badge/Ubuntu-20.04.2%20LTS-orange)
===============================

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
# Alter authentication and set password for root, this procedure will prevent someone 
# login to your mysql service if he has permissions of your root.
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password'
# re-login to mysql-server with password 'password'
mysql -u username -p
````




# 主要功能

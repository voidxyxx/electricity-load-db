import pymysql
import pickle

'''
需要以下准备：
安装python MySQLdb
创建数据库TESTDB（或修改代码同名变量）
在TESTDB数据库中创建表DEVMENT（或修改代码同名变量）
DEVNAME表中字段包括：reading_time, 1~28(sn数量), iednum
连接数据库的用户名为testuser，密码test123（或修改同名变量）
'''

local_db = pymysql.connect(host="localhost",
                           user='testuser',
                           passwd="test123",
                           db="TESTDB")

local_cursor = local_db.cursor()

sql = "create database testdb character set utf8 if not exists db_name;"
local_cursor.execute(sql)
local_cursor.execute("use testdb")

##################
# 获取设备名，这种方式是沿袭旧版本，先下载再读取
try:
    with open("Entername_Enterid_RealDevname_Devname.pickle", 'rb') as f:
        a = pickle.load(f)
except FileNotFoundError as e:
    print("未记录Entername_Enterid_RealDevname_Devname，运行fetch_manager.py")
    print(e)

sn_range = 46

for i in range(len(a)):
    Devname = a[i, 3]
    sql = "create table %s(reading_time datetime()" % (Devname)
    sql += ", iednum str"
    for sn in range(sn_range):
        sql += ", %d float" % (sn + 1)
    sql += ", primary key (reading_time))character set utf8;"

    local_cursor.execute(sql)

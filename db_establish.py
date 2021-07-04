import pymysql
from date import date_list  # 如需修改时间范围，需要人工修改date_list
import pickle

remote_db = pymysql.connect(host="119.3.38.243",
                            port=3306,
                            user="root",
                            passwd="Admin123!@#",
                            db="energy_dsm_db")

remote_cursor = remote_db.cursor()

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

##################
# 获取设备名，这种方式是沿袭旧版本，先下载再读取
try:
    with open("Entername_Enterid_RealDevname_Devname.pickle", 'rb') as f:
        a = pickle.load(f)
except FileNotFoundError as e:
    print("未记录Entername_Enterid_RealDevname_Devname，运行fetch_manager.py")
    print(e)

for d in date_list:
    remote_sql = "SELECT * FROM dsm_data_grp_" + d
    remote_cursor.execute(remote_sql)
    for row in remote_cursor.fetchall():
        try:
            local_sql = "update " + row[5] + " set "
            local_sql += str(row[7]) + "=" + str(row[8])
            local_sql += " iednum=" + str(row[6])
            local_sql += " where reading_time=" + str(row[3])
            local_cursor.execute(local_sql)
            local_db.commit()
        except:
            local_db.rollback()
            local_sql = "insert into " + row[5] + "(reading_time, iednum, " + str(row[7]) + ") " + \
                        "values (" + str(row[3]) + ", " + str(row[6]) + ", " + str(row[8]) + ")"
            print("2: ", local_sql)
            local_sql = "insert into %s(reading_time, iednum, %s) values (str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'), %s, %.2f)" %\
                        (row[5], str(row[7]), row[3].strftime("%Y-%m-%d %H:%M:%S"), str(row[6]), row[8])
            print("3: ", local_sql)
            local_cursor.execute(local_sql)
            local_db.commit()
        print("***")
        exit(0)


# 以下是直接获取设备名，记不太清数据库结构了，用备注放上来，和上面那种二选一即可
# 相应的后续代码也要做一定调整
'''
manager = "SELECT * FROM dsm_device"

cursor.execute(manager)

manager_list = []

for row in cursor.fetchall():
    # 以下4项酌情选用，实际只用到Devname
    # 可添加Entername，这里没有加上，有需求可以再弄
    data_piece = {"RealDevname": row[2],
                  "Devname": row[10],
                  "Enterid": row[3],
                  "Iednum": row[11]}
    manager_list.append(data_piece)
'''
##################


import os
import pymysql
import pickle
from date import date_list

db = pymysql.connect(
    host="119.3.38.243",
    port=3306, user="root",
    passwd="Admin123!@#",
    db="energy_dsm_db")

cursor = db.cursor()

try:
    with open("enterprise.pickle", 'rb') as f:
        a = pickle.load(f)
except FileNotFoundError as e:
    print("未获取企业数据dsm_enterprise，运行fetch_enterprise.py")
    print(e)

if os.path.exists("./data_by_date/") is not True:
    os.mkdir("./data_by_date")

for d in date_list:
    sql = "SELECT * FROM dsm_data_grp_" + d

    cursor.execute(sql)

    data_list = []

    for row in cursor.fetchall():
        data_list.append(row)

    with open("./data_by_date/"+d+".pickle", 'wb') as f:
        pickle.dump(data_list, f)

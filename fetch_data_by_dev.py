import os
import pymysql
import pickle
import numpy as np
from date import date_list

sn_list = [4, 5, 6,  # 线电压
           7, 8, 9,  # 相电流
           14,       # 总有功功率
           18,       # 总无功功率
           26,       # 总功率因数
           28,       # 正有功电能
           30        # 正无功电能
           ]

db = pymysql.connect(
    host="119.3.38.243",
    port=3306,
    user="root",
    passwd="Admin123!@#",
    db="energy_dsm_db"
)

cursor = db.cursor()

try:
    with open("Entername_Enterid_RealDevname_Devname.pickle", 'rb') as f:
        a = pickle.load(f)
except FileNotFoundError as e:
    print("未记录Entername_Enterid_RealDevname_Devname，运行fetch_manager.py")
    print(e)

for i in range(len(a)):

    Entername = a[i, 0]
    Enterid = a[i, 1]
    RealDevname = a[i, 2]
    Devname = a[i, 3]

    data_list = []

    for sn in sn_list:
        data_sn_list = []
        for d in date_list:
            sql = "SELECT * FROM dsm_data_grp_" + d
            print(Devname)
            sql = sql + ' WHERE devname="' + Devname + '"' + ' AND sn="' + str(sn) + '"'
            sql = sql + 'ORDER BY reading_time asc'
            cursor.execute(sql)

            for row in cursor.fetchall():
                data_sn_list.append(row)
                print(row)
            exit(0)
        data_list.append(data_sn_list)
    data_list = np.array(data_list)
    with open("./data_by_enterprise/" + Entername + "/" + Devname + ".pickle", 'wb') as f:
        pickle.dump(data_list, f)
    print(data_list)
    print(Entername)
    print(Devname)
    exit(0)
import pymysql
import pickle
import numpy as np
import os

db = pymysql.connect(
    host="119.3.38.243",
    port=3306, user="root",
    passwd="Admin123!@#",
    db="energy_dsm_db")

cursor = db.cursor()

enterprise = "SELECT * FROM dsm_enterprise"

cursor.execute(enterprise)

enterprise_list = []

for row in cursor.fetchall():
    enterprise_list.append(row)

enterprise_list = np.array(enterprise_list)

with open("enterprise.pickle", 'wb') as f:
    pickle.dump(enterprise_list, f)

with open("Name_Enterid.pickle", 'wb') as f:
    pickle.dump(enterprise_list[:, [1, 0]], f)

if os.path.exists("./data_by_enterprise/") is not True:
    os.mkdir("./data_by_enterprise/")

for name in enterprise_list[:, 1]:
    if os.path.exists("./data_by_enterprise/" + name + '/') is not True:
        os.mkdir("./data_by_enterprise/" + name + '/')

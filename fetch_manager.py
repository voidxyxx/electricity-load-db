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

manager = "SELECT * FROM dsm_manager"

cursor.execute(manager)

manager_list = []

for row in cursor.fetchall():
    manager_list.append(row)

manager_list = np.array(manager_list)

with open("manager.pickle", 'wb') as f:
    pickle.dump(manager_list, f)

with open("RealDevname_Devname_Enterid.pickle", 'wb') as f:
    pickle.dump(manager_list[:, [2, 7, 3]], f)

try:
    with open("Name_Enterid.pickle", 'rb') as f:
        name_enterid = pickle.load(f)
except FileNotFoundError as e:
    print("未记录Name_Enterid，运行fetch_enterprise.py")
    print(e)

Entername_Enterid_RealDevname_Devname_list = []

for i in range(manager_list.shape[0]):
    RealDevname = manager_list[i, 2]
    Devname = manager_list[i, 7]
    Enterid = manager_list[i, 3]
    Entername = name_enterid[np.where(name_enterid[:, 1] == Enterid), 0]
    if os.path.exists("./data_by_enterprise/" + Entername[0][0] + "/" + RealDevname) is not True:
        os.mkdir("./data_by_enterprise/" + Entername[0][0] + "/" + RealDevname)
    Entername_Enterid_RealDevname_Devname_list.append([Entername[0][0], Enterid, RealDevname, Devname])

final_list = np.array(Entername_Enterid_RealDevname_Devname_list)

with open("Entername_Enterid_RealDevname_Devname.pickle",'wb') as f:
    pickle.dump(final_list, f)

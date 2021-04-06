import pickle
import pandas as pd
import math
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# from date import date_list
date_list = ['20200901']
start_time = time.asctime(time.localtime(time.time()))

sn_list = [1, 2, 3,  # 相电压
           4, 5, 6,  # 线电压
           7, 8, 9,  # 相电流
           11, 12, 13,  # 各相有功功率
           14,  # 总有功功率
           18,  # 总无功功率
           23, 24, 25,  # 各相功率因数
           26,  # 总功率因数
           28,  # 正有功电能
           30  # 正无功电能
           ]
columns = ['id', 'type', 'cot', 'reading_time', 'create_time', 'devname', 'iednum', 'sn', 'value', 'qos']
new_columns = ['index_datetime', '年', '月', '日', '时刻', '总有功功率', 'A相电压', 'B相电压', 'C相电压', 'AB线电压', 'BC线电压', 'CA线电压',
               'A相电流', 'B相电流', 'C相电流', 'A相有功功率', 'B相有功功率', 'C相有功功率', 'A相功率因数', 'B相功率因数', 'C相功率因数', '总无功功率', '频率', '功率因数', '总正向有功电能']

interval = 30  # seconds per row

def grab_data_one(name_group):
    name = name_group[0]
    group = name_group[1]
    data_exist = False
    for sn in sn_list:
        data_exist = data_exist or (sn in group['sn'].values)
        if data_exist:
            break
    if data_exist is False:
        return 0, None

    data_one = {'index_datetime': name, '年': name.year, '月': name.month, '日': name.day,
                '时刻': name.strftime('%H:%M:%S')}
    try:
        data_one['A相电压'] = group.loc[group['sn'] == 1, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['B相电压'] = group.loc[group['sn'] == 2, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['C相电压'] = group.loc[group['sn'] == 1, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['AB线电压'] = group.loc[group['sn'] == 4, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['BC线电压'] = group.loc[group['sn'] == 5, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['CA线电压'] = group.loc[group['sn'] == 6, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['A相电流'] = group.loc[group['sn'] == 7, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['B相电流'] = group.loc[group['sn'] == 8, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['C相电流'] = group.loc[group['sn'] == 9, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['总有功功率'] = group.loc[group['sn'] == 14, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['总无功功率'] = group.loc[group['sn'] == 18, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['频率'] = group.loc[group['sn'] == 27, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['功率因数'] = group.loc[group['sn'] == 26, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['总正向有功电能'] = group.loc[group['sn'] == 28, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['A相有功功率'] = group.loc[group['sn'] == 11, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['B相有功功率'] = group.loc[group['sn'] == 12, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['C相有功功率'] = group.loc[group['sn'] == 13, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['A相功率因数'] = group.loc[group['sn'] == 23, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['B相功率因数'] = group.loc[group['sn'] == 24, 'value'].values[0]
    except IndexError:
        pass
    try:
        data_one['C相功率因数'] = group.loc[group['sn'] == 25, 'value'].values[0]
    except IndexError:
        pass

    return 1, data_one

try:
    with open("Entername_Enterid_RealDevname_Devname.pickle", 'rb') as f:
        a = pickle.load(f)
except FileNotFoundError as e:
    print("未记录Entername_Enterid_RealDevname_Devname，运行fetch_manager.py")
    print(e)

integrity = pd.DataFrame(columns=['企业名称', '设备名称', '起始时间', '终止时间', '总数据量', '总采集数据量', '总有功功率', 'A相电压', 'B相电压',
                                  'C相电压', 'AB线电压', 'BC线电压', 'CA线电压', 'A相电流', 'B相电流', 'C相电流', 'A相有功功率', 'B相有功功率', 'C相有功功率',
                                  'A相功率因数', 'B相功率因数', 'C相功率因数', '总无功功率', '频率', '功率因数', '总正向有功电能'])

# iter by dev, considering the unaffordable cost of memory
for i in range(len(a)):
    Entername = a[i, 0]
    Enterid = a[i, 1]
    RealDevname = a[i, 2]
    Devname = a[i, 3]
    Iednum = a[i, 4]
    data_output_by_dev = pd.DataFrame(columns=new_columns)
    stop = 0

    for d in date_list:
        with open("./data_by_date/" + d + ".pickle", 'rb') as f:
            data = pickle.load(f)
            data = pd.DataFrame(data, columns=columns)
        data_dev = data.groupby(['devname', 'iednum'])
        stop = 0

        try:
            data_selected_by_dev = data_dev.get_group((Devname, int(Iednum)))
        except KeyError as e:
            continue

        data_selected_by_dev_by_time = data_selected_by_dev.groupby('reading_time')

        '''pool = ThreadPoolExecutor()
        threads = []
        for name_group in data_selected_by_dev_by_time:
            threads.append(pool.submit(grab_data_one, name_group))
        for t in threads:
            stop_result, data_one = t.result()
            if stop_result == 1:
                stop = 1
                if data_one is not None:
                    data_output_by_dev = data_output_by_dev.append([data_one], ignore_index=True)
        pool.shutdown()'''

        with ThreadPoolExecutor(max_workers=50) as executor:
            results = executor.map(grab_data_one, (data_selected_by_dev_by_time))

            for stop_result, data_one in results:
                if stop_result == 1:
                    stop = 1
                    if data_one is not None:
                        data_output_by_dev = data_output_by_dev.append([data_one], ignore_index=True)

    if stop == 1:
        print(Entername)
        print(RealDevname)

        data_output_by_dev = data_output_by_dev.sort_values(by='index_datetime')  # earlier to later
        data_counts = data_output_by_dev.shape[0]
        data_total = math.floor((data_output_by_dev['index_datetime'][data_counts - 1] - data_output_by_dev['index_datetime'][
            0]).total_seconds() / interval)

        data_notna = (data_counts - data_output_by_dev.isna().sum()) / data_total

        integ = {'企业名称': Entername, '设备名称': RealDevname, '起始时间': data_output_by_dev['index_datetime'][0],
                 '终止时间': data_output_by_dev['index_datetime'][data_counts - 1], '总数据量': data_total, '总采集数据量': data_counts,
                 'A相电压': data_notna['A相电压'],
                 'B相电压': data_notna['B相电压'],
                 'C相电压': data_notna['C相电压'],
                 'AB线电压': data_notna['AB线电压'],
                 'BC线电压': data_notna['BC线电压'],
                 'CA线电压': data_notna['CA线电压'],
                 'A相电流': data_notna['A相电流'],
                 'B相电流': data_notna['B相电流'],
                 'C相电流': data_notna['C相电流'],
                 'A相有功功率': data_notna['A相有功功率'],
                 'B相有功功率': data_notna['B相有功功率'],
                 'C相有功功率': data_notna['C相有功功率'],
                 'A相功率因数': data_notna['A相功率因数'],
                 'B相功率因数': data_notna['B相功率因数'],
                 'C相功率因数': data_notna['C相功率因数'],
                 '总有功功率': data_notna['总有功功率'],
                 '总无功功率': data_notna['总无功功率'],
                 '频率': data_notna['频率'],
                 '功率因数': data_notna['功率因数'],
                 '总正向有功电能': data_notna['总正向有功电能']}

        integrity = integrity.append([integ], ignore_index=True)
        data_output_by_dev.to_excel("./data_by_enterprise/" + Entername + "/" + RealDevname + ".xlsx", header=new_columns)
        print("*****")
        if i+1 >= 10:
            break

integrity.to_excel("./data_by_enterprise/integrity.xlsx",
                   header=['企业名称', '设备名称', '起始时间', '终止时间', '总数据量', '总采集数据量', '总有功功率', 'A相电压', 'B相电压',
                                  'C相电压', 'AB线电压', 'BC线电压', 'CA线电压', 'A相电流', 'B相电流', 'C相电流', 'A相有功功率', 'B相有功功率', 'C相有功功率',
                                  'A相功率因数', 'B相功率因数', 'C相功率因数', '总无功功率', '频率', '功率因数', '总正向有功电能'])
end_time = time.asctime(time.localtime(time.time()))
print(start_time)
print(end_time)
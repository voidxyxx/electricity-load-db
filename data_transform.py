import pickle
import pandas as pd
import math

# from date import date_list
date_list = ['20200901']

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

try:
    with open("Entername_Enterid_RealDevname_Devname.pickle", 'rb') as f:
        a = pickle.load(f)
except FileNotFoundError as e:
    print("未记录Entername_Enterid_RealDevname_Devname，运行fetch_manager.py")
    print(e)

integrity = pd.DataFrame(columns=['企业名称', '设备名称', '起始时间', '终止时间', '总数据量', '总采集数据量', '总有功功率', 'A相电压', 'B相电压',
                                  'C相电压', 'AB线电压', 'BC线电压', 'CA线电压', 'A相电流', 'B相电流', 'C相电流', 'A相有功功率', 'B相有功功率', 'C相有功功率',
                                  'A相功率因数', 'B相功率因数', 'C相功率因数', '总无功功率', '频率', '功率因数', '总正向有功电能'])

# for i in range(len(a)):
#
#     Entername = a[i, 0]
#     Enterid = a[i, 1]
#     RealDevname = a[i, 2]
#     Devname = a[i, 3]
#
#     data_list = []
#
#     for d in date_list:
#         with open("./data_by_date/" + d + ".pickle", 'rb') as f:
#             data = pickle.load(f)
#             data = pd.DataFrame(data, columns=columns)
#         data_dev = data.groupby('devname')
#         data_selected = data_dev.get_group(Devname)
#         data_sn_list = []
#         for sn in sn_list:
#
#             data_selected = data[np.where((data[:, sn_index] == sn) & (data[:, dev_index] == Devname)), :]
#             data_sn_list.append(data_selected)
#         print(sn)
#         data_list.append(data_sn_list)
#     data_list = np.array(data_list)
#     with open("./data_by_enterprise/" + Entername + "/" + Devname + ".pickle", 'wb') as f:
#         pickle.dump(data_list, f)
#         print(Entername)

# iter by dev, considering the unaffordable cost of memory
for i in range(len(a)):
    Entername = a[i, 0]
    Enterid = a[i, 1]
    RealDevname = a[i, 2]
    Devname = a[i, 3]
    data_output_by_dev = pd.DataFrame(columns=new_columns)
    stop = 0

    for d in date_list:
        with open("./data_by_date/" + d + ".pickle", 'rb') as f:
            data = pickle.load(f)
            data = pd.DataFrame(data, columns=columns)
        data_dev = data.groupby('devname')
        stop = 0

        try:
            data_selected_by_dev = data_dev.get_group(Devname)
        except KeyError as e:
            continue

        data_selected_by_dev_by_time = data_selected_by_dev.groupby('reading_time')

        for name, group in data_selected_by_dev_by_time:
            data_length = len(group.loc[group['sn'] == 1, 'value']) + len(group.loc[group['sn'] == 2, 'value']) + \
                          len(group.loc[group['sn'] == 3, 'value']) + len(group.loc[group['sn'] == 4, 'value']) + \
                          len(group.loc[group['sn'] == 5, 'value']) + len(group.loc[group['sn'] == 6, 'value']) + \
                          len(group.loc[group['sn'] == 7, 'value']) + len(group.loc[group['sn'] == 8, 'value']) + \
                          len(group.loc[group['sn'] == 9, 'value']) + len(group.loc[group['sn'] == 14, 'value']) + \
                          len(group.loc[group['sn'] == 18, 'value']) + len(group.loc[group['sn'] == 27, 'value']) + \
                          len(group.loc[group['sn'] == 26, 'value']) + len(group.loc[group['sn'] == 28, 'value']) + \
                          len(group.loc[group['sn'] == 11, 'value']) + len(group.loc[group['sn'] == 12, 'value']) + \
                          len(group.loc[group['sn'] == 13, 'value']) + len(group.loc[group['sn'] == 23, 'value']) + \
                          len(group.loc[group['sn'] == 24, 'value']) + len(group.loc[group['sn'] == 25, 'value'])
            if data_length == 0:
                continue
            stop = 1

            data_one = {'index_datetime': name, '年': name.year, '月': name.month, '日': name.day,
                        '时刻': name.strftime('%H:%M:%S')}
            # data_one['A相电压'] = 'NAN' if len(group.loc[group['sn'] == 1, 'value'])==0 else group.loc[group['sn'] == 1, 'value'].values[0]
            # data_one['B相电压'] = 'NaN' if len(group.loc[group['sn'] == 2, 'value'])==0 else group.loc[group['sn'] == 2, 'value'].values[0]
            # data_one['C相电压'] = 'NaN' if len(group.loc[group['sn'] == 3, 'value'])==0 else group.loc[group['sn'] == 3, 'value'].values[0]
            # data_one['AB线电压'] = 'NAN' if len(group.loc[group['sn'] == 4, 'value'])==0 else group.loc[group['sn'] == 4, 'value'].values[0]
            # data_one['BC线电压'] = 'NAN' if len(group.loc[group['sn'] == 5, 'value'])==0 else group.loc[group['sn'] == 5, 'value'].values[0]
            # data_one['CA线电压'] = 'NAN' if len(group.loc[group['sn'] == 6, 'value'])==0 else group.loc[group['sn'] == 6, 'value'].values[0]
            # data_one['A相电流'] = 'NAN' if len(group.loc[group['sn'] == 7, 'value'])==0 else group.loc[group['sn'] == 7, 'value'].values[0]
            # data_one['B相电流'] = 'NAN' if len(group.loc[group['sn'] == 8, 'value'])==0 else group.loc[group['sn'] == 8, 'value'].values[0]
            # data_one['C相电流'] = 'NAN' if len(group.loc[group['sn'] == 9, 'value'])==0 else group.loc[group['sn'] == 9, 'value'].values[0]
            # data_one['总有功功率'] = 'NAN' if len(group.loc[group['sn'] == 14, 'value'])==0 else group.loc[group['sn'] == 14, 'value'].values[0]
            # data_one['总无功功率'] = 'NAN' if len(group.loc[group['sn'] == 18, 'value'])==0 else group.loc[group['sn'] == 18, 'value'].values[0]
            # data_one['频率'] = 'NAN' if len(group.loc[group['sn'] == 27, 'value'])==0 else group.loc[group['sn'] == 27, 'value'].values[0]
            # data_one['功率因数'] = 'NAN' if len(group.loc[group['sn'] == 26, 'value'])==0 else group.loc[group['sn'] == 26, 'value'].values[0]
            # data_one['总正向有功电能'] = 'NAN' if len(group.loc[group['sn'] == 28, 'value'])==0 else group.loc[group['sn'] == 28, 'value'].values[0]
            if len(group.loc[group['sn'] == 1, 'value']) != 0:
                data_one['A相电压'] = group.loc[group['sn'] == 1, 'value'].values[0]
            if len(group.loc[group['sn'] == 2, 'value']) != 0:
                data_one['B相电压'] = group.loc[group['sn'] == 2, 'value'].values[0]
            if len(group.loc[group['sn'] == 3, 'value']) != 0:
                data_one['C相电压'] = group.loc[group['sn'] == 3, 'value'].values[0]
            if len(group.loc[group['sn'] == 4, 'value']) != 0:
                data_one['AB线电压'] = group.loc[group['sn'] == 4, 'value'].values[0]
            if len(group.loc[group['sn'] == 5, 'value']) != 0:
                data_one['BC线电压'] = group.loc[group['sn'] == 5, 'value'].values[0]
            if len(group.loc[group['sn'] == 6, 'value']) != 0:
                data_one['CA线电压'] = group.loc[group['sn'] == 6, 'value'].values[0]
            if len(group.loc[group['sn'] == 7, 'value']) != 0:
                data_one['A相电流'] = group.loc[group['sn'] == 7, 'value'].values[0]
            if len(group.loc[group['sn'] == 8, 'value']) != 0:
                data_one['B相电流'] = group.loc[group['sn'] == 8, 'value'].values[0]
            if len(group.loc[group['sn'] == 9, 'value']) != 0:
                data_one['C相电流'] = group.loc[group['sn'] == 9, 'value'].values[0]
            if len(group.loc[group['sn'] == 14, 'value']) != 0:
                data_one['总有功功率'] = group.loc[group['sn'] == 14, 'value'].values[0]
            if len(group.loc[group['sn'] == 18, 'value']) != 0:
                data_one['总无功功率'] = group.loc[group['sn'] == 18, 'value'].values[0]
            if len(group.loc[group['sn'] == 27, 'value']) != 0:
                data_one['频率'] = group.loc[group['sn'] == 27, 'value'].values[0]
            if len(group.loc[group['sn'] == 26, 'value']) != 0:
                data_one['功率因数'] = group.loc[group['sn'] == 26, 'value'].values[0]
            if len(group.loc[group['sn'] == 28, 'value']) != 0:
                data_one['总正向有功电能'] = group.loc[group['sn'] == 28, 'value'].values[0]
            if len(group.loc[group['sn'] == 11, 'value']) != 0:
                data_one['A相有功功率'] = group.loc[group['sn'] == 11, 'value'].values[0]
            if len(group.loc[group['sn'] == 12, 'value']) != 0:
                data_one['B相有功功率'] = group.loc[group['sn'] == 12, 'value'].values[0]
            if len(group.loc[group['sn'] == 13, 'value']) != 0:
                data_one['C相有功功率'] = group.loc[group['sn'] == 13, 'value'].values[0]
            if len(group.loc[group['sn'] == 23, 'value']) != 0:
                data_one['A相功率因数'] = group.loc[group['sn'] == 23, 'value'].values[0]
            if len(group.loc[group['sn'] == 24, 'value']) != 0:
                data_one['B相功率因数'] = group.loc[group['sn'] == 24, 'value'].values[0]
            if len(group.loc[group['sn'] == 25, 'value']) != 0:
                data_one['C相功率因数'] = group.loc[group['sn'] == 25, 'value'].values[0]

            data_output_by_dev = data_output_by_dev.append([data_one], ignore_index=True)

    if stop == 1:
        print(Entername)
        print(RealDevname)

        data_output_by_dev = data_output_by_dev.sort_values(by='index_datetime')  # earlier to later
        data_counts = data_output_by_dev.shape[0]
        data_total = (data_output_by_dev['index_datetime'][data_counts - 1] - data_output_by_dev['index_datetime'][
            0]).total_seconds() / interval

        integ = {'企业名称': Entername, '设备名称': RealDevname, '起始时间': data_output_by_dev['index_datetime'][0],
                 '终止时间': data_output_by_dev['index_datetime'][data_counts - 1], '总数据量': data_total, '总采集数据量': data_counts,
                 'A相电压': (data_counts - data_output_by_dev['A相电压'].isna().sum()) / math.floor(data_total),
                 'B相电压': (data_counts - data_output_by_dev['B相电压'].isna().sum()) / math.floor(data_total),
                 'C相电压': (data_counts - data_output_by_dev['C相电压'].isna().sum()) / math.floor(data_total),
                 'AB线电压': (data_counts - data_output_by_dev['AB线电压'].isna().sum()) / math.floor(data_total),
                 'BC线电压': (data_counts - data_output_by_dev['BC线电压'].isna().sum()) / math.floor(data_total),
                 'CA线电压': (data_counts - data_output_by_dev['CA线电压'].isna().sum()) / math.floor(data_total),
                 'A相电流': (data_counts - data_output_by_dev['A相电流'].isna().sum()) / math.floor(data_total),
                 'B相电流': (data_counts - data_output_by_dev['B相电流'].isna().sum()) / math.floor(data_total),
                 'C相电流': (data_counts - data_output_by_dev['C相电流'].isna().sum()) / math.floor(data_total),
                 'A相有功功率': (data_counts - data_output_by_dev['A相有功功率'].isna().sum()) / math.floor(data_total),
                 'B相有功功率': (data_counts - data_output_by_dev['B相有功功率'].isna().sum()) / math.floor(data_total),
                 'C相有功功率': (data_counts - data_output_by_dev['C相有功功率'].isna().sum()) / math.floor(data_total),
                 'A相功率因数': (data_counts - data_output_by_dev['A相功率因数'].isna().sum()) / math.floor(data_total),
                 'B相功率因数': (data_counts - data_output_by_dev['B相功率因数'].isna().sum()) / math.floor(data_total),
                 'C相功率因数': (data_counts - data_output_by_dev['C相功率因数'].isna().sum()) / math.floor(data_total),
                 '总有功功率': (data_counts - data_output_by_dev['总有功功率'].isna().sum()) / math.floor(data_total),
                 '总无功功率': (data_counts - data_output_by_dev['总无功功率'].isna().sum()) / math.floor(data_total),
                 '频率': (data_counts - data_output_by_dev['频率'].isna().sum()) / math.floor(data_total),
                 '功率因数': (data_counts - data_output_by_dev['功率因数'].isna().sum()) / math.floor(data_total),
                 '总正向有功电能': (data_counts - data_output_by_dev['总正向有功电能'].isna().sum()) / math.floor(data_total)}

        integrity = integrity.append([integ], ignore_index=True)
        data_output_by_dev.to_excel("./data_by_enterprise/" + Entername + "/" + RealDevname + ".xlsx", header=new_columns)
        break
    # data_selected_by_dev_sn = data_selected_by_dev.groupby('sn')

    # for sn in sn_list:
    #     try:
    #         data_selected_by_dev_selected_by_sn = data_selected_by_dev_sn.get_group(sn)
    #     except KeyError:
    #         continue
    #     year = data_selected_by_dev_selected_by_sn['reading_time'].year
    #     month = data_selected_by_dev_selected_by_sn['reading_time'].month
    #     day = data_selected_by_dev_selected_by_sn['reading_time'].day
    #     moment = data_selected_by_dev_selected_by_sn['reading_time'].strftime('%H:%M:%S')
    #     excel_path = "./data_by_enterprise/" + Entername + "/" + RealDevname + ".xlsx"
    #     if os.path.exists(RealDevname + ".xlsx") is False:
    #         data_selected_by_dev_selected_by_sn.to_excel(excel_path, sheet_name=str(sn), header=columns)
    #     elif str(sn) in pd.ExcelFile(excel_path).sheet_names is False:
    #         data_selected_by_dev_selected_by_sn.to_excel(excel_path, sheet_name=str(sn), header=columns)
    #     else:
    #         writer = pd.ExcelWriter(excel_path)
    #         data_exist = pd.read_excel(excel_path, header=True, sheet_name=str(sn))
    #         data_exist = data_exist.append(data_selected_by_dev_selected_by_sn)
    #         data_exist.to_excel(writer, sheet_name=str(sn), header=columns)
    #         writer.close()
    #     print(sn)
    #     print("***")
    print("*****")

integrity.to_excel("./data_by_enterprise/integrity.xlsx",
                   header=['企业名称', '设备名称', '起始时间', '终止时间', '总数据量', '总采集数据量', '总有功功率', 'A相电压', 'B相电压',
                                  'C相电压', 'AB线电压', 'BC线电压', 'CA线电压', 'A相电流', 'B相电流', 'C相电流', 'A相有功功率', 'B相有功功率', 'C相有功功率',
                                  'A相功率因数', 'B相功率因数', 'C相功率因数', '总无功功率', '频率', '功率因数', '总正向有功电能'])

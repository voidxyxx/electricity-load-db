import pandas as pd
import pickle


columns = ['index_datetime', '年', '月', '日', '时刻', '总有功功率', 'A相电压', 'B相电压', 'C相电压', 'AB线电压',
               'BC线电压', 'CA线电压', 'A相电流', 'B相电流', 'C相电流', 'A相有功功率', 'B相有功功率', 'C相有功功率',
               'A相功率因数', 'B相功率因数', 'C相功率因数', '总无功功率', '频率', '功率因数', '总正向有功电能']
new_columns = ['index', 'index_datetime', '年', '月', '日', '时刻', '总有功功率', 'A相电压', 'B相电压', 'C相电压', 'AB线电压',
               'BC线电压', 'CA线电压', 'A相电流', 'B相电流', 'C相电流', 'A相有功功率', 'B相有功功率', 'C相有功功率',
               'A相功率因数', 'B相功率因数', 'C相功率因数', '总无功功率', '频率', '功率因数', '总正向有功电能', '可用性']
sqrt3 = 1.73205
epsilon = 1e-8


def eta12(measurement, calculation):
    return abs((abs(measurement) - abs(calculation)) / calculation)


def eta1(df):
    return abs((abs(df['总有功功率']) / (((df['A相电压'] * df['A相电流']) + (df['B相电压'] * df['B相电流']) + (df['C相电压'] * df['C相电流'])) * abs(df['功率因数']) + epsilon)) - 1.0)


def eta2(df):
    return abs(((9 * abs(df['总有功功率'])) / abs(sqrt3 * (df['AB线电压'] + df['BC线电压'] + df['CA线电压']) * (df['A相电流'] + df['B相电流'] + df['C相电流']) * abs(df['功率因数']) + epsilon)) - 1)


def etava(df):
    return abs(((3 * df['A相电压']) / (df['A相电压'] + df['B相电压'] + df['C相电压'] + epsilon)) - 1)


def etavb(df):
    return abs(((3 * df['B相电压']) / (df['A相电压'] + df['B相电压'] + df['C相电压'] + epsilon)) - 1)


def etavc(df):
    return abs(((3 * df['C相电压']) / (df['A相电压'] + df['B相电压'] + df['C相电压'] + epsilon)) - 1)


def etauab(df):
    return abs(((3 * df['AB线电压']) / (df['AB线电压'] + df['BC线电压'] + df['CA线电压'] + epsilon)) - 1)


def etaubc(df):
    return abs(((3 * df['BC线电压']) / (df['AB线电压'] + df['BC线电压'] + df['CA线电压'] + epsilon)) - 1)


def etauca(df):
    return abs(((3 * df['CA线电压']) / (df['AB线电压'] + df['BC线电压'] + df['CA线电压'] + epsilon)) - 1)


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
    Iednum = a[i, 4]
    try:
        df = pd.read_excel("./data_by_enterprise/" + Entername + "/" + RealDevname + ".xlsx", names=new_columns[:-1])
        df = df.drop(columns=['index'])
        df['可用性'] = ''
    except Exception as e:
        print(e)
        continue
    dfna = df.isna()
    for j in range(df.shape[0]):
        if dfna.iloc[j]['功率因数']:
            df['可用性'].iloc[j] = 0
            continue
        if dfna.iloc[j]['A相电流'] or dfna.iloc[j]['B相电流'] or dfna.iloc[j]['C相电流']:
            df.iloc[j]['可用性'] = 0
            continue
        if df.iloc[j]['总正向有功电能'] < 0:
            df['可用性'].iloc[j] = 0
            continue
        if dfna.iloc[j]['A相电压'] or dfna.iloc[j]['B相电压'] or dfna.iloc[j]['C相电压']:
            if dfna.iloc[j]['AB线电压'] or dfna.iloc[j]['BC线电压'] or dfna.iloc[j]['CA线电压']:
                df['可用性'].iloc[j] = 0
                continue
            if (etauab(df.iloc[j]) > 0.15) or (etaubc(df.iloc[j]) > 0.15) or (etauca(df.iloc[j]) > 0.15) or (eta2(df.iloc[j]) > 0.15):
                df['可用性'].iloc[j] = 0
                continue
        # if (etava(df.iloc[j]) > 0.15) or (etavb(df.iloc[j]) > 0.15) or (etavc(df.iloc[j]) > 0.15):
        #     df['可用性'].iloc[j] = 0
        #     continue
        if (eta1(df.iloc[j]) > 0.15):
            df['可用性'].iloc[j] = 0
            continue
        df['可用性'].iloc[j] = 1
    df.to_excel("./data_by_enterprise/" + Entername + "/" + RealDevname + "accuracy.xlsx", header=new_columns[1:])

    if sum(df['可用性']):
        print(Entername)
        print(RealDevname)
        break




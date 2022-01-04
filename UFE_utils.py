# Import the necessary modules

import numpy as np
import pandas as pd
from pandas import ExcelWriter
import datetime as dt
import matplotlib.pyplot as plt

def load_sum(inpdf):
    
    ldf = []
    for i in range(1, 25):
        kwh = "I" + format(i, '02d') + "00_KWH"
        ldf.append(pd.pivot_table(inpdf, index = ["READ_DATE"], values=[kwh], aggfunc=[np.sum]))

    ihr = pd.DataFrame()
    for i in range(0, 24):
        ihr = pd.concat([ihr,ldf[i]], axis=1)

    ihr.columns = ihr.columns.map('_'.join)
    
    return ihr

def reg_sum(inp):

    inp_sum = pd.DataFrame()
    inp_sum['READ_DATE'] = inp['READ_DATE']
    inp_sum['Metered_Load'] = inp['sum_I0100_KWH'] + inp['sum_I0200_KWH'] + inp['sum_I0300_KWH'] + \
    inp['sum_I0400_KWH'] + inp['sum_I0500_KWH'] + inp['sum_I0600_KWH'] + inp['sum_I0700_KWH'] + \
    inp['sum_I0800_KWH'] + inp['sum_I0900_KWH'] + inp['sum_I1000_KWH'] + inp['sum_I1100_KWH'] + \
    inp['sum_I1200_KWH'] + inp['sum_I1300_KWH'] + inp['sum_I1400_KWH'] + inp['sum_I1500_KWH'] + \
    inp['sum_I1600_KWH'] + inp['sum_I1700_KWH'] + inp['sum_I1800_KWH'] + inp['sum_I1900_KWH'] + \
    inp['sum_I2000_KWH'] + inp['sum_I2100_KWH'] + inp['sum_I2200_KWH'] + inp['sum_I2300_KWH'] + \
    inp['sum_I2400_KWH']
    
    inp_sum['Day'] = pd.to_datetime(inp_sum['READ_DATE'], format="%Y-%m-%d").apply(lambda x: dt.datetime.strftime(x, '%a'))
    ltemp = inp_sum.drop(inp_sum[pd.to_datetime(inp_sum['READ_DATE'], format='%Y-%m-%d') < dt.date(2017,2,3)].index)

    inp_day = pd.pivot_table(ltemp, index=["Day"], values=["Metered_Load"], aggfunc=[np.mean]).reset_index()
    inp_sum = inp_sum.merge(inp_day, how="left", left_on="Day", right_on="Day")
    inp_sum.columns=['READ_DATE', 'Metered_Load', 'Day', 'Day_Average']
    inp_sum['READ_DATE'] = pd.to_datetime(inp_sum['READ_DATE'], format='%Y-%m-%d')
    inp_sum['REG_Diff'] = (inp_sum['Metered_Load'] - inp_sum['Day_Average']) * 100 / inp_sum['Day_Average']
    
    return inp_sum

def idproc(inp):
    
    retdf = inp
    retdf['Total_Load'] = retdf['I0100_KWH'] + retdf['I0200_KWH'] + retdf['I0300_KWH'] + retdf['I0400_KWH'] + \
    retdf['I0500_KWH'] + retdf['I0600_KWH'] + retdf['I0700_KWH'] + retdf['I0800_KWH'] + retdf['I0900_KWH'] + \
    retdf['I1000_KWH'] + retdf['I1100_KWH'] + retdf['I1200_KWH'] + retdf['I1300_KWH'] + \
    retdf['I1400_KWH'] + retdf['I1500_KWH'] + retdf['I1600_KWH'] + retdf['I1700_KWH'] + \
    retdf['I1800_KWH'] + retdf['I1900_KWH'] + retdf['I2000_KWH'] + retdf['I2100_KWH'] + \
    retdf['I2200_KWH'] + retdf['I2300_KWH'] + retdf['I2400_KWH'] 

    retdf['Day'] = pd.to_datetime(retdf['READ_DATE'], format="%Y-%m-%d").apply(lambda x: dt.datetime.strftime(x, '%a'))
    temp = retdf.drop(retdf[pd.to_datetime(retdf['READ_DATE'], format='%Y-%m-%d') < dt.date(2017,2,3)].index)

    ret_avg = pd.pivot_table(temp, index=["ID","Day"], values=["Total_Load", "I0100_KWH", "I0200_KWH", \
          "I0300_KWH", "I0400_KWH","I0500_KWH", "I0600_KWH", "I0700_KWH", "I0800_KWH", "I0900_KWH", "I1000_KWH", \
          "I1100_KWH", "I1200_KWH", "I1300_KWH", "I1400_KWH", "I1500_KWH", "I1600_KWH", "I1700_KWH", "I1800_KWH", \
          "I1900_KWH", "I2000_KWH", "I2100_KWH", "I2200_KWH", "I2300_KWH", "I2400_KWH",], aggfunc=[np.mean]).reset_index()

    ret_avg.columns = ret_avg.columns.map('_'.join)

    ret_all = retdf.merge(ret_avg, how='left', left_on=['ID', 'Day'], right_on=['ID_','Day_'])

    ret_all['Total_Diff'] = (ret_all['Total_Load'] - ret_all['mean_Total_Load']) * 100 / ret_all['mean_Total_Load']
    for i in range(1,25):
        c1 = "H"+str(i)+'_Diff'
        c2 = "I"+format(i, '02d')+"00_KWH"
        c3 = "mean_I"+format(i, '02d')+"00_KWH"
        ret_all[c1] = (ret_all[c2] - ret_all[c3]) * 100 / ret_all[c3]

    cols = ['ID', 'RETAIL_CATEGORY', 'READ_DATE', 'Total_Load', 'mean_Total_Load', 'Total_Diff', \
        'I0100_KWH', 'mean_I0100_KWH', 'H1_Diff', \
        'I0200_KWH', 'mean_I0200_KWH', 'H2_Diff', \
        'I0300_KWH', 'mean_I0300_KWH', 'H3_Diff', \
        'I0400_KWH', 'mean_I0400_KWH', 'H4_Diff', \
        'I0500_KWH', 'mean_I0500_KWH', 'H5_Diff', \
        'I0600_KWH', 'mean_I0600_KWH', 'H6_Diff', \
        'I0700_KWH', 'mean_I0700_KWH', 'H7_Diff', \
        'I0800_KWH', 'mean_I0800_KWH', 'H8_Diff', \
        'I0900_KWH', 'mean_I0900_KWH', 'H9_Diff', \
        'I1000_KWH', 'mean_I1000_KWH', 'H10_Diff', \
        'I1100_KWH', 'mean_I1100_KWH', 'H11_Diff', \
        'I1200_KWH', 'mean_I1200_KWH', 'H12_Diff', \
        'I1300_KWH', 'mean_I1300_KWH', 'H13_Diff', \
        'I1400_KWH', 'mean_I1400_KWH', 'H14_Diff', \
        'I1500_KWH', 'mean_I1500_KWH', 'H15_Diff', \
        'I1600_KWH', 'mean_I1600_KWH', 'H16_Diff', \
        'I1700_KWH', 'mean_I1700_KWH', 'H17_Diff', \
        'I1800_KWH', 'mean_I1800_KWH', 'H18_Diff', \
        'I1900_KWH', 'mean_I1900_KWH', 'H19_Diff', \
        'I2000_KWH', 'mean_I2000_KWH', 'H20_Diff', \
        'I2100_KWH', 'mean_I2100_KWH', 'H21_Diff', \
        'I2200_KWH', 'mean_I2200_KWH', 'H22_Diff', \
        'I2300_KWH', 'mean_I2300_KWH', 'H23_Diff', \
        'I2400_KWH', 'mean_I2400_KWH', 'H24_Diff']
    
    temp = ret_all[cols]
    
    excptdf = temp.query("Total_Diff < -1 | H1_Diff < -1 | H2_Diff < -1 | H3_Diff < -1 | H4_Diff < -1 | H5_Diff < -1 | H6_Diff < -1 | H7_Diff < -1 | H8_Diff < -1 | H9_Diff < -1 | H10_Diff < -1 | H11_Diff < -1 | H12_Diff < -1 | H13_Diff < -1 | H14_Diff < -1 | H15_Diff < -1 | H16_Diff < -1 | H17_Diff < -1 | H18_Diff < -1 | H19_Diff < -1 | H20_Diff < -1 | H21_Diff < -1 | H22_Diff < -1 | H23_Diff < -1 | H24_Diff < -1")

    return excptdf

def zero_rpt(e1, e2, e3):
    zerodf = pd.DataFrame()
    zdf = e1.query("Total_Load==0 | I0100_KWH==0 | I0200_KWH==0 | I0300_KWH==0 | I0400_KWH==0 | I0500_KWH==0 | I0600_KWH==0 | I0700_KWH==0 | I0800_KWH==0 | I0900_KWH==0 | I1000_KWH==0 | I1100_KWH==0 | I1200_KWH==0 | I1300_KWH==0 | I1400_KWH==0 | I1500_KWH==0 | I1600_KWH==0 | I1700_KWH==0 | I1800_KWH==0 | I1900_KWH==0 | I2000_KWH==0 | I2100_KWH==0 | I2200_KWH==0 | I2300_KWH==0 | I2400_KWH==0")
    zerodf = zerodf.append(zdf)
    zdf = e2.query("Total_Load==0 | I0100_KWH==0 | I0200_KWH==0 | I0300_KWH==0 | I0400_KWH==0 | I0500_KWH==0 | I0600_KWH==0 | I0700_KWH==0 | I0800_KWH==0 | I0900_KWH==0 | I1000_KWH==0 | I1100_KWH==0 | I1200_KWH==0 | I1300_KWH==0 | I1400_KWH==0 | I1500_KWH==0 | I1600_KWH==0 | I1700_KWH==0 | I1800_KWH==0 | I1900_KWH==0 | I2000_KWH==0 | I2100_KWH==0 | I2200_KWH==0 | I2300_KWH==0 | I2400_KWH==0")
    zerodf = zerodf.append(zdf)
    zdf = e3.query("Total_Load==0 | I0100_KWH==0 | I0200_KWH==0 | I0300_KWH==0 | I0400_KWH==0 | I0500_KWH==0 | I0600_KWH==0 | I0700_KWH==0 | I0800_KWH==0 | I0900_KWH==0 | I1000_KWH==0 | I1100_KWH==0 | I1200_KWH==0 | I1300_KWH==0 | I1400_KWH==0 | I1500_KWH==0 | I1600_KWH==0 | I1700_KWH==0 | I1800_KWH==0 | I1900_KWH==0 | I2000_KWH==0 | I2100_KWH==0 | I2200_KWH==0 | I2300_KWH==0 | I2400_KWH==0")
    zerodf = zerodf.append(zdf)
    
    zcols = ['ID', 'RETAIL_CATEGORY', 'READ_DATE', 'Total_Load', 'I0100_KWH', 'I0200_KWH', 'I0300_KWH', 'I0400_KWH', \
        'I0500_KWH', 'I0600_KWH', 'I0700_KWH', 'I0800_KWH', 'I0900_KWH', 'I1000_KWH', 'I1100_KWH', 'I1200_KWH', \
        'I1300_KWH', 'I1400_KWH', 'I1500_KWH', 'I1600_KWH', 'I1700_KWH', 'I1800_KWH', 'I1900_KWH', 'I2000_KWH', \
        'I2100_KWH', 'I2200_KWH', 'I2300_KWH', 'I2400_KWH'] 
    
    zerodf = zerodf[zcols]
    
    return zerodf

def msng_rpt(inpdf, s_dt, e_dt):
    unq = inpdf.ID.unique()
    num_id = len(unq)
    
    dtrg = pd.date_range(s_dt, e_dt)
    num_days = len(dtrg)
    dtrg = np.tile(dtrg,num_id).reshape(-1,1)
    dt_df = pd.DataFrame(dtrg, columns=['Date'])
    dt_df['Date'] = pd.to_datetime(dt_df['Date'], format='%Y-%m-%d')
    
    inp_id = pd.DataFrame(np.repeat(unq, num_days), columns=['ID'])
    ref = pd.concat([inp_id,dt_df], axis=1)
    inpdf['READ_DATE'] = pd.to_datetime(inpdf['READ_DATE'], format='%Y-%m-%d')
    result = inpdf.merge(ref, how='outer', left_on=['ID', 'READ_DATE'], right_on=['ID', 'Date'], indicator=True)
    msng = result[result['_merge']=="right_only"][ref.columns]
    
    return msng
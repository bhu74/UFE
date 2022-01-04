#!/usr/bin/env python
# coding: utf-8

# ## Unaccounted for Energy (UFE) Analysis

# In[ ]:


import numpy as np
import pandas as pd
from pandas import ExcelWriter
import datetime as dt
import matplotlib.pyplot as plt

from UFE_utils import *


# In[ ]:


# Load Dataset 1 - Independent Measured Hourly Usage data
ds1 = pd.read_csv("Independent_Measured_Hourly_Usage.txt", sep=",", header = 0, thousands=',')

#Load Dataset - 2 - Meter Measured Hourly Usage data
ds2 = pd.read_csv("Meter_Measured_Hourly_Usage.txt", sep=",", header = 0)

# Filter Large C&I records, remove records before 15-Jan-2017 and create LargeCI file 
ds2lci = ds2[ds2['RETAIL_CATEGORY']=='LARGE C&I'].copy()
ds2lci['READ_DATE'] = pd.to_datetime(ds2lci['READ_DATE'], format='%Y-%m-%d')
ds2lci.drop(ds2lci[ds2lci.READ_DATE < dt.date(2017,1,15)].index, inplace=True)
       
# Filter Small C&I records, remove records before 15-Jan-2017 and create LargeCI file 
ds2sci = ds2[ds2['RETAIL_CATEGORY']=='SMALL_C&I'].copy()
ds2sci['READ_DATE'] = pd.to_datetime(ds2sci['READ_DATE'], format='%Y-%m-%d')
ds2sci.drop(ds2sci.loc[ds2sci.READ_DATE < dt.date(2017,1,15)].index, inplace=True)
        
# Filter Residential records and create file 
ds2res = ds2[ds2['RETAIL_CATEGORY']=='RESIDENTIAL'].copy()
ds2res['READ_DATE'] = pd.to_datetime(ds2res['READ_DATE'], format='%Y-%m-%d')
ds2res.drop(ds2res[ds2res.READ_DATE < dt.date(2017,1,15)].index, inplace=True)

tot_rec = tot_rec.append(ds2lci)
tot_rec = tot_rec.append(ds2sci)
tot_rec = tot_rec.append(ds2res)


# In[ ]:


# Overall Level processing

# Calculate total metered usage, collate ISO measured data and compute UFE% 
lci = load_sum(ds2lci)
sci = load_sum(ds2sci)
res = load_sum(ds2res)
lsr = load_sum(tot_rec)
lci = lci.reset_index()
sci = sci.reset_index()
res = res.reset_index()

ds1['ISO_DATE'] = pd.to_datetime(ds1['ISO_DATE'], format='%m/%d/%Y')
isohr = pd.pivot_table(ds1, index = ["ISO_DATE"], values=["ISO_ZONAL_LOAD"], aggfunc=[np.sum])
isohr.columns = isohr.columns.map('_'.join)
allhr = pd.concat([isohr, lsr], axis=1).reset_index()

allhr['Metered_Load'] = allhr['sum_I0100_KWH'] + allhr['sum_I0200_KWH'] + allhr['sum_I0300_KWH'] +     allhr['sum_I0400_KWH'] + allhr['sum_I0500_KWH'] + allhr['sum_I0600_KWH'] + allhr['sum_I0700_KWH'] +     allhr['sum_I0800_KWH'] + allhr['sum_I0900_KWH'] + allhr['sum_I1000_KWH'] + allhr['sum_I1100_KWH'] +     allhr['sum_I1200_KWH'] + allhr['sum_I1300_KWH'] + allhr['sum_I1400_KWH'] + allhr['sum_I1500_KWH'] +     allhr['sum_I1600_KWH'] + allhr['sum_I1700_KWH'] + allhr['sum_I1800_KWH'] + allhr['sum_I1900_KWH'] +     allhr['sum_I2000_KWH'] + allhr['sum_I2100_KWH'] + allhr['sum_I2200_KWH'] + allhr['sum_I2300_KWH'] +     allhr['sum_I2400_KWH']

allhr['UFE_perc'] = (allhr['sum_ISO_ZONAL_LOAD'] - allhr['Metered_Load']) * 100 / allhr['sum_ISO_ZONAL_LOAD']

allhr.drop(columns=['sum_I0100_KWH', 'sum_I0200_KWH', 'sum_I0300_KWH', 'sum_I0400_KWH','sum_I0500_KWH',     'sum_I0600_KWH', 'sum_I0700_KWH', 'sum_I0800_KWH', 'sum_I0900_KWH', 'sum_I1000_KWH', 'sum_I1100_KWH',    'sum_I1200_KWH', 'sum_I1300_KWH', 'sum_I1400_KWH', 'sum_I1500_KWH', 'sum_I1600_KWH', 'sum_I1700_KWH',    'sum_I1800_KWH', 'sum_I1900_KWH', 'sum_I2000_KWH', 'sum_I2100_KWH', 'sum_I2200_KWH', 'sum_I2300_KWH', 'sum_I2400_KWH'], inplace=True)

allhr = allhr.rename(columns={'index':'READ_DATE'})
allhr.to_csv("Summary_All.csv")


# In[ ]:


# Region Level processing - Compute region level summary
lci_sum = reg_sum(lci)
sci_sum = reg_sum(sci)
res_sum = reg_sum(res)

writer = ExcelWriter('Summary-Region.xlsx')
lci_sum.to_excel(writer,'Large C&I')
sci_sum.to_excel(writer,'Small C&I')
res_sum.to_excel(writer,'Residential')
writer.save()


# In[ ]:


# ID Level processing

# Compute day-wise average,total usage and identify exceptions for each ID
excpt_lci = idproc(ds2lci)
excpt_sci = idproc(ds2sci)
excpt_res = idproc(ds2res)

# Prepare zero usage report
zrpt = zero_rpt(excpt_lci, excpt_sci, excpt_res)
writer = ExcelWriter('Zero Usage Report.xlsx')
zrpt.to_excel(writer,'ZeroUsage')
writer.save()

# Prepare missing records report
stdt = dt.date(2017,1,15)
endt = dt.date(2017,2,14)
msng = msng_rpt(ds2lci, stdt, endt)
smsng = msng_rpt(ds2sci, stdt, endt)
msng = msng.append(smsng)
rmsng = msng_rpt(ds2res, stdt, endt)
msng = msng.append(rmsng)

writer = ExcelWriter('Missing Records Report.xlsx')
msng.to_excel(writer,'MissingRecords')
writer.save()

# Prepare Low Total Usage report
low_tot = excpt_lci.query("Total_Diff < -50")
low_tot = low_tot.append(excpt_sci.query("Total_Diff < -50"))
low_tot = low_tot.append(excpt_res.query("Total_Diff < -50"))

low_tid = low_tot.merge(zrpt, how='outer', on='ID', indicator=True)
tot_unq = low_tid[low_tid['_merge']=="left_only"][low_tid.columns]

writer = ExcelWriter('Low-Total-Usage.xlsx')
tot_unq.to_excel(writer,'Low_Total')
writer.save()

# Prepare Low hourly usage report
low_hr = excpt_lci.query("H1_Diff < -50 | H2_Diff < -50 | H3_Diff < -50 | H4_Diff < -50 | H5_Diff < -50 | H6_Diff < -50 | H7_Diff < -50 | H8_Diff < -50 | H9_Diff < -50 | H10_Diff < -50 | H11_Diff < -50 | H12_Diff < -50 | H13_Diff < -50 | H14_Diff < -50 | H15_Diff < -50 | H16_Diff < -50 | H17_Diff < -50 | H18_Diff < -50 | H19_Diff < -50 | H20_Diff < -50 | H21_Diff < -50 | H22_Diff < -50 | H23_Diff < -50 | H24_Diff < -50")
low_hr = low_hr.append(excpt_sci.query("H1_Diff < -50 | H2_Diff < -50 | H3_Diff < -50 | H4_Diff < -50 | H5_Diff < -50 | H6_Diff < -50 | H7_Diff < -50 | H8_Diff < -50 | H9_Diff < -50 | H10_Diff < -50 | H11_Diff < -50 | H12_Diff < -50 | H13_Diff < -50 | H14_Diff < -50 | H15_Diff < -50 | H16_Diff < -50 | H17_Diff < -50 | H18_Diff < -50 | H19_Diff < -50 | H20_Diff < -50 | H21_Diff < -50 | H22_Diff < -50 | H23_Diff < -50 | H24_Diff < -50"))
low_hr = low_hr.append(excpt_res.query("H1_Diff < -50 | H2_Diff < -50 | H3_Diff < -50 | H4_Diff < -50 | H5_Diff < -50 | H6_Diff < -50 | H7_Diff < -50 | H8_Diff < -50 | H9_Diff < -50 | H10_Diff < -50 | H11_Diff < -50 | H12_Diff < -50 | H13_Diff < -50 | H14_Diff < -50 | H15_Diff < -50 | H16_Diff < -50 | H17_Diff < -50 | H18_Diff < -50 | H19_Diff < -50 | H20_Diff < -50 | H21_Diff < -50 | H22_Diff < -50 | H23_Diff < -50 | H24_Diff < -50"))

low_hid = low_hr.merge(zrpt, how='outer', on='ID', indicator=True)
hr_unq = low_hid[low_hid['_merge']=="left_only"][low_hid.columns]

writer = ExcelWriter('Low-Hourly-Usage.xlsx')
hr_unq.to_excel(writer,'Low_Hourly')
writer.save()


#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import pandas as pd
import numpy as np
from itertools import chain
import datetime as dt
import shutil
import datetime
from datetime import datetime

#Change the file name as date
src = r'orginal_file_download_folder'
dst = r'C:\Users\wendysu\Desktop\QDR'
folder_time = datetime.now().strftime('%Y%m%d')
dst1 = dst + '\LotStatusWithCQDR-' + folder_time + '.csv'

file = shutil.copy(src,dst1)

df = pd.read_csv(file)

#filter C_QDR_wafer_count != 0
new_df = df[df['C_QDR_wafer_count'] != 0]

#add work week
new_df['date_to_probe'].fillna(0, inplace=True)
new_df['date'] = pd.to_datetime(new_df['date_to_probe']) + pd.DateOffset(days = 3)
new_df['WW'] = new_df['date'].dt.week

#separate C_QDR_info
split1 = new_df['C_QDR_info'].str.split('::', n = 1, expand = True).rename(columns = {0:'QDR_no', 1:'split2'})
split2 = split1['split2'].str.split('::', n = 1, expand = True).rename(columns = {0:'QDR_wafer', 1:'QDR_desc'})
new_df['QDR_no'] = split1['QDR_no'].str[8:]
new_df['QDR_wafer'] = split2['QDR_wafer'].str[11:]
new_df['QDR_desc'] = split2['QDR_desc'].str[10:]


# return list from series of comma-separated strings
def chainer(s):
    return list(chain.from_iterable(s.str.split(',')))

# calculate lengths of splits
lens = new_df['QDR_wafer'].str.split(',').map(len)

# create new dataframe, repeating or chaining as appropriate
res = pd.DataFrame({'lot_id': np.repeat(new_df['lot_id'], lens),
                    'lot_no': np.repeat(new_df['lot_no'], lens),
                    'design_id': np.repeat(new_df['design_id'], lens),
                    'mfg_area_id': np.repeat(new_df['mfg_area_id'], lens),
                    'trav_id': np.repeat(new_df['trav_id'], lens),
                    'step_name': np.repeat(new_df['step_name'], lens),
                    'status': np.repeat(new_df['status'], lens),
                    'qty': np.repeat(new_df['qty'], lens),
                    'days_to_probe': np.repeat(new_df['days_to_probe'], lens),
                    'date_to_probe': np.repeat(new_df['date_to_probe'], lens),
                    'WW': np.repeat(new_df['WW'], lens),
                    'C_QDR_wafer_count': np.repeat(new_df['C_QDR_wafer_count'], lens),
                    'qdr_area': np.repeat(new_df['qdr_area'], lens),
                    'source_cause_code': np.repeat(new_df['source_cause_code'], lens),
                    'occurred_step_name': np.repeat(new_df['occurred_step_name'], lens),
                    'QDR_no': np.repeat(new_df['QDR_no'], lens),
                    'QDR_wafer': chainer(new_df['QDR_wafer']),
                    'QDR_desc': np.repeat(new_df['QDR_desc'], lens),
                   })



# res['QDR_Category'] = 'PHOTO REWORK' or 'CMP REWORK' or 'QTIME BREACH' or 'TOOL ABORT' or 'CD OOS' or 'DEFECT OOS' or 'THK OOS' or 'REG OOS' or 'EPF' or 'PROBE' or 'OTHER'

conditions = [
    (res['qdr_area'].str.contains('PHOTO') & res['QDR_desc'].str.contains(r'rework|RWK|Rework|rwk|REWORK|RKW' )),
    (res['source_cause_code'].str.contains('PHOTO REWORK')),
    (res['qdr_area'].str.contains('CMP') & res['QDR_desc'].str.contains(r'rework|RWK|Rework|rwk|REWORK|RKW' )),
    (res['qdr_area'].str.contains('CMP') & res['source_cause_code'].str.contains('REWORK')),
    (res['QDR_desc'].str.contains('Q time|Q TIME|Q-TIME|Q-time|q-time|q time|Qtime|qtime')),
    (res['source_cause_code'].str.contains('QTIME')),
    (res['QDR_desc'].str.contains('equipment variation|abort|trbl')),
    (res['source_cause_code'].str.contains('ABORT|WAFER HANGED|TOOL|INTERRUPTED|FAULT|PUMP')),
    (res['QDR_desc'].str.contains('arcing|cb|defect|corrosion|scratch')),
    (res['source_cause_code'].str.contains('CONTAM|Z-RDA|DEFECT|FLAKE|')),
    (res['QDR_desc'].str.contains('thk|thickness|delta')),
    (res['source_cause_code'].str.contains('THICKNESS')),
    (res['QDR_desc'].str.contains('cd')),
    (res['source_cause_code'].str.contains('OOS CD|CD')),
    (res['QDR_desc'].str.contains('reg ooa|reg ooc|reg oos|reg x|reg y')),
    (res['source_cause_code'].str.contains('REGISTRATION')),
    (res['source_cause_code'].str.contains('PROBE')),
    (res['qdr_area'].str.contains('PROBE')),
]
values = ['PHOTO REWORK', 'PHOTO REWORK', 'CMP REWORK', 'CMP REWORK', 'QTIME BREACH', 'QTIME BREACH','TOOL ABORT', 'TOOL ABORT','DEFECT OOS', 'DEFECT OOS', 'CD OOS', 'CD OOS', 'THK OOS', 'THK OOS', 'REG OOS', 'REG OOS', 'PROBE', 'PROBE']

res['QDR_Categpry'] = np.select(conditions, values)

#combine lot_no and QDR_wafer no
res['Wafer_no'] = res.agg('{0[lot_no]}-{0[QDR_wafer]}'.format, axis=1)

qdr = res.drop_duplicates(subset = ['QDR_no', 'Wafer_no'], keep = 'last')
print(qdr)

#save file
path = r'C:\Users\wendysu\Desktop\QDR'
qdr.to_csv(os.path.join(path + r'\QDR_1.csv')) 


# In[3]:


#QDR__wafer_cnt based on QDR_no to judge if it's a GDM case
GDM = qdr['QDR_no'].value_counts().rename_axis('QDR_no').reset_index(name='QDR_wafer_cnt')

conditions = [
    (GDM['QDR_wafer_cnt'] >= 50),
    (GDM['QDR_wafer_cnt'] <50),
]
values = ['GDM', 'Non-GDM']
GDM['GDM'] = np.select(conditions, values)
print(GDM)
path = r'C:\Users\wendysu\Desktop\QDR'
GDM.to_csv(os.path.join(path + r'\QDR_wafer_cnt_gdm.csv'))


# In[4]:


#append into CQDR% table and check if it meents target
NCD = qdr['WW'].value_counts().rename_axis('WW').reset_index(name='QDR_wafer_cnt')
print(NCD)

OMT_Output_CQDR_goal = pd.read_csv(r'C:\Users\wendysu\Desktop\QDR\OMT_Output_CQDR_goal.csv')

result = pd.merge(OMT_Output_CQDR_goal, NCD, how = 'outer', on = ['WW'])
result['Real_CQDR%'] = result['QDR_wafer_cnt']/result['Shipped_Wafer_Qty']
print(result)
path = r'C:\Users\wendysu\Desktop\QDR'
result.to_csv(os.path.join(path + r'\OMT_Output_CQDR_goal_1.csv'))


# In[ ]:





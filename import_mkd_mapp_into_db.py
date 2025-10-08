# -*- coding: utf-8 -*-
"""
Created on Mon May 19 13:54:50 2025

@author: zkoritnik
"""

import os
import pyodbc
import pandas as pd
from password import password
from helpers.data_handler import import_data

conn = pyodbc.connect(f"DRIVER={{NetezzaSQL}}; SERVER=bucko.zav-mb.loc; PORT=5480; DATABASE=LAKE_RE; UID={os.getlogin()}; PWD={password()};")

table_mappings = {
     'Missing Transaction Types': 'MKD_TRANSACTION_TYPE',
     'Missing Investment Types': 'MKD_INVESTMENT_TYPE',
     'Missing MKD Mappings': 'MKD_MAPP_INVESTMENTS',
     'Missing Navison Mapping': 'MKD_REG_REPORT_NAV_MAPP',
     'Missing Simcorp Mapping': 'MKD_REG_REPORT_MAPPING'
     }

PATH = 'T:/Finance_ZS-SR/3_BO_Skupine_SavaRe/SO_MKD/mapping'
file_name = 'insert_mapping'

df_dict = pd.read_excel(f"{PATH}/{file_name}.xlsx", sheet_name=None)

for k, df in df_dict.items():
    sql_table = table_mappings.get(k)
    import_data('FIN', sql_table, df, conn)

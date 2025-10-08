# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 12:31:25 2025

@author: zkoritnik
"""

import os
import sys
import pyodbc
import warnings
import numpy as np
import pandas as pd
from typing import List, Dict
from password import password

warnings.filterwarnings('ignore')

def get_excel_writer(paths: List[str]) -> pd.ExcelWriter:
    """
    Try to create Excel writer for multiple paths, return the first successful one.
    
    Args:
        paths: List of file paths to try
        
    Returns:
        pd.ExcelWriter: Successfully created Excel writer
        
    Raises:
        OSError: If all paths fail
    """
    writer = None
    for path in paths:
        try:
            writer = pd.ExcelWriter(path)
            print(f'Connected to: {path}')
            break
        except OSError:
            continue
    
    if writer is None:
        raise OSError(f'All paths failed! Tried: {", ".join(paths)}')
    
    return writer

class DatabaseConnector():
    def __init__(self):
        self.username = os.getlogin()
        self.password = password()
        self.connection_string = self._create_connection()

    def _create_connection(self):
        return pyodbc.connect(
            f"DRIVER={{NetezzaSQL}}; SERVER=bucko.zav-mb.loc; PORT=5480; DATABASE=LAKE_RE; UID={self.username}; PWD={self.password};"
            )
    
    def query_data(self, SQL):
        return pd.read_sql(SQL, self.connection_string)

class InvestmentCategory:
    """Class representing investment categories with predefined tags"""
    LAND_BUILDINGS_FOR_BUSINESS = 'I. Zemljišča in gradbeni objekti, ki se uporabljajo za opravljanje dejavnosti'
    LAND_BUILDINGS_NOT_FOR_BUSINESS = 'II. Zemljišča in gradbeni objekti, ki se ne uporabljajo za opravljanje dejavnosti'
    SHARES_IN_SUBSIDIARIES = 'III. Delnice, deleži in drugi lastniški instrumenti v hčerinskih družbah skupine, povezanih družbah in skupaj obvladovanih družbah'
    DEBT_SECURITIES_IN_GROUP = 'IV. Dolžniški vrednostni papirji, ki jih izdajo družbe v skupini – hčerinske družbe, povezana podjetja in skupaj obvladovane družbe'
    DEBT_SECURITIES_UNDER_ONE_YEAR = 'V. Dolžniški vrednostni papirji z zapadlostjo do enega leta (razen tistih iz točke IV zgoraj)'
    DEBT_SECURITIES_OVER_ONE_YEAR = 'VI. Dolžniški vrednostni papirji z zapadlostjo več kot eno leto (razen tistih iz točke IV zgoraj)'
    SHARES_EQUITY_INSTRUMENTS = 'VII. Delnice, delnice in drugi lastniški instrumenti (razen tistih, ki so navedeni v točki III zgoraj)'
    SHARES_IN_INVESTMENT_FUNDS = 'VIII. Delnice in deleži v investicijskih skladih (razen tistih iz točke III zgoraj)'
    DERIVATIVE_FINANCIAL_INSTRUMENTS = 'IX. Izvedeni finančni instrumenti'
    
    @classmethod
    def all_categories(cls) -> List[str]:
        """Return all investment categories as a list"""
        return [
            cls.LAND_BUILDINGS_FOR_BUSINESS,
            cls.LAND_BUILDINGS_NOT_FOR_BUSINESS,
            cls.SHARES_IN_SUBSIDIARIES,
            cls.DEBT_SECURITIES_IN_GROUP,
            cls.DEBT_SECURITIES_UNDER_ONE_YEAR,
            cls.DEBT_SECURITIES_OVER_ONE_YEAR,
            cls.SHARES_EQUITY_INSTRUMENTS,
            cls.SHARES_IN_INVESTMENT_FUNDS,
            cls.DERIVATIVE_FINANCIAL_INSTRUMENTS
        ]


class TransactionType:
    """Class representing transaction type codes"""
    ACCOUNTING_VALUE = '01 Skupni nabavni stroški/računovodska vrednost (do datuma zadnje ocene)'
    REVALUATION_EFFECT = '03 Učinek prevrednotenja'
    REVALUATION_RESERVE = '04 Revalorizacijska rezerva (status)'
    EXCHANGE_RATE_DIFFERENCE = '06 Neto tečajna razlika'
    AMORTIZATION = '07 Amortizacija popusta/premije na finančne instrumente z določeno zapadlostjo'


class DataLoader(DatabaseConnector):
    def __init__(self):
        super().__init__()
        self.OFFSET = 1
        self.REPORT_DATE = self._calculate_report_date()
        self.YEAR_START = self._calculate_year_start()
        self.PREVIOUS_QUARTER_END  = self._calculate_previous_quarter_end()
        self.QUARTER_START = (self.PREVIOUS_QUARTER_END  + pd.tseries.offsets.MonthBegin())
        self.transactions = None
        self.transaction_types = None
        self.investment_mapping = None
        self.investment_types = None
        self.gl = None
        self.loi = None
        self.nav = None
        self.scd_map = None
        self.scd_reg_report_map = None
        self.navision_map = None
        
        print(f'Report date: {self.REPORT_DATE.date()}',
              f'Year start date: {self.YEAR_START.date()}',
              f'Previous quarter end date: {self.PREVIOUS_QUARTER_END }',
              f'Current quarter start date: {self.QUARTER_START}',
              sep='\n')
    
    def _calculate_report_date(self):
        return (pd.Timestamp.now().normalize() - pd.tseries.offsets.QuarterEnd(self.OFFSET))
    
    def _calculate_previous_quarter_end(self):
        return (pd.Timestamp.now().normalize() - pd.tseries.offsets.QuarterEnd(self.OFFSET+1))
    
    def _calculate_year_start(self):
        return (pd.Timestamp.now().normalize() - pd.tseries.offsets.YearBegin(1 if not pd.Timestamp.now().month <= 3 else 2))
    
    def data_checker(self, df, column, table):
        year, month = self.REPORT_DATE.year, self.REPORT_DATE.month
        
        df[column] = pd.to_datetime(df[column], errors='coerce')
   
        match = df[(df[column].dt.year == year) & (df[column].dt.month == month)]
    
        if match.empty:
            sys.exit(f"No data found in table {table} for year {year} and month {month} in column '{column}'.")
    
        print(f"Data check passed: Data found in table {table} for year {year} and month {month} in column {column}.")

    def load_data(self):
        self._load_transactions()
        self._load_transactions_types()
        self._load_investment_mapping()
        self._load_investment_types()
        self._load_scd_mapping()
        self._load_gl_export()
        self._load_list_of_investments()
        self._load_navision_data()
        self._load_scd_reg_report_map()
        self._load_navision_mapping()        
        self._process_data()
        self._check_mapping()
    
    def _load_transactions(self):
        query = f"""
            SELECT DISTINCT *
            FROM LAKE_RE.FIN.MKD_TRANSACTIONS
            WHERE REPORT_DATE = '{self.REPORT_DATE}'
        """
        self.transactions = self.query_data(query)
        self.data_checker(self.transactions, 'REPORT_DATE', 'MKD_TRANSACTIONS')
    
    def _load_transactions_types(self):
        query = """
            SELECT DISTINCT *
            FROM LAKE_RE.FIN.MKD_TRANSACTION_TYPE
        """
        self.transaction_types = self.query_data(query)
        
    def _load_investment_mapping(self):
        query = """
            SELECT DISTINCT *
            FROM LAKE_RE.FIN.MKD_MAPP_INVESTMENTS
        """
        self.investment_mapping = self.query_data(query)

    def _load_investment_types(self):
        query = """
            SELECT DISTINCT *
            FROM LAKE_RE.FIN.MKD_INVESTMENT_TYPE
        """
        self.investment_types = self.query_data(query)
        self.investment_types.rename(columns={'KEY': 'INVEST_KEY'}, inplace=True)

    def _load_gl_export(self):
        query = f"""
            SELECT *
            FROM OLTP_SOMKD.SIMCORP.GL_EXPORT_CSV_HIST
            WHERE BOOKING_DATE <= '{self.REPORT_DATE}'
        """
        self.gl = self.query_data(query)
        self.data_checker(self.gl, 'BOOKING_DATE', 'GL_EXPORT_CSV_HIST')

    def _load_list_of_investments(self):
        query = f"""
            SELECT REPORT_DATE, 
            	INVESTMENT_TYPE,
            	IFRS_GROUP,
            	INVESTMENT_NAME,
            	ISIN,
            	NOMINAL_VALUE_OF_LOT_QC,
            	NUMBER_OF_LOTS as KOLIČINA,
            	QUOTATION_CURRENCY,
            	ACQUISITION_VALUE_IN_QC,
            	ACQUISITION_VALUE_IN_PC as VREDNOST_PRIDOBITVE,
            	BALANCE_BOOK_VALUE_IN_QC,
            	BALANCE_BOOK_VALUE_IN_PC,
            	COUPON_RATE AS OBRESTNA_MERA,
            	EFFECTIVE_INTEREST_RATE AS EFEKTIVNA_OBRESTNA_MERADONOS,
            	ACCRUED_INTEREST_IN_QC,
            	ACCRUED_INTEREST_IN_PC as AKUMULIRANE_OBRESTI,
            	PURCHASE_DATE AS DATUM_NALOŽBE,
            	MATURITY_DATE AS DATUM_ZAPADLOSTI,
            	ISSUER_RATING_SECOND_BEST AS OCENE,
            	ISSUER_RATING_AGENCY_SECOND_BEST,
            	DIRTY_MARKET_VALUE_IN_QC,
            	DIRTY_MARKET_VALUE_IN_PC,
            	SECURITY_ID,
            	LT_ST,
                COUPON_FREQUENCY AS POGOSTOST_KUPONOV,
                (BALANCE_BOOK_VALUE_IN_QC + ACCRUED_INTEREST_IN_QC) AS RAČUNOVODSKA_VREDNOST_V_IZVIRNI_VALUTI,
                (BALANCE_BOOK_VALUE_IN_PC + ACCRUED_INTEREST_IN_PC) AS OBJEKTIVNA_VREDNOST,
                (BALANCE_BOOK_VALUE_IN_PC + ACCRUED_INTEREST_IN_PC) AS RAČUNOVODSKA_VREDNOST,
                SECURITY_ID || INVESTMENT_TYPE || LT_ST AS SCD_ID,
                NULL AS AMORTIZIRANI_ODHODKI
            FROM OLTP_SOMKD.SIMCORP.LIST_OF_INVESTMENTS_POSITIONS_HIST 
            WHERE REPORT_DATE = '{self.REPORT_DATE}'
        """
        self.loi = self.query_data(query)
        self.data_checker(self.loi, 'REPORT_DATE', 'LIST_OF_INVESTMENTS_POSITIONS_HIST')

    def _load_navision_data(self):
        query = f"""
        SELECT
            B.NO_ || B.NO_2 || B.NAME AS "KEY",
        	B.NO_,
            B.NO_2,
            B.NAME,
            SUM(A.AMOUNT) AS SALDO
        FROM OLTP_SOMKD.NAV.G_L_ENTRY_1 AS A
        LEFT JOIN (
        	SELECT DISTINCT NO_2, NO_, NAME 
        	FROM OLTP_SOMKD.NAV.G_L_ACCOUNT_1
        ) AS B ON A.G_L_ACCOUNT_NO_ = B.NO_
        WHERE A.POSTING_DATE <= '{self.REPORT_DATE}'
        GROUP BY B.NO_, B.NO_2, B.NAME
        ORDER BY NO_ ASC
        """
        self.nav = self.query_data(query)
        
        query = f"""
        SELECT DISTINCT
            A.POSTING_DATE
        FROM OLTP_SOMKD.NAV.G_L_ENTRY_1 AS A
        LEFT JOIN (
        	SELECT DISTINCT NO_2, NO_, NAME 
        	FROM OLTP_SOMKD.NAV.G_L_ACCOUNT_1
        ) AS B ON A.G_L_ACCOUNT_NO_ = B.NO_
        WHERE A.POSTING_DATE <= '{self.REPORT_DATE}'
        """
        self.nav_posting = self.query_data(query)
        self.data_checker(self.nav_posting, 'POSTING_DATE', 'G_L_ENTRY_1')

    def _load_scd_mapping(self):
        query = """
        SELECT DISTINCT * FROM LAKE_RE.FIN.MKD_LOI_MAPP
        """
        self.scd_map = self.query_data(query)
        self.scd_map = dict(zip(self.scd_map['KEY'], self.scd_map['VALUE']))
        
    def _load_scd_reg_report_map(self):
        query = """
        SELECT DISTINCT * FROM LAKE_RE.FIN.MKD_REG_REPORT_MAPPING
        """
        self.scd_reg_report_map = self.query_data(query)
        self.scd_reg_report_map = self.scd_reg_report_map.set_index('SCD_ID').to_dict(orient='index')
        
    def _load_navision_mapping(self):
        query = """
        SELECT DISTINCT * FROM LAKE_RE.FIN.MKD_REG_REPORT_NAV_MAPP
        """
        self.navision_map = self.query_data(query)
        self.navision_map = self.navision_map.set_index('KEY').to_dict(orient='index')

    def _process_data(self):
        self.gl['SALDO_STANJE'] = self.gl['DEBIT_AMOUNT_FOREIGN_CUR'] - self.gl['CREDIT_AMOUNT_FOREIGN_CUR']
        self.gl['SALDO_SPREMEMBA'] = self.gl['SALDO_STANJE'] * -1
        self.gl['KEY'] = self.gl[['GROUP_ACCOUNT', 'SECURITY_TYPE', 'INVESTMENTS']].astype(str).agg(''.join, axis=1).str.strip()
        self.gl['KEY_1'] = self.gl[['SECURITY_TYPE', 'LT_ST']].astype(str).agg(''.join, axis=1).str.strip()
        self.gl['KEY_2'] = self.gl[['SECURITY_ID', 'SECURITY_TYPE']].astype(str).agg(''.join, axis=1).str.strip()
        
        self.gl = self.gl.merge(
            self.transaction_types[['KEY', 'MAPIRANJE_STANJE', 'MAPIRANJE_SPREMEMBA', 'VRSTA_TRANSKACIJE_NERDZ', 'VRTA_TRANSAKCIJE_RDZ']], 
            on='KEY',
            how='left'
        )
        self.gl = self.gl.merge(
            self.investment_types[['INVEST_KEY', 'VALUE']], 
            left_on='KEY_1',
            right_on='INVEST_KEY',
            how='left'
        )
        self.gl = self.gl.merge(
            self.investment_mapping[['KEY', 'TAGS', 'MSRP_RAZVRSTITEV', 'METODA_VREDNOTENJA', 'METODA_VREDNOTELJA_VOL2','VIR_SREDSTEV']],
            left_on='KEY_2', 
            right_on='KEY', 
            how='left',
            suffixes=('', '_mapping')
        )
        
        self.transactions['KEY'] = self.transactions[['SECURITY_ID', 'SECTYPE']].astype(str).agg(''.join, axis=1).str.strip()
        self.transactions['INVEST_KEY'] = self.transactions[['SECTYPE', 'LT_ST']].astype(str).agg(''.join, axis=1).str.strip()

        self.transactions = self.transactions.merge(
            self.investment_mapping[['KEY', 'TAGS']], 
            on='KEY', 
            how='left'
        )
        self.transactions = self.transactions.merge(
            self.investment_types[['INVEST_KEY', 'VALUE']], 
            on='INVEST_KEY', 
            how='left'
        )
        
        mapp_nav = self.nav['KEY'].apply(lambda x: pd.Series(self.navision_map.get(x, {})))
        self.nav = pd.concat([self.nav, mapp_nav], axis=1)
        
        mapp_loi = self.loi['SCD_ID'].apply(lambda x: pd.Series(self.scd_reg_report_map.get(x, {})))
        self.loi = pd.concat([self.loi, mapp_loi], axis=1)
        
    def _check_mapping(self):
        trans_type = set(self.gl['KEY']) - set(self.transaction_types['KEY'])
        investment_type = set(self.gl['KEY_1']) - set(self.investment_types['INVEST_KEY'])
        mkd_mapping = set(self.gl['KEY_2']) - set(self.investment_mapping['KEY'])
        nav_mapp = set(self.nav['KEY']) - set(self.navision_map.keys())
        scd_mapp = set(self.loi['SCD_ID']) - set(self.scd_reg_report_map.keys())
        
        self.mapping_issues = {}
        if trans_type:
            self.mapping_issues['Missing Transaction Types'] = self.gl.loc[
                self.gl['KEY'].isin(trans_type),
                ['KEY', 'GROUP_ACCOUNT', 'SECURITY_TYPE', 'INVESTMENTS', 'MAPIRANJE_STANJE', 
                 'MAPIRANJE_SPREMEMBA', 'VRSTA_TRANSKACIJE_NERDZ', 'VRTA_TRANSAKCIJE_RDZ']
            ].drop_duplicates()
            
        if investment_type:
            self.mapping_issues['Missing Investment Types'] = self.gl.loc[
                self.gl['KEY_1'].isin(investment_type),
                ['KEY_1', 'SECURITY_TYPE', 'LT_ST', 'VALUE']
            ].drop_duplicates()
            
        if mkd_mapping:
            self.mapping_issues['Missing MKD Mappings'] = self.gl.loc[
                self.gl['KEY_2'].isin(mkd_mapping),
                ['KEY_2', 'SECURITY_ID', 'SECURITY_TYPE', 'PURPOSE', 'TAGS',
                 'MSRP_RAZVRSTITEV', 'METODA_VREDNOTENJA', 'METODA_VREDNOTELJA_VOL2', 'VIR_SREDSTEV']
            ].drop_duplicates()
            
        if nav_mapp:
            self.mapping_issues['Missing Navison Mapping'] = self.nav.loc[
                self.nav['KEY'].isin(nav_mapp)
            ].drop_duplicates().drop(columns=['KONTO_ST1', 'KONTO_ST2', 'OPIS', 'SALDO'])
            
        if scd_mapp:
            self.mapping_issues['Missing Simcorp Mapping'] = self.loi.loc[
                self.loi['SCD_ID'].isin(scd_mapp),
                ['SCD_ID', 'LASTNOST', 'VIR_SREDSTEV', 'ŠTEVILO_DELOVNIH_MEST_V_BS',
                'VRSTA_PODJETJA', 'PODVRSTA_PODJETJA', 'GARANCIJA',
                'IME_ZALOŽNIKA_IZVAJALEC', 'IME_ZALOŽNIKA_IZVAJALCA_ČE_JE_DRUGAČE',
                'RAZVRSTITEV_V_SKLADU_Z_MSRP', 'METODA_VREDNOTENJA',
                'ZALOŽNIKOVA_DRŽAVA_IZVAJALEC', 'TRGOVINSKA_DEŽELA',
                'REGULIRANI_TRGOVALNI_TRG', 'VIR_VREDNOTENJA', 'VRSTA_KUPONA',
                'SEKTOR', 'ISIN'
                ]
            ].drop_duplicates()            
            
        if self.mapping_issues:
            paths = [
                r'C:/ASO/mapping/insert_mapping.xlsx',
                r'T:/Finance_ZS-SR/3_BO_Skupine_SavaRe/SO_MKD/mapping/insert_mapping.xlsx'
                ]
            
            writer = get_excel_writer(paths)
        
            with writer:
                for sheet_name, df in self.mapping_issues.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            sys.exit('Update mapping!')
        else:
            print('All mapping up-to date')

class ReportGenerator:
    """Base class for generating financial reports"""
    def __init__(self, data_loader: DataLoader):
        """Initialize with data loader"""
        self.data = data_loader
        self.templates = {}
        
    def _add_total_row(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add a summary row with totals to a DataFrame"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if not len(numeric_cols):
            return df
            
        sums = df[numeric_cols].sum()
        total_row = pd.DataFrame({col: [sums[col] if col in numeric_cols else 'Skupaj'] 
                                for col in df.columns}, index=[0])
        return pd.concat([df, total_row], ignore_index=True)
        
    def save_report(self) -> None:
        """Save all report templates to Excel file"""
        
        paths = [
            f'C:/ASO/report_SO_MKD_{self.data.REPORT_DATE.date()}.xlsx',
            f'T:/Finance_ZS-SR/3_BO_Skupine_SavaRe/SO_MKD/report_SO_MKD_{self.data.REPORT_DATE.date()}.xlsx'
        ]
        
        writer = get_excel_writer(paths)
    
        with writer:
            for sheet_name, df in self.templates.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)


class RealizedProfitGenerator(ReportGenerator):
    def __init__(self, data_loader: DataLoader):
        super().__init__(data_loader)
        self.data = data_loader
        self.realized = self._load_realized_data()
        
    def _load_realized_data(self):
        return self.data.gl.loc[self.data.gl['BOOKING_DATE'].between(self.data.YEAR_START, self.data.REPORT_DATE)]
    
    def generate_report(self):
        self._generate_all_categories_template()
        self._generate_equity_template()

    def _generate_all_categories_template(self) -> None:
        """Generate template for all investment categories realized profits"""
        
        result = []
        
        for tag in InvestmentCategory.all_categories():
            equity = self.data.transactions.loc[self.data.transactions['VALUE'] == tag, 'NOMINAL'].sum()
    
            accouting_value = self.realized.loc[
                (self.realized['VRTA_TRANSAKCIJE_RDZ'] == 'Računovodska vrednost') & 
                (self.realized['VALUE'] == tag), 
                'SALDO_STANJE'].sum()
            
            pnl = self.realized.loc[
                (self.realized['VRTA_TRANSAKCIJE_RDZ'] == 'Realizirani dobiček (izguba)') &
                (self.realized['VALUE'] == tag), 
                'SALDO_STANJE'].sum()*-1
            
            sell_value = accouting_value + pnl
            
            result.append({
                'Tags': tag,
                'Število vrednostnih papirjev': equity,
                'Računovodska vrednost': accouting_value,
                'Prodajna vrednost': sell_value,
                'Realizirani dobiček (izguba)': pnl if pnl != -0 else 0
            })
            
        template = pd.DataFrame(result)
        self.templates['REALIZED_PROFIT_ALL'] = self._add_total_row(template)

    def _generate_equity_template(self):      
        
        equity_data = self.realized.loc[
            self.realized['VALUE'] == InvestmentCategory.SHARES_IN_INVESTMENT_FUNDS]

        transactions_equity = self.data.transactions.loc[
            self.data.transactions['VALUE']== InvestmentCategory.SHARES_IN_INVESTMENT_FUNDS]
        
        tags = set(equity_data['TAGS'])
        
        result = []
        for tag in sorted(tags):
            msrp = equity_data.loc[equity_data['TAGS'] == tag, 'MSRP_RAZVRSTITEV'].iloc[0]
            vir_sredstev = equity_data.loc[equity_data['TAGS'] == tag, 'VIR_SREDSTEV'].iloc[0]
            equity = transactions_equity.loc[transactions_equity['TAGS'] == tag, 'NOMINAL'].sum()
            
            book_values = equity_data.loc[
                (equity_data['TAGS'] == tag) &
                (equity_data['VRTA_TRANSAKCIJE_RDZ'] == 'Računovodska vrednost'),
                'SALDO_STANJE'].sum()
            
            pnl = equity_data.loc[
                (equity_data['TAGS'] == tag) &
                (equity_data['VRTA_TRANSAKCIJE_RDZ'] == 'Realizirani dobiček (izguba)'),
                'SALDO_STANJE'].sum()*-1
            
            sell_value = book_values + pnl
            
            result.append({
                'Tags': tag,
                'Razvrstitev v skladu z MSRP': msrp,
                'Število vrednostnih papirjev': equity,
                'Računovodska vrednost': book_values,
                'Prodajna vrednost (FORMULA)': sell_value,
                'Realizirani dobiček (izguba)': pnl,
                'Vir sredstev': vir_sredstev
            })
            
            template = pd.DataFrame(result)
            self.templates['REALIZED_PROFIT_EQUITY'] = template


class UnrealizedProfitReportGenerator(ReportGenerator):
    def __init__(self, data_loader: DataLoader):
        super().__init__(data_loader)
        self.gl_status = self._load_gl_to_quarter()
        self.gl_change = self._load_gl_quarter()
        self.gl_combined = self._load_gl_combined()
        
    def _load_gl_to_quarter(self):
        return  self.data.gl.loc[ 
            (self.data.gl['BOOKING_DATE'] <= self.data.PREVIOUS_QUARTER_END ) &
            (self.data.gl['MAPIRANJE_STANJE'] == 'Stanje')
        ]
    
    def _load_gl_quarter(self):
        return self.data.gl.loc[
            (self.data.gl['BOOKING_DATE'].between(self.data.QUARTER_START, self.data.REPORT_DATE)) &
            (self.data.gl['MAPIRANJE_SPREMEMBA'] == 'Sprememba')
        ]
    
    def _load_gl_combined(self):
        return self.data.gl.loc[
            ((self.data.gl['BOOKING_DATE'] <= self.data.PREVIOUS_QUARTER_END ) & (self.data.gl['MAPIRANJE_STANJE'] == 'Stanje')) | 
            ((self.data.gl['BOOKING_DATE'].between(self.data.QUARTER_START, self.data.REPORT_DATE)) & (self.data.gl['MAPIRANJE_SPREMEMBA'] == 'Sprememba'))
        ]
    
    def generate_report(self):
        self._generate_all_categories_template()
        self._generate_equity_template()
        self._generate_bonds_template()
    
    def _calculate_gl_values(self, gl_to_quarter: pd.DataFrame, gl_quarter: pd.DataFrame, tag: str) -> Dict[str, float]:
        """Helper method to calculate GL values for a specific tag"""
        values = {}
        
        values['buy_sell_status'] = gl_to_quarter.loc[
            (gl_to_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.ACCOUNTING_VALUE) &
            (gl_to_quarter['VALUE'] == tag),
            'SALDO_STANJE'
        ].sum()
        
        values['buy_sell_change'] = gl_quarter.loc[
            (gl_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.ACCOUNTING_VALUE) &
            (gl_quarter['VALUE'] == tag),
            'SALDO_SPREMEMBA'
        ].sum()
        
        values['revaluation_reserve_status'] = gl_to_quarter.loc[
            (gl_to_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.REVALUATION_RESERVE) &
            (gl_to_quarter['VALUE'] == tag),
            'SALDO_STANJE'
        ].sum() * -1
        
        values['revaluation_reserve_change'] = gl_quarter.loc[
            (gl_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.REVALUATION_RESERVE) &
            (gl_quarter['VALUE'] == tag),
            'SALDO_SPREMEMBA'
        ].sum()
        
        values['fx_status'] = gl_to_quarter.loc[
            (gl_to_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.EXCHANGE_RATE_DIFFERENCE) &
            (gl_to_quarter['VALUE'] == tag),
            'SALDO_STANJE'
        ].sum()
        
        values['fx_change'] = gl_quarter.loc[
            (gl_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.EXCHANGE_RATE_DIFFERENCE) &
            (gl_quarter['VALUE'] == tag),
            'SALDO_SPREMEMBA'
        ].sum()
        
        values['amortization_status'] = gl_to_quarter.loc[
            (gl_to_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.AMORTIZATION) &
            (gl_to_quarter['VALUE'] == tag),
            'SALDO_STANJE'
        ].sum()
        
        values['amortization_change'] = gl_quarter.loc[
            (gl_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.AMORTIZATION) &
            (gl_quarter['VALUE'] == tag),
            'SALDO_SPREMEMBA'
        ].sum()
        
        values['revaluation_status'] = gl_to_quarter.loc[
            (gl_to_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.REVALUATION_EFFECT) &
            (gl_to_quarter['VALUE'] == tag),
            'SALDO_STANJE'
        ].sum()
        
        values['revaluation_change'] = gl_quarter.loc[
            (gl_quarter['VRSTA_TRANSKACIJE_NERDZ'] == TransactionType.REVALUATION_EFFECT) &
            (gl_quarter['VALUE'] == tag),
            'SALDO_SPREMEMBA'
        ].sum()
        
        return values
    
    def _generate_all_categories_template(self):
        result = []
        
        for tag in InvestmentCategory.all_categories():
            
            values = self._calculate_gl_values(self.gl_status, self.gl_change, tag)
            
            revaluation_effect = (values['revaluation_reserve_change'] + values['revaluation_change'])
            
            objective_value = (values['buy_sell_status'] +
                               values['buy_sell_change'] +
                               revaluation_effect +
                               values['fx_status'] +
                               values['fx_change'] +
                               values['amortization_status'] +
                               values['amortization_change'])
            
            result.append({
                'Tags': tag,
                'Skupni nabavni stroški/računovodska vrednost (do datuma zadnje ocene)': values['buy_sell_status'] + values['buy_sell_change'],
                'Objektivna vrednost na datum zadnje ocene (formula)': objective_value,
                'Učinek prevrednotenja': revaluation_effect,
                'Revalorizacijska rezerva (status)': values['revaluation_reserve_status'] + values['revaluation_reserve_change'],
                'Uskladitev vrednosti (nerealizirani dobički, zmanjšanje na objektivno vrednost) pripoznana neposredno v BU': np.nan,
                'Neto tečajna razlika': values['fx_status'] + values['fx_change'],
                'Amortizacija popusta/premije na finančne instrumente z določeno zapadlostjo': values['amortization_status'] + values['amortization_change'],
                })
            
            template = pd.DataFrame(result)
            self.templates['UNREALIZED_PROFIT_ALL'] = self._add_total_row(template)

    def _generate_detailed_template(self, investment_type: str) -> pd.DataFrame:
        """Generate detailed template for a specific investment type"""
        gl_to_quarter = self.gl_status.loc[
            self.gl_status['VALUE'] == investment_type
        ]
        
        gl_quarter = self.gl_change.loc[
            (self.gl_change['VALUE'] == investment_type)
        ]
        
        gl_combined = self.gl_change.loc[
            (self.gl_change['VALUE'] == investment_type)
        ]
        
        tags = set(gl_combined['TAGS'].dropna().astype(str))        
        result = []
        
        for tag in sorted(tags):
            msrp = gl_combined.loc[gl_combined['TAGS'] == tag, 'MSRP_RAZVRSTITEV'].iloc[0]
            metoda_vrednotenja = gl_combined.loc[gl_combined['TAGS'] == tag, 'METODA_VREDNOTENJA'].iloc[0]
            metoda_vrednotenja_other = gl_combined.loc[gl_combined['TAGS'] == tag, 'METODA_VREDNOTELJA_VOL2'].iloc[0]
            vir_sredstev = gl_combined.loc[gl_combined['TAGS'] == tag, 'VIR_SREDSTEV'].iloc[0]
            
            values = {}
            
            for prefix, gl_data in [('status', gl_to_quarter), ('change', gl_quarter)]:
                for trans_type, trans_name in [
                    (TransactionType.ACCOUNTING_VALUE, 'buy_sell'),
                    (TransactionType.REVALUATION_EFFECT, 'revaluation'),
                    (TransactionType.REVALUATION_RESERVE, 'revaluation_reserve'),
                    (TransactionType.EXCHANGE_RATE_DIFFERENCE, 'fx'),
                    (TransactionType.AMORTIZATION, 'amortization')
                ]:
                    field = f"{trans_name}_{prefix}"
                    saldo_field = 'SALDO_STANJE' if prefix == 'status' else 'SALDO_SPREMEMBA'
                    
                    values[field] = gl_data.loc[
                        (gl_data['TAGS'] == tag) &
                        (gl_data['VRSTA_TRANSKACIJE_NERDZ'] == trans_type),
                        saldo_field
                    ].sum()
                    
                    if trans_name == 'revaluation_reserve' and prefix == 'status':
                        values[field] *= -1
            
            revaluation_effect = (values['revaluation_reserve_change'] + values['revaluation_change'])
            
            objective_value = (
                values['buy_sell_status'] + 
                values['buy_sell_change'] + 
                revaluation_effect +
                values['fx_status'] +
                values['fx_change'] +
                values['amortization_status'] +
                values['amortization_change']
                )
            
            result.append({
                'Tags': tag,
                'Razvrstitev v skladu z MSRP': msrp,
                'Metoda vrednotenja': metoda_vrednotenja,
                'Metoda vrednotenja (če je druga)': metoda_vrednotenja_other,
                'Datum zadnje ocene': self.data.REPORT_DATE,
                'Skupni nabavni stroški/računovodska vrednost': values['buy_sell_status'] + values['buy_sell_change'],
                'Objektivna vrednost na datum zadnje ocene': objective_value,
                'Učinek prevrednotenja': revaluation_effect,
                'Revalorizacijska rezerva (status)': values['revaluation_reserve_status'] + values['revaluation_reserve_change'],
                'Uskladitev vrednosti (nerealizirani dobički, zmanjšanje na objektivno vrednost) pripoznana neposredno v BU': np.nan,
                'Neto tečajna razlika': values['fx_status'] + values['fx_change'],
                'Amortizacija popusta/premije na finančne instrumente z določeno zapadlostjo': values['amortization_status'] + values['amortization_change'],
                'Vir sredstev': vir_sredstev   
            })
            
        return result
    
    def _generate_equity_template(self) -> None:
        """Generate template for equity investments unrealized profits"""
        equity_template = self._generate_detailed_template(InvestmentCategory.SHARES_IN_INVESTMENT_FUNDS)
        
        self.templates['NEREAL_PROFIT_DELNICE'] = pd.DataFrame(equity_template)
    
    def _generate_bonds_template(self) -> None:
        """Generate template for bond investments unrealized profits"""
        bonds_under_year = self._generate_detailed_template(InvestmentCategory.DEBT_SECURITIES_UNDER_ONE_YEAR)
        bonds_over_year = self._generate_detailed_template(InvestmentCategory.DEBT_SECURITIES_OVER_ONE_YEAR)
        
        self.templates['NEREAL_PROFIT_BONDS_UNDER_1Y'] = pd.DataFrame(bonds_under_year)
        self.templates['NEREAL_PROFIT_BONDS_OVER_1Y'] = pd.DataFrame(bonds_over_year)
        
        
class SupFinReportGenerator(ReportGenerator):
    def __init__(self, data_loader: DataLoader):
        super().__init__(data_loader)
        self.data = data_loader
        
    def generate_report(self):
        self._generate_navision_template()
        self._generate_loi_template()
        self._combine_nav_loi_data()
    
    def _generate_navision_template(self):
        nav = self.data.nav.copy()
        acc_to_zero = ['020300', '020380', '021307', '021387', '0213901']
        mapp = nav['KEY'].apply(lambda x: pd.Series(self.data.navision_map.get(x, {})))
        nav = pd.concat([nav, mapp], axis=1).mask(lambda x: x=='')
        
        nav['VREDNOST_PRIDOBITVE'] = nav['SALDO']
        nav['OBJEKTIVNA_VREDNOST'] = nav['SALDO']
        nav['RAČUNOVODSKA_VREDNOST'] = nav['SALDO']
        nav['RAČUNOVODSKA_VREDNOST_V_IZVIRNI_VALUTI'] = nav['SALDO']

        nav.loc[nav['NO_'].isin(acc_to_zero), 'VREDNOST_PRIDOBITVE'] = 0
        
        nav = nav.drop(columns=['NO_', 'NO_2', 'NAME', 'SALDO', 'KEY', 'KONTO_ST1', 'KONTO_ST2', 'OPIS']).dropna(subset=['VIR_SREDSTEV'])
        nav = nav.loc[:, ~nav.columns.duplicated()]
        
        self.templates['NAVISION_LOOKUP'] = nav[
            ['VIR_SREDSTEV',
             'ŠTEVILO_DELOVNIH_MEST_V_BS',
             'VRSTA_PODJETJA', 
             'PODVRSTA_PODJETJA',
             'GARANCIJA',
             'IME_ZALOŽNIKA_IZVAJALEC',
             'IME_ZALOŽNIKA_IZVAJALCA_ČE_JE_DRUGAČE',
             'SEKTOR',
             'ISIN',
             'LASTNOST',
             'KOLIČINA',
             'RAZVRSTITEV_V_SKLADU_Z_MSRP',
             'METODA_VREDNOTENJA',
             'ZALOŽNIKOVA_DRŽAVA_IZVAJALEC',
             'TRGOVINSKA_DEŽELA',
             'REGULIRANI_TRGOVALNI_TRG',
             'VIR_VREDNOTENJA',
             'VREDNOST_PRIDOBITVE',
             'AKUMULIRANE_OBRESTI',
             'AMORTIZIRANI_ODHODKI',
             'OBJEKTIVNA_VREDNOST',
             'RAČUNOVODSKA_VREDNOST',
             'RAČUNOVODSKA_VREDNOST_V_IZVIRNI_VALUTI',
             'VALUTA',
             'VRSTA_KUPONA',
             'POGOSTOST_KUPONOV',
             'OBRESTNA_MERA',
             'EFEKTIVNA_OBRESTNA_MERADONOS',
             'DATUM_NALOŽBE',
             'DATUM_ZAPADLOSTI',	
             'OCENE',
             'BONITETNA_AGENCIJA']
            ]     
        
    def _generate_loi_template(self):
        loi = self.data.loi.copy()
        loi['VALUTA'] = loi['QUOTATION_CURRENCY'].map(self.data.scd_map)
        loi['BONITETNA_AGENCIJA'] = loi['ISSUER_RATING_AGENCY_SECOND_BEST'].map(self.data.scd_map)   
        
        mapp = self.data.loi['SCD_ID'].apply(lambda x: pd.Series(self.data.scd_reg_report_map.get(x, {})))
        loi = pd.concat([loi, mapp], axis=1).mask(lambda x: x == '')
        
        loi.loc[loi['INVESTMENT_NAME'].str.contains('Stejšn'), 'KOLIČINA'] = 0
        loi.loc[loi['NOMINAL_VALUE_OF_LOT_QC'] == 100, 'KOLIČINA'] /= 100
        loi.loc[loi['INVESTMENT_TYPE'] != 'L_MEMBERS']
        loi = loi.loc[:, ~loi.columns.duplicated()].dropna(subset=['VIR_SREDSTEV'])

        self.templates['SIMCORP_LOOKUP'] = loi[
            ['VIR_SREDSTEV',
             'ŠTEVILO_DELOVNIH_MEST_V_BS',
             'VRSTA_PODJETJA', 
             'PODVRSTA_PODJETJA',
             'GARANCIJA',
             'IME_ZALOŽNIKA_IZVAJALEC',
             'IME_ZALOŽNIKA_IZVAJALCA_ČE_JE_DRUGAČE',
             'SEKTOR',
             'ISIN',
             'LASTNOST',
             'KOLIČINA',
             'RAZVRSTITEV_V_SKLADU_Z_MSRP',
             'METODA_VREDNOTENJA',
             'ZALOŽNIKOVA_DRŽAVA_IZVAJALEC',
             'TRGOVINSKA_DEŽELA',
             'REGULIRANI_TRGOVALNI_TRG',
             'VIR_VREDNOTENJA',
             'VREDNOST_PRIDOBITVE',
             'AKUMULIRANE_OBRESTI',
             'AMORTIZIRANI_ODHODKI',
             'OBJEKTIVNA_VREDNOST',
             'RAČUNOVODSKA_VREDNOST',
             'RAČUNOVODSKA_VREDNOST_V_IZVIRNI_VALUTI',
             'VALUTA',
             'VRSTA_KUPONA',
             'POGOSTOST_KUPONOV',
             'OBRESTNA_MERA',
             'EFEKTIVNA_OBRESTNA_MERADONOS',
             'DATUM_NALOŽBE',
             'DATUM_ZAPADLOSTI',	
             'OCENE',
             'BONITETNA_AGENCIJA']
            ] 
    
    def _combine_nav_loi_data(self):
        loi = self.templates['SIMCORP_LOOKUP']
        nav = self.templates['NAVISION_LOOKUP']
        
        self.templates['NAV_LOI_COMB_LOOKUP'] = pd.concat([nav, loi], ignore_index=False)
        
        
data_loader = DataLoader()
data_loader.load_data()

realized = RealizedProfitGenerator(data_loader)
unrealized = UnrealizedProfitReportGenerator(data_loader)
supfin = SupFinReportGenerator(data_loader)

realized.generate_report()
unrealized.generate_report()
supfin.generate_report()

generator = ReportGenerator(data_loader)
generator.templates = {**realized.templates, **unrealized.templates, **supfin.templates}
generator.save_report()

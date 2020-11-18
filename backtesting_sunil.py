#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 22:45:03 2018

@author: sunilguglani
"""
from nsepy import get_history

#from upstox_api.api import *
from pandas.io.json import json_normalize

from pprint import pprint
import datetime
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize 
from nsepy.derivatives import get_expiry_date
from nsepy.history import get_price_list
import numpy as np
from nsepy import get_history
import sqlite3
from TechnicalIndicator_Sunil import *
import matplotlib.pyplot as plt
import seaborn as sns
from numba import jit
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
usstocklist_sp500=[
'MMM',
'ABT',
'ABBV',
'ABMD',
'ACN',
'ATVI',
'ADBE',
'AMD',
'AAP',
'AES',
'AFL',
'A',
'APD',
'AKAM',
'ALK',
'ALB',
'ARE',
'ALXN',
'ALGN',
'ALLE',
'AGN',
'ADS',
'LNT',
'ALL',
'GOOGL',
'GOOG',
'MO',
'AMZN',
'AMCR',
'AEE',
'AAL',
'AEP',
'AXP',
'AIG',
'AMT',
'AWK',
'AMP',
'ABC',
'AME',
'AMGN',
'APH',
'ADI',
'ANSS',
'ANTM',
'AON',
'AOS',
'APA',
'AIV',
'AAPL',
'AMAT',
'APTV',
'ADM',
'ARNC',
'ANET',
'AJG',
'AIZ',
'ATO',
'T',
'ADSK',
'ADP',
'AZO',
'AVB',
'AVY',
'BKR',
'BLL',
'BAC',
'BK',
'BAX',
'BDX',
'BRK.B',
'BBY',
'BIIB',
'BLK',
'BA',
'BKNG',
'BWA',
'BXP',
'BSX',
'BMY',
'AVGO',
'BR',
'BF.B',
'CHRW',
'COG',
'CDNS',
'CPB',
'COF',
'CPRI',
'CAH',
'KMX',
'CCL',
'CAT',
'CBOE',
'CBRE',
'CDW',
'CE',
'CNC',
'CNP',
'CTL',
'CERN',
'CF',
'SCHW',
'CHTR',
'CVX',
'CMG',
'CB',
'CHD',
'CI',
'XEC',
'CINF',
'CTAS',
'CSCO',
'C',
'CFG',
'CTXS',
'CLX',
'CME',
'CMS',
'KO',
'CTSH',
'CL',
'CMCSA',
'CMA',
'CAG',
'CXO',
'COP',
'ED',
'STZ',
'COO',
'CPRT',
'GLW',
'CTVA',
'COST',
'COTY',
'CCI',
'CSX',
'CMI',
'CVS',
'DHI',
'DHR',
'DRI',
'DVA',
'DE',
'DAL',
'XRAY',
'DVN',
'FANG',
'DLR',
'DFS',
'DISCA',
'DISCK',
'DISH',
'DG',
'DLTR',
'D',
'DOV',
'DOW',
'DTE',
'DUK',
'DRE',
'DD',
'DXC',
'ETFC',
'EMN',
'ETN',
'EBAY',
'ECL',
'EIX',
'EW',
'EA',
'EMR',
'ETR',
'EOG',
'EFX',
'EQIX',
'EQR',
'ESS',
'EL',
'EVRG',
'ES',
'RE',
'EXC',
'EXPE',
'EXPD',
'EXR',
'XOM',
'FFIV',
'FB',
'FAST',
'FRT',
'FDX',
'FIS',
'FITB',
'FE',
'FRC',
'FISV',
'FLT',
'FLIR',
'FLS',
'FMC',
'F',
'FTNT',
'FTV',
'FBHS',
'FOXA',
'FOX',
'BEN',
'FCX',
'GPS',
'GRMN',
'IT',
'GD',
'GE',
'GIS',
'GM',
'GPC',
'GILD',
'GL',
'GPN',
'GS',
'GWW',
'HRB',
'HAL',
'HBI',
'HOG',
'HIG',
'HAS',
'HCA',
'PEAK',
'HP',
'HSIC',
'HSY',
'HES',
'HPE',
'HLT',
'HFC',
'HOLX',
'HD',
'HON',
'HRL',
'HST',
'HPQ',
'HUM',
'HBAN',
'HII',
'IEX',
'IDXX',
'INFO',
'ITW',
'ILMN',
'IR',
'INTC',
'ICE',
'IBM',
'INCY',
'IP',
'IPG',
'IFF',
'INTU',
'ISRG',
'IVZ',
'IPGP',
'IQV',
'IRM',
'JKHY',
'J',
'JBHT',
'SJM',
'JNJ',
'JCI',
'JPM',
'JNPR',
'KSU',
'K',
'KEY',
'KEYS',
'KMB',
'KIM',
'KMI',
'KLAC',
'KSS',
'KHC',
'KR',
'LB',
'LHX',
'LH',
'LRCX',
'LW',
'LVS',
'LEG',
'LDOS',
'LEN',
'LLY',
'LNC',
'LIN',
'LYV',
'LKQ',
'LMT',
'L',
'LOW',
'LYB',
'MTB',
'M',
'MRO',
'MPC',
'MKTX',
'MAR',
'MMC',
'MLM',
'MAS',
'MA',
'MKC',
'MXIM',
'MCD',
'MCK',
'MDT',
'MRK',
'MET',
'MTD',
'MGM',
'MCHP',
'MU',
'MSFT',
'MAA',
'MHK',
'TAP',
'MDLZ',
'MNST',
'MCO',
'MS',
'MOS',
'MSI',
'MSCI',
'MYL',
'NDAQ',
'NOV',
'NTAP',
'NFLX',
'NWL',
'NEM',
'NWSA',
'NWS',
'NEE',
'NLSN',
'NKE',
'NI',
'NBL',
'JWN',
'NSC',
'NTRS',
'NOC',
'NLOK',
'NCLH',
'NRG',
'NUE',
'NVDA',
'NVR',
'ORLY',
'OXY',
'ODFL',
'OMC',
'OKE',
'ORCL',
'PCAR',
'PKG',
'PH',
'PAYX',
'PYPL',
'PNR',
'PBCT',
'PEP',
'PKI',
'PRGO',
'PFE',
'PM',
'PSX',
'PNW',
'PXD',
'PNC',
'PPG',
'PPL',
'PFG',
'PG',
'PGR',
'PLD',
'PRU',
'PEG',
'PSA',
'PHM',
'PVH',
'QRVO',
'PWR',
'QCOM',
'DGX',
'RL',
'RJF',
'RTN',
'O',
'REG',
'REGN',
'RF',
'RSG',
'RMD',
'RHI',
'ROK',
'ROL',
'ROP',
'ROST',
'RCL',
'SPGI',
'CRM',
'SBAC',
'SLB',
'STX',
'SEE',
'SRE',
'NOW',
'SHW',
'SPG',
'SWKS',
'SLG',
'SNA',
'SO',
'LUV',
'SWK',
'SBUX',
'STT',
'STE',
'SYK',
'SIVB',
'SYF',
'SNPS',
'SYY',
'TMUS',
'TROW',
'TTWO',
'TPR',
'TGT',
'TEL',
'FTI',
'TFX',
'TXN',
'TXT',
'TMO',
'TIF',
'TJX',
'TSCO',
'TDG',
'TRV',
'TFC',
'TWTR',
'TSN',
'UDR',
'ULTA',
'USB',
'UAA',
'UA',
'UNP',
'UAL',
'UNH',
'UPS',
'URI',
'UTX',
'UHS',
'UNM',
'VFC',
'VLO',
'VAR',
'VTR',
'VRSN',
'VRSK',
'VZ',
'VRTX',
'VIAC',
'V',
'VNO',
'VMC',
'WRB',
'WAB',
'WMT',
'WBA',
'DIS',
'WM',
'WAT',
'WEC',
'WCG',
'WFC',
'WELL',
'WDC',
'WU',
'WRK',
'WY',
'WHR',
'WMB',
'WLTW',
'WYNN',
'XEL',
'XRX',
'XLNX',
'XYL',
'YUM',
'ZBRA',
'ZBH',
'ZION',
'ZTS']
'''
'''

lst_der=['ADANIENT',
'ADANIPORTS',
'ADANIPOWER',
'AJANTPHARM',
'ALBK',
'AMARAJABAT',
'AMBUJACEM',
'ANDHRABANK',
'APOLLOHOSP',
'APOLLOTYRE',
'ARVIND',
'ASHOKLEY',
'ASIANPAINT',
'AUROPHARMA',
'AXISBANK',
'BAJAJ-AUTO',
'BAJFINANCE',
'BAJAJFINSV',
'BALKRISIND',
'BALRAMCHIN',
'BANKBARODA',
'BANKINDIA',
'BATAINDIA',
'BEML',
'BERGEPAINT',
'BEL',
'BHARATFIN',
'BHARATFORG',
'BPCL',
'BHARTIARTL',
'INFRATEL',
'BHEL',
'BIOCON',
'BOSCHLTD',
'BRITANNIA',
'CADILAHC',
'CANFINHOME',
'CANBK',
'CAPF',
'CASTROLIND',
'CEATLTD',
'CENTURYTEX',
'CESC',
'CGPOWER',
'CHENNPETRO',
'CHOLAFIN',
'CIPLA',
'COALINDIA',
'COLPAL',
'CONCOR',
'CUMMINSIND',
'DABUR',
'DALMIABHA',
'DCBBANK',
'DHFL',
'DISHTV',
'DIVISLAB',
'DLF',
'DRREDDY',
'EICHERMOT',
'ENGINERSIN',
'EQUITAS',
'ESCORTS',
'EXIDEIND',
'FEDERALBNK',
'GAIL',
'GLENMARK',
'GMRINFRA',
'GODFRYPHLP',
'GODREJCP',
'GODREJIND',
'GRANULES',
'GRASIM',
'GSFC',
'HAVELLS',
'HCLTECH',
'HDFCBANK',
'HDFC',
'HEROMOTOCO',
'HEXAWARE',
'HINDALCO',
'HCC',
'HINDPETRO',
'HINDUNILVR',
'HINDZINC',
'ICICIBANK',
'ICICIPRULI',
'IDBI',
'IDFCBANK',
'IDFC',
'IFCI',
'IBULHSGFIN',
'INDIANB',
'IOC',
'IGL',
'INDUSINDBK',
'INFIBEAM',
'INFY',
'INDIGO',
'IRB',
'ITC',
'JISLJALEQS',
'JPASSOCIAT',
'JETAIRWAYS',
'JINDALSTEL',
'JSWSTEEL',
'JUBLFOOD',
'JUSTDIAL',
'KAJARIACER',
'KTKBANK',
'KSCL',
'KOTAKBANK',
'KPIT',
'L&TFH',
'LT',
'LICHSGFIN',
'LUPIN',
'M&MFIN',
'MGL',
'M&M',
'MANAPPURAM',
'MRPL',
'MARICO',
'MARUTI',
'MFSL',
'MINDTREE',
'MOTHERSUMI',
'MRF',
'MCX',
'MUTHOOTFIN',
'NATIONALUM',
'NBCC',
'NCC',
'NESTLEIND',
'NHPC',
'NIITTECH',
'NMDC',
'NTPC',
'ONGC',
'OIL',
'OFSS',
'ORIENTBANK',
'PAGEIND',
'PCJEWELLER',
'PETRONET',
'PIDILITIND',
'PEL',
'PFC',
'POWERGRID',
'PTC',
'PNB',
'PVR',
'RAYMOND',
'RBLBANK',
'RELCAPITAL',
'RCOM',
'RNAVAL',
'RELIANCE',
'RELINFRA',
'RPOWER',
'REPCOHOME',
'RECLTD',
'SHREECEM',
'SRTRANSFIN',
'SIEMENS',
'SREINFRA',
'SRF',
'SBIN',
'SAIL',
'STAR',
'SUNPHARMA',
'SUNTV',
'SUZLON',
'SYNDIBANK',
'TATACHEM',
'TATACOMM',
'TCS',
'TATAELXSI',
'TATAGLOBAL',
'TATAMTRDVR',
'TATAMOTORS',
'TATAPOWER',
'TATASTEEL',
'TECHM',
'INDIACEM',
'RAMCOCEM',
'SOUTHBANK',
'TITAN',
'TORNTPHARM',
'TORNTPOWER',
'TV18BRDCST',
'TVSMOTOR',
'UJJIVAN',
'ULTRACEMCO',
'UNIONBANK',
'UBL',
'MCDOWELL-N',
'UPL',
'VEDL',
'VGUARD',
'VOLTAS',
'WIPRO',
'WOCKPHARMA',
'YESBANK',
'ZEEL',
'NIFTY',
'BANKNIFTY'
 ]
lst_der=['BHEL',
'ITC',
'ADANIPORTS',
'POWERGRID',
'RBLBANK',
'CANFINHOME',
'HDFC',
'TATACOMM',
'HINDUNILVR',
'BOSCHLTD',
'BHARTIARTL',
'MFSL',
'KTKBANK',
'DIVISLAB',
'CIPLA',
'MARUTI',
'M&MFIN',
'BATAINDIA',
'TATAMTRDVR',
'INDIGO',
'ALBK',
'IBULHSGFIN',
'LUPIN',
'DALMIABHA',
'DABUR',
'NHPC',
'INDUSINDBK',
'CUMMINSIND',
'LICHSGFIN',
'IRB',
'KOTAKBANK',
'RELIANCE',
'MCDOWELL-N',
'TORNTPHARM',
'HCC',
'HEXAWARE',
'TV18BRDCST',
'IDFCBANK',
'ONGC',
'TCS',
'HAVELLS',
'UNIONBANK',
'NTPC',
'ZEEL',
'ADANIPOWER',
'ULTRACEMCO',
'HINDZINC',
'TATAPOWER',
'CESC',
'JPASSOCIAT',
'BALRAMCHIN',
'UJJIVAN',
'GODREJCP',
'HDFCBANK',
'SREINFRA',
'TVSMOTOR',
'BAJAJ-AUTO',
'NMDC',
'BHARATFORG',
'IDFC',
'KAJARIACER',
'OFSS',
'INFY',
'GRASIM',
'HCLTECH',
'BRITANNIA',
'ASHOKLEY',
'SOUTHBANK',
'EICHERMOT',
'AXISBANK',
'INDIACEM',
'CEATLTD',
'TATAMOTORS',
'SUNTV',
'MRPL',
'SUNPHARMA',
'GLENMARK',
'HEROMOTOCO',
'RAYMOND',
'AJANTPHARM',
'TATASTEEL',
'RAMCOCEM',
'CAPF',
'TECHM',
'ENGINERSIN',
'ICICIBANK',
'APOLLOTYRE',
'UPL',
'PETRONET',
'DLF',
'JINDALSTEL',
'FEDERALBNK',
'BAJFINANCE',
'TORNTPOWER',
'IDBI',
'BEML',
'EXIDEIND',
'ORIENTBANK',
'HINDALCO',
'MRF',
'AMARAJABAT',
'DISHTV',
'M&M',
'CASTROLIND',
'LT',
'SBIN',
'COALINDIA',
'COLPAL',
'GSFC',
'IFCI',
'CANBK',
'MARICO',
'GRANULES',
'KPIT',
'SYNDIBANK',
'PFC',
'NATIONALUM',
'NCC',
'PNB',
'MOTHERSUMI',
'PTC',
'OIL',
'CENTURYTEX',
'DHFL',
'CONCOR',
'BPCL',
'INFRATEL',
'BEL',
'GMRINFRA',
'EQUITAS',
'GAIL',
'IOC',
'BANKINDIA',
'AUROPHARMA',
'MCX',
'INDIANB',
'CADILAHC',
'RECLTD',
'NBCC',
'ANDHRABANK',
'HINDPETRO']
'''
#lst_der=['ADANIPOWER','ADANIENT','ADANIPORTS']
lst_der=['RELIANCE','ITC','HDFC','HDFCBANK','LT','SBIN']
lst_der=['BAJFINANCE']
'''
#lst_der=['RELIANCE','ITC','HDFC','HDFCBANK','LT','SBIN','ADANIENT','ADANIPORTS']

usstocklist_sp500=[
'MMM',
'ABT',
'ABBV',
'ABMD',
'ACN',
'ATVI',
'ADBE',
'AMD',
'AAP',
'AES',
'AFL',
'A',
'APD',
'AKAM',
'ALK',
'ALB',
'ARE',
'ALXN',
'ALGN',
'ALLE',
'AGN',
'ADS',
'LNT',
'ALL',
'GOOGL',
'GOOG',
'MO',
'AMZN',
'AMCR',
'AEE',
'AAL',
'AEP',
'AXP',
'AIG',
'AMT',
'AWK',
'AMP',
'ABC',
'AME',
'AMGN',
'APH',
'ADI',
'ANSS',
'ANTM',
'AON',
'AOS',
'APA',
'AIV',
'AAPL',
'AMAT',
'APTV',
'ADM',
'ARNC',
'ANET',
'AJG',
'AIZ',
'ATO',
'T',
'ADSK',
'ADP',
'AZO',
'AVB',
'AVY',
'BKR',
'BLL',
'BAC',
'BK',
'BAX',
'BDX',
'BRK.B',
'BBY',
'BIIB',
'BLK',
'BA',
'BKNG',
'BWA',
'BXP',
'BSX',
'BMY',
'AVGO',
'BR',
'BF.B',
'CHRW',
'COG',
'CDNS',
'CPB',
'COF',
'CPRI',
'CAH',
'KMX',
'CCL',
'CAT',
'CBOE',
'CBRE',
'CDW',
'CE',
'CNC',
'CNP',
'CTL',
'CERN',
'CF',
'SCHW',
'CHTR',
'CVX',
'CMG',
'CB',
'CHD',
'CI',
'XEC',
'CINF',
'CTAS',
'CSCO',
'C',
'CFG',
'CTXS',
'CLX',
'CME',
'CMS',
'KO',
'CTSH',
'CL',
'CMCSA',
'CMA',
'CAG',
'CXO',
'COP',
'ED',
'STZ',
'COO',
'CPRT',
'GLW',
'CTVA',
'COST',
'COTY',
'CCI',
'CSX',
'CMI',
'CVS',
'DHI',
'DHR',
'DRI',
'DVA',
'DE',
'DAL',
'XRAY',
'DVN',
'FANG',
'DLR',
'DFS',
'DISCA',
'DISCK',
'DISH',
'DG',
'DLTR',
'D',
'DOV',
'DOW',
'DTE',
'DUK',
'DRE',
'DD',
'DXC',
'ETFC',
'EMN',
'ETN',
'EBAY',
'ECL',
'EIX',
'EW',
'EA',
'EMR',
'ETR',
'EOG',
'EFX',
'EQIX',
'EQR',
'ESS',
'EL',
'EVRG',
'ES',
'RE',
'EXC',
'EXPE',
'EXPD',
'EXR',
'XOM',
'FFIV',
'FB',
'FAST',
'FRT',
'FDX',
'FIS',
'FITB',
'FE',
'FRC',
'FISV',
'FLT',
'FLIR',
'FLS',
'FMC',
'F',
'FTNT',
'FTV',
'FBHS',
'FOXA',
'FOX',
'BEN',
'FCX',
'GPS',
'GRMN',
'IT',
'GD',
'GE',
'GIS',
'GM',
'GPC',
'GILD',
'GL',
'GPN',
'GS',
'GWW',
'HRB',
'HAL',
'HBI',
'HOG',
'HIG',
'HAS',
'HCA',
'PEAK',
'HP',
'HSIC',
'HSY',
'HES',
'HPE',
'HLT',
'HFC',
'HOLX',
'HD',
'HON',
'HRL',
'HST',
'HPQ',
'HUM',
'HBAN',
'HII',
'IEX',
'IDXX',
'INFO',
'ITW',
'ILMN',
'IR',
'INTC',
'ICE',
'IBM',
'INCY',
'IP',
'IPG',
'IFF',
'INTU',
'ISRG',
'IVZ',
'IPGP',
'IQV',
'IRM',
'JKHY',
'J',
'JBHT',
'SJM',
'JNJ',
'JCI',
'JPM',
'JNPR',
'KSU',
'K',
'KEY',
'KEYS',
'KMB',
'KIM',
'KMI',
'KLAC',
'KSS',
'KHC',
'KR',
'LB',
'LHX',
'LH',
'LRCX',
'LW',
'LVS',
'LEG',
'LDOS',
'LEN',
'LLY',
'LNC',
'LIN',
'LYV',
'LKQ',
'LMT',
'L',
'LOW',
'LYB',
'MTB',
'M',
'MRO',
'MPC',
'MKTX',
'MAR',
'MMC',
'MLM',
'MAS',
'MA',
'MKC',
'MXIM',
'MCD',
'MCK',
'MDT',
'MRK',
'MET',
'MTD',
'MGM',
'MCHP',
'MU',
'MSFT',
'MAA',
'MHK',
'TAP',
'MDLZ',
'MNST',
'MCO',
'MS',
'MOS',
'MSI',
'MSCI',
'MYL',
'NDAQ',
'NOV',
'NTAP',
'NFLX',
'NWL',
'NEM',
'NWSA',
'NWS',
'NEE',
'NLSN',
'NKE',
'NI',
'NBL',
'JWN',
'NSC',
'NTRS',
'NOC',
'NLOK',
'NCLH',
'NRG',
'NUE',
'NVDA',
'NVR',
'ORLY',
'OXY',
'ODFL',
'OMC',
'OKE',
'ORCL',
'PCAR',
'PKG',
'PH',
'PAYX',
'PYPL',
'PNR',
'PBCT',
'PEP',
'PKI',
'PRGO',
'PFE',
'PM',
'PSX',
'PNW',
'PXD',
'PNC',
'PPG',
'PPL',
'PFG',
'PG',
'PGR',
'PLD',
'PRU',
'PEG',
'PSA',
'PHM',
'PVH',
'QRVO',
'PWR',
'QCOM',
'DGX',
'RL',
'RJF',
'RTN',
'O',
'REG',
'REGN',
'RF',
'RSG',
'RMD',
'RHI',
'ROK',
'ROL',
'ROP',
'ROST',
'RCL',
'SPGI',
'CRM',
'SBAC',
'SLB',
'STX',
'SEE',
'SRE',
'NOW',
'SHW',
'SPG',
'SWKS',
'SLG',
'SNA',
'SO',
'LUV',
'SWK',
'SBUX',
'STT',
'STE',
'SYK',
'SIVB',
'SYF',
'SNPS',
'SYY',
'TMUS',
'TROW',
'TTWO',
'TPR',
'TGT',
'TEL',
'FTI',
'TFX',
'TXN',
'TXT',
'TMO',
'TIF',
'TJX',
'TSCO',
'TDG',
'TRV',
'TFC',
'TWTR',
'TSN',
'UDR',
'ULTA',
'USB',
'UAA',
'UA',
'UNP',
'UAL',
'UNH',
'UPS',
'URI',
'UTX',
'UHS',
'UNM',
'VFC',
'VLO',
'VAR',
'VTR',
'VRSN',
'VRSK',
'VZ',
'VRTX',
'VIAC',
'V',
'VNO',
'VMC',
'WRB',
'WAB',
'WMT',
'WBA',
'DIS',
'WM',
'WAT',
'WEC',
'WCG',
'WFC',
'WELL',
'WDC',
'WU',
'WRK',
'WY',
'WHR',
'WMB',
'WLTW',
'WYNN',
'XEL',
'XRX',
'XLNX',
'XYL',
'YUM',
'ZBRA',
'ZBH',
'ZION',
'ZTS']

import sys
global s,u
'''
todate=datetime.datetime.now().date()
fromdate=todate-datetime.timedelta(days=180)
script='COALINDIA'
script_cat='NSE_EQ'
frequency=OHLCInterval.Minute_1

error,df=Fetch_Historical_Data_upstox('',script,script_cat,fromdate,todate,OHLCInterval.Minute_1)
'''
def resample_df2(df,targ_time_interval):

    df2=df.resample(targ_time_interval).agg({'Open': 'first', 
                                 'High': 'max', 
                                 'Low': 'min', 
                                 'Close': 'last',
                                 'Volume': 'sum'})
    df2.dropna(inplace=True,axis=0)
    return df2

def resample_df(df,targ_time_interval):
    df_temp= first_letter_upper(df)
    
    df_open=(df_temp['Open'].resample(targ_time_interval).ohlc())['open']
    df_high=(df_temp['High'].resample(targ_time_interval).max())
    df_low=(df_temp['Low'].resample(targ_time_interval).min())
    df_close=(df_temp['Close'].resample(targ_time_interval).ohlc())['close']
    df_volume=(df_temp['Volume'].resample(targ_time_interval).sum())
    #*****************************************************************************
    
    df=pd.DataFrame()
    df['Low'] = df_low
    df['High'] = df_high
    df['Close'] = df_close
    df['Open'] = df_open
    df['Volume'] = df_volume

    return df

#Create a Session object with your api_key, redirect_uri and api_secret
def connect():
    global s,u
    your_api_key='57PaplLiai9aHETMP8niI1JoiLQ4saMd6HAQJGnB'
    #your_redirect_uri='https://developer.upstox.com/'
    your_redirect_uri='http://127.0.0/'
    your_api_secret='cauqv2p2k1'
    
    s = Session (your_api_key)
    s.set_redirect_uri (your_redirect_uri)
    s.set_api_secret (your_api_secret)
    #Get the login URL so you can login with your Upstox UCC ID and password.
    print (s.get_login_url())
    ## this will return a URL such as https://api.upstox.com/index/dialog/authorize?apiKey={your_api_key}&redirect_uri={your_redirect_uri}&response_type=code
    #Login to the URL and set the code returned by the login response in your Session object
    s.set_code ('4b85573f0dc01511acc2af8eb0d705658eef7acb')
    #Retrieve your access token
    access_token = s.retrieve_access_token()
    print ('Received access_token: %s' % access_token)
    
    u = Upstox (your_api_key, access_token)
    
    u.get_master_contract('NSE_EQ') # get contracts for NSE EQ
    u.get_master_contract('NSE_FO') # get contracts for NSE FO
    u.get_master_contract('NSE_INDEX') # get contracts for NSE INDEX
    u.get_master_contract('BSE_EQ') # get contracts for BSE EQ
    u.get_master_contract('BSE_INDEX') # get contracts for BSE INDEX
    u.get_master_contract('MCX_INDEX') # get contracts for MCX INDEX
    u.get_master_contract('MCX_FO') # get contracts for MCX FO
    NCD=u.get_master_contract('NCD_FO') # get contracts for MCX FO
    dfncd=json_normalize(NCD)
    dfncd=dfncd.T


'''
def backtest_strategy(df, strategy_type,lst_entry_criteria,lst_exit_criteria, positional_field,price_field):
    df['buy_price']=0
    df['sell_price']=0
  
    df['buy_time']=''
    df['sell_time']=''
    
    df[positional_field]=0
    pos=0
    
    for d in range(0,len(df)):
        entry=lst_entry_criteria[d]
        exit=lst_exit_criteria[d]
        if entry&(pos==0) :
             if strategy_type=='long':
                df['buy_price'].iat[d]= df[price_field].iloc[d]
                df[positional_field].iat[d]=1
             elif strategy_type=='short':
                df['sell_price'].iat[d]= df[price_field].iloc[d]
                df[positional_field].iat[d]=-1
             pos=1
        elif exit&(pos==1) :
             if strategy_type=='long':
                df['sell_price'].iat[d]= df[price_field].iloc[d]
                df[positional_field].iat[d]=-1
             elif strategy_type=='short':
                df['buy_price'].iat[d]= df[price_field].iloc[d]
                df[positional_field].iat[d]=1
             pos=0

    df_temp=df[df[positional_field]!=0].copy()
    
    if strategy_type=='short' :
        df_temp['sell_time']=df_temp.index
        df_temp['buy_time']=df_temp.index
        df_temp['buy_time']=df_temp['buy_time'].shift(-1)
        df_temp['buy_price']=df_temp['buy_price'].shift(-1)

    if strategy_type=='long' :
        df_temp['buy_time']=df_temp.index
        df_temp['sell_time']=df_temp.index
        df_temp['sell_time']=df_temp['sell_time'].shift(-1)
        df_temp['sell_price']=df_temp['sell_price'].shift(-1)
    
    df_summary=df_temp[(df_temp.buy_price>0)&(df_temp.sell_price>0)][['buy_price','buy_time','sell_price','sell_time']].copy()
    df_summary['returns']=(df_summary['sell_price']-df_summary['buy_price'])*100/df_summary['buy_price']
    df_summary['cum_returns']=(df_summary['sell_price']/df_summary['buy_price']).cumprod()
    df_summary['cum_returns']=df_summary['cum_returns']
    df_summary['investment_period']=(df_summary['sell_time']-df_summary['buy_time'])
    
    return df,df_summary
'''


def Fetch_Historical_Data_Futures_nsepy(script,script_cat,fromdate,todate,mode,expiry_date):
    global s,u
    if mode=='nsepy':
        if (script_cat=='NSE_EQ')|(script_cat=='BSE_EQ'):
            try:
                df_index_data = get_history(symbol=script, 
                                        start=fromdate, 
                                        end=todate,
                                        futures=True, expiry_date=expiry_date)
            except (AttributeError,RuntimeError) as err:
                print ("Fetch_Historical_Data function, nsepy function: ",err)
                df_index_data = get_history(symbol=script, 
                                start=fromdate, 
                                end=todate,
                                futures=True, expiry_date=expiry_date)
            except (ConnectionError) as err:
                #print ("Fetch_Historical_Data function, nsepy function: ",err)
                df_index_data = get_history(symbol=script, 
                                start=fromdate, 
                                end=todate,
                                futures=True, expiry_date=expiry_date)
            

            return df_index_data
            
        elif script_cat=='NSE_INDEX':
            try:
                df_index_data = get_history(symbol=script, 
                                        start=fromdate, 
                                        end=todate,
                                        index=True,
                                        futures=True, expiry_date=expiry_date)
            except (AttributeError,RuntimeError) as err:
                print ("Fetch_Historical_Data function, nsepy function: ",err)
                df_index_data = get_history(symbol=script, 
                                start=fromdate, 
                                end=todate,
                                index=True,
                                futures=True, expiry_date=expiry_date)
            except (ConnectionError) as err:
                #print ("Fetch_Historical_Data function, nsepy function: ",err)
                df_index_data = get_history(symbol=script, 
                                start=fromdate, 
                                end=todate,
                                index=True,
                                futures=True, expiry_date=expiry_date)

            return df_index_data

def Fetch_Historical_Data_Options_nsepy(script,script_cat,fromdate,todate,mode,expiry_date,p_option_type,p_strike_price):
    global s,u
    
    if mode=='' or mode is None:    
        mode='nsepy'

    if (script_cat=='')|(script_cat is None):
        script_cat='NSE_INDEX'
    
    if mode=='nsepy':
        if (script_cat=='NSE_EQ'):
            try:
                df_index_data = get_history(symbol=script, 
                                        start=fromdate, 
                                        end=todate,
                                        option_type=p_option_type,
                                        strike_price=p_strike_price,
                                        expiry_date=expiry_date)

            except (AttributeError,RuntimeError) as err:
                print ("Fetch_Historical_Data function, nsepy function: ",err)
                df_index_data = get_history(symbol=script, 
                                        start=fromdate, 
                                        end=todate,
                                        option_type=p_option_type,
                                        strike_price=p_strike_price,
                                        expiry_date=expiry_date)
            except (ConnectionError) as err:
                #print ("Fetch_Historical_Data function, nsepy function: ",err)
                df_index_data = get_history(symbol=script, 
                                        start=fromdate, 
                                        end=todate,
                                        option_type=p_option_type,
                                        strike_price=p_strike_price,
                                        expiry_date=expiry_date)
            

            return df_index_data
            
        elif script_cat=='NSE_INDEX':
            try:
                df_index_data = get_history(symbol=script, 
                                        start=fromdate, 
                                        end=todate,
                                        index=True,
                                        option_type=p_option_type,
                                        strike_price=p_strike_price,
                                        expiry_date=expiry_date)
            except (AttributeError,RuntimeError) as err:
                print ("Fetch_Historical_Data function, nsepy function: ",err)
                df_index_data = get_history(symbol=script, 
                                        start=fromdate, 
                                        end=todate,
                                        index=True,
                                        option_type=p_option_type,
                                        strike_price=p_strike_price,
                                        expiry_date=expiry_date)
            except (ConnectionError) as err:
                #print ("Fetch_Historical_Data function, nsepy function: ",err)
                df_index_data = get_history(symbol=script, 
                                        start=fromdate, 
                                        end=todate,
                                        index=True,
                                        option_type=p_option_type,
                                        strike_price=p_strike_price,
                                        expiry_date=expiry_date)
            return df_index_data

'''
nifty_opt = get_history(symbol="NIFTY",
start=date(2015,1,1),
end=date(2015,1,10),
index=True,
option_type='CE',
strike_price=8200,
expiry_date=date(2015,1,29))
'''
def Fetch_Historical_Data_nsepy(script,script_cat,fromdate,todate,mode):
    if mode=='nsepy':
        if (script_cat=='NSE_EQ')|(script_cat=='BSE_EQ'):
            try:
                df_stock_data = get_history(symbol=script, 
                                    start=fromdate, 
                                    end=todate)
            
            except (RuntimeError,ConnectionError):
                print ("Fetch_Historical_Data function, nsepy function: ",RuntimeError)
                df_stock_data = get_history(symbol=script, 
                                    start=fromdate, 
                                    end=todate)
            return df_stock_data
            
        elif script_cat=='NSE_INDEX':
            try:
                df_index_data = get_history(symbol=script, 
                                    start=fromdate, 
                                    end=todate,
                					index=True)
            
            except (RuntimeError,ConnectionError):
                print ("Fetch_Historical_Data function, nsepy function: ",RuntimeError)
                df_index_data = get_history(symbol=script, 
                                    start=fromdate, 
                                    end=todate,
                					index=True)
            return df_index_data


def Fetch_Historical_Data_upstox(instrument,script,script_cat,fromdate,todate,frequency):
    error_in_fetching=False
    hist_data=None
    
    if str(instrument)=='':
        try:
            instrument=u.get_instrument_by_symbol(script_cat, script)
        except TypeError as se:
            if script_cat=='NSE_EQ':
                instrument=u.get_instrument_by_symbol('NSE_EQ', script)
            elif script_cat=='BSE_EQ':
                instrument=u.get_instrument_by_symbol('NSE_EQ', script)
        except (RuntimeError,ConnectionError):
            print('error in fetching instrument for script', script, RuntimeError, ConnectionError)
            error_in_fetching=True

    try:
        hist_data=u.get_ohlc(instrument, frequency, fromdate,todate )
    except TypeError as se:
        error_in_fetching=True
    except:
        error_in_fetching=True
        print('error in fetching historical data for script', script)
    
    if (hist_data is not None):
        df=json_normalize(hist_data)
        df['Date']=(df['timestamp']/1000).apply(datetime.datetime.fromtimestamp).dt.strftime('%Y-%m-%d')
        df['timestamp']=(df['timestamp']/1000).apply(datetime.datetime.fromtimestamp).dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        error_in_fetching=True
    
    return error_in_fetching,df 
    
def Fetch_live_feed_upstox(instrument,feedtype='Full'):
    
    error_in_fetching=False
    
    try:
        if str(feedtype).upper()=='FULL':
            live_quotes=u.get_live_feed(instrument, LiveFeedType.Full)
        elif str(feedtype).upper()=='LTP':   
            live_quotes=u.get_live_feed(instrument, LiveFeedType.LTP)
    except ValueError as ssl:
        print("error in fetching live data",ssl)
        error_in_fetching=True
        pass
    except requests.HTTPError as e:
        print('there was an error [%s]. Let''s start over\n\n' % e)
        error_in_fetching=True
    except:
        error_in_fetching=True
        
    if (live_quotes is not None)&(live_quotes!=''):
        df=json_normalize(live_quotes)
    else:
        error_in_fetching=True
    
    return error_in_fetching,df

    
def Connect_db(dbfile='algo_trading_framework.db'):
    global dbconn
    path_index="/Users/sunilguglani/Documents/Framework/"
    dbconn=sqlite3.connect(path_index+"/Database/"+dbfile)
    dbconn=sqlite3.connect('/Users/sunilguglani/Documents/Framework/Database/algo_trading_framework.db')
    return dbconn


def Fetch_Historical_Data_db(tablename):
    Connect_db()
    stock_query="SELECT * from "+tablename
    df =pd.read_sql_query(stock_query,dbconn)

    return df

def stochastic_setup(df,strategy_type):
    df_temp=df
    
    #df_temp.dropna(inplace=True)
    
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['%K']>df_temp['%D'])&(df_temp['%K'].shift(1)<=df_temp['%D'].shift(1))&(df_temp['%D']>23)&(df_temp['%D']<80)
        lst_exit_criteria= ((df_temp['%K']<df_temp['%D'])&(df_temp['%K'].shift(1)>=df_temp['%D'].shift(1))&((df_temp['%D'].shift(1)>=80)&(df_temp['%D']<=80)))
    
    if str(strategy_type).upper()=='SHORT':
        
        lst_exit_criteria=((df_temp['%K']>=df_temp['%D'])&(df_temp['%K'].shift(1)<=df_temp['%D'].shift(1)))
        lst_entry_criteria= ((df_temp['%K']<=df_temp['%D'])&(df_temp['%K'].shift(1)>=df_temp['%D'].shift(1))&((df_temp['%D']<=80)&(df_temp['%K']<=80)))
        '''
        lst_entry_criteria= ((df_temp['%K']<df_temp['%D'])&(df_temp['%K'].shift(1)>=df_temp['%D'].shift(1))&((df_temp['%D']<=80)&(df_temp['%K']<=80)))
        lst_exit_criteria=((df_temp['%K']>df_temp['%D'])&(df_temp['%K'].shift(1)<=df_temp['%D'].shift(1)) &((df_temp['%D'].shift(1)<20)&(df_temp['%D']>20)))
        '''

    return lst_entry_criteria,lst_exit_criteria

def stochastic_setup_v2(df,strategy_type):
    df_temp=df
    
    #df_temp.dropna(inplace=True)
    
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['%K']>df_temp['%D'])&(df_temp['%D']>23)&(df_temp['%D']<80)
        lst_exit_criteria= ((df_temp['%K']<df_temp['%D'])&((df_temp['%D'].shift(1)>=80)&(df_temp['%D']<=80)))
    
    if str(strategy_type).upper()=='SHORT':
        
        lst_exit_criteria=((df_temp['%K']>=df_temp['%D'])&(df_temp['%K'].shift(1)<=df_temp['%D'].shift(1)))
        lst_entry_criteria= ((df_temp['%K']<=df_temp['%D'])&(df_temp['%K'].shift(1)>=df_temp['%D'].shift(1))&((df_temp['%D']<=80)&(df_temp['%K']<=80)))
        '''
        lst_entry_criteria= ((df_temp['%K']<df_temp['%D'])&(df_temp['%K'].shift(1)>=df_temp['%D'].shift(1))&((df_temp['%D']<=80)&(df_temp['%K']<=80)))
        lst_exit_criteria=((df_temp['%K']>df_temp['%D'])&(df_temp['%K'].shift(1)<=df_temp['%D'].shift(1)) &((df_temp['%D'].shift(1)<20)&(df_temp['%D']>20)))
        '''

    return lst_entry_criteria,lst_exit_criteria


def stochastic_setup_v3(df,strategy_type):
    df_temp=df
    
    #df_temp.dropna(inplace=True)
    
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['%K']>df_temp['%D'])
        lst_exit_criteria= (df_temp['%K']<df_temp['%D'])
    
    if str(strategy_type).upper()=='SHORT':
        
        lst_exit_criteria=((df_temp['%K']>=df_temp['%D'])&(df_temp['%K'].shift(1)<=df_temp['%D'].shift(1)))
        lst_entry_criteria= ((df_temp['%K']<=df_temp['%D'])&(df_temp['%K'].shift(1)>=df_temp['%D'].shift(1))&((df_temp['%D']<=80)&(df_temp['%K']<=80)))
        '''
        lst_entry_criteria= ((df_temp['%K']<df_temp['%D'])&(df_temp['%K'].shift(1)>=df_temp['%D'].shift(1))&((df_temp['%D']<=80)&(df_temp['%K']<=80)))
        lst_exit_criteria=((df_temp['%K']>df_temp['%D'])&(df_temp['%K'].shift(1)<=df_temp['%D'].shift(1)) &((df_temp['%D'].shift(1)<20)&(df_temp['%D']>20)))
        '''

    return lst_entry_criteria,lst_exit_criteria


def candle_setup(df,strategy_type):
    df_temp=df
        
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['candle_score']>0)&(df_temp['candle_cumsum']>0)
        lst_exit_criteria= (df_temp['candle_score']<0)&(df_temp['candle_cumsum']>0)
    
    if str(strategy_type).upper()=='SHORT':        
        lst_entry_criteria= (df_temp['candle_score']<0)&(df_temp['candle_cumsum']>0)
        lst_exit_criteria=(df_temp['candle_score']>0)&(df_temp['candle_cumsum']>0)

    return lst_entry_criteria,lst_exit_criteria


def HA_candle_setup(df,strategy_type):
    df_temp=df
        
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['HA_candle_score']>0)
        lst_exit_criteria= (df_temp['HA_candle_score']<0)
    
    if str(strategy_type).upper()=='SHORT':        
        lst_entry_criteria= (df_temp['HA_candle_score']<0)
        lst_exit_criteria=(df_temp['HA_candle_score']>0)

    return lst_entry_criteria,lst_exit_criteria

def supertrend_setup(df,strategy_type):
    df_temp=df
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['Close']>df_temp['ST_7_3'])&(df_temp['Close'].shift(1)<=df_temp['ST_7_3'].shift(1))
        lst_exit_criteria=(df_temp['Close']<df_temp['ST_7_3'])&(df_temp['Close'].shift(1)>=df_temp['ST_7_3'].shift(1))
    
    if str(strategy_type).upper()=='SHORT':        
        lst_entry_criteria= (df_temp['Close']<df_temp['ST_7_3'])&(df_temp['Close'].shift(1)>=df_temp['ST_7_3'].shift(1))
        lst_exit_criteria=(df_temp['Close']>df_temp['ST_7_3'])&(df_temp['Close'].shift(1)<=df_temp['ST_7_3'].shift(1))

    return lst_entry_criteria,lst_exit_criteria


def ema_crossover_setup(df,strategy_type):
    df_temp=df
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['Ewm_short']>df_temp['Ewm_long'])
        lst_exit_criteria=(df_temp['Ewm_short']<=df_temp['Ewm_long']) 
    
    if str(strategy_type).upper()=='SHORT':        
        lst_entry_criteria= (df_temp['Ewm_short']<df_temp['Ewm_long']) 
        lst_exit_criteria=(df_temp['Ewm_short']>=df_temp['Ewm_long'])

    return lst_entry_criteria,lst_exit_criteria

def pivotpoint_setup_3(df,strategy_type):
    df_temp=df
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['S2']>df_temp['Low'])
        lst_exit_criteria=(df_temp['R2']<df_temp['High']) 
    
    if str(strategy_type).upper()=='SHORT':        
        lst_entry_criteria= (df_temp['R2']<df_temp['High']) 
        lst_exit_criteria=(df_temp['S2']>df_temp['Low'])

    return lst_entry_criteria,lst_exit_criteria


def pivotpoint_setup_2(df,strategy_type):
    df_temp=df
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['S1']>df_temp['Low'])
        lst_exit_criteria=(df_temp['R1']<df_temp['High']) 
    
    if str(strategy_type).upper()=='SHORT':        
        lst_entry_criteria= (df_temp['R1']<df_temp['High']) 
        lst_exit_criteria=(df_temp['S1']>df_temp['Low'])

    return lst_entry_criteria,lst_exit_criteria

def pivotpoint_setup_1(df,strategy_type):
    df_temp=df
    if str(strategy_type).upper()=='LONG':
        lst_entry_criteria=(df_temp['S1']>df_temp['Low'])
        lst_exit_criteria=(df_temp['PP']<df_temp['High']) 
    
    if str(strategy_type).upper()=='SHORT':        
        lst_entry_criteria= (df_temp['R1']<df_temp['High']) 
        lst_exit_criteria=(df_temp['S1']>df_temp['Low'])

    return lst_entry_criteria,lst_exit_criteria

def backtest_strategy_stoploss(df, strategy_type,lst_entry_criteria,lst_exit_criteria, positional_field,price_field,stoploss_pct,target_pct,only_profit):
    df['buy_price']=0.0
    df['sell_price']=0.0
  
    df['buy_time']=None
    df['sell_time']=None
    exit_reason_field=positional_field+'_exit_flag'
    df[positional_field]=0
    df[exit_reason_field]=''
    pos=0

    last_buy_price=0
    last_sell_price=0
    
    for d in range(0,len(df)):
        entry_flag=lst_entry_criteria[d]
        exit_flag=lst_exit_criteria[d]
        curr_price=df[price_field].iloc[d]
        stoploss_exit=False
        target_exit=False
        only_profit_exit=False
        exit_reason=''
        
        if stoploss_pct!=0:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)<stoploss_pct:
                    stoploss_exit=True
                    exit_reason='SLM'
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)<stoploss_pct:
                    stoploss_exit=True
                    exit_reason='SLM'
                    
        if target_pct!=0:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)>target_pct:
                    target_exit=True
                    exit_reason='TRM'
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)>target_pct:
                    target_exit=True
                    exit_reason='TRM'

        if only_profit==True:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)>0:
                    only_profit_exit=True
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)>0:
                    only_profit_exit=True
        else:
            only_profit_exit=True

        if exit_flag:
            exit_reason='ECM'
            
        if entry_flag&(pos==0) :
             if strategy_type=='long':
                df['buy_price'].iat[d]= df[price_field].iloc[d]
                last_buy_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=1
             elif strategy_type=='short':
                df['sell_price'].iat[d]= df[price_field].iloc[d]
                last_sell_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=-1
             pos=1
        
        #if (exit_flag|stoploss_exit|target_exit)& only_profit_exit & (pos==1) :
        elif (exit_flag|stoploss_exit|target_exit)& only_profit_exit & (pos==1) :
             df[exit_reason_field].iat[d]=exit_reason
             
             if strategy_type=='long':
                df['sell_price'].iat[d]= df[price_field].iloc[d]
                last_sell_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=-1
             elif strategy_type=='short':
                df['buy_price'].iat[d]= df[price_field].iloc[d]
                last_buy_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=1
             pos=0

    df_temp=df[df[positional_field]!=0].copy()
    
    if strategy_type=='short' :
        df_temp['sell_time']=df_temp.index
        df_temp['buy_time']=df_temp.index
        df_temp['buy_time']=df_temp['buy_time'].shift(-1)
        df_temp['buy_price']=df_temp['buy_price'].shift(-1)
        df_temp[exit_reason_field]=df_temp[exit_reason_field].shift(-1)
    if strategy_type=='long' :
        df_temp['buy_time']=df_temp.index
        df_temp['sell_time']=df_temp.index
        df_temp['sell_time']=df_temp['sell_time'].shift(-1)
        df_temp['sell_price']=df_temp['sell_price'].shift(-1)
        df_temp[exit_reason_field]=df_temp[exit_reason_field].shift(-1)
    
    df_summary=df_temp[(df_temp.buy_price>0)&(df_temp.sell_price>0)][['buy_price','buy_time','sell_price','sell_time',exit_reason_field]].copy()
    df_summary['returns']=(df_summary['sell_price']-df_summary['buy_price'])*100/df_summary['buy_price']
    df_summary['cum_returns']=(df_summary['sell_price']/df_summary['buy_price']).cumprod()
    #df_summary['cum_returns']=df_summary['cum_returns']
    df_summary['investment_period']=(df_summary['sell_time']-df_summary['buy_time'])
    
    return df,df_summary
                          
#*****************example***********************
'''
df_stock_data2=df_stock_data.copy()
df_stock_data2=HA_candle_score(df_stock_data2)

entry_criteria=list((df_stock_data2['HA_candle_score']<0))
exit_criteria=list((df_stock_data2['HA_candle_score']>0))
positional_field='pos_ha_strategy'
strategy_type='short'
price_field='Close'

df_stock_data3,summary= backtest_strategy(df_stock_data2, strategy_type,exit_criteria,entry_criteria, positional_field,price_field)
'''

#*****************example***********************


def backtest_strategy_stoploss_v2(df, strategy_type,lst_entry_criteria,lst_exit_criteria, positional_field,price_field,stoploss_pct,target_pct,only_profit):
    df['buy_price']=0.00
    df['sell_price']=0.00
  
    df['buy_time']=0
    df['sell_time']=0
    exit_reason_field=positional_field+'_exit_flag'
    df[positional_field]=0
    df[exit_reason_field]=''
    pos=0

    last_buy_price=0.00
    last_sell_price=0.00
    
    for d in range(1,len(df)):
        entry_flag=lst_entry_criteria[d-1]
        exit_flag=lst_exit_criteria[d-1]
        curr_price=df[price_field].iloc[d]
        stoploss_exit=False
        target_exit=False
        only_profit_exit=False
        exit_reason=''
        
        if stoploss_pct!=0:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)<stoploss_pct:
                    stoploss_exit=True
                    exit_reason='SLM'
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)<stoploss_pct:
                    stoploss_exit=True
                    exit_reason='SLM'
                    
        if target_pct!=0:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)>target_pct:
                    target_exit=True
                    exit_reason='TRM'
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)>target_pct:
                    target_exit=True
                    exit_reason='TRM'

        if only_profit==True:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)>0:
                    only_profit_exit=True
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)>0:
                    only_profit_exit=True
        else:
            only_profit_exit=True

        if exit_flag:
            exit_reason='ECM'
            
        if entry_flag&(pos==0) :
             if strategy_type=='long':
                df['buy_price'].iat[d]= df[price_field].iloc[d]
                last_buy_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=1
             elif strategy_type=='short':
                df['sell_price'].iat[d]= df[price_field].iloc[d]
                last_sell_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=-1
             pos=1
        elif (exit_flag|stoploss_exit|target_exit)& only_profit_exit & (pos==1) :
             df[exit_reason_field].iat[d]=exit_reason
             
             if strategy_type=='long':
                df['sell_price'].iat[d]= df[price_field].iloc[d]
                last_sell_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=-1
             elif strategy_type=='short':
                df['buy_price'].iat[d]= df[price_field].iloc[d]
                last_buy_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=1
             pos=0

    df_temp=df[df[positional_field]!=0].copy()
    
    if strategy_type=='short' :
        df_temp['sell_time']=df_temp.index
        df_temp['buy_time']=df_temp.index
        df_temp['buy_time']=df_temp['buy_time'].shift(-1)
        df_temp['buy_price']=df_temp['buy_price'].shift(-1)
        df_temp[exit_reason_field]=df_temp[exit_reason_field].shift(-1)
    if strategy_type=='long' :
        df_temp['buy_time']=df_temp.index
        df_temp['sell_time']=df_temp.index
        df_temp['sell_time']=df_temp['sell_time'].shift(-1)
        df_temp['sell_price']=df_temp['sell_price'].shift(-1)
        df_temp[exit_reason_field]=df_temp[exit_reason_field].shift(-1)
    
    df_summary=df_temp[(df_temp.buy_price>0)&(df_temp.sell_price>0)][['buy_price','buy_time','sell_price','sell_time',exit_reason_field]].copy()
    df_summary['returns']=(df_summary['sell_price']-df_summary['buy_price'])*100/df_summary['buy_price']
    df_summary['cum_returns']=(df_summary['sell_price']/df_summary['buy_price']).cumprod()
    #df_summary['cum_returns']=df_summary['cum_returns']
    df_summary['investment_period']=0#df_summary['sell_time']-df_summary['buy_time'])
    
    return df,df_summary



def backtest_strategy_stoploss_v3(df, strategy_type,lst_entry_criteria,lst_exit_criteria, positional_field,price_field,stoploss_pct,trail_stoploss_pct,target_pct,only_profit):
    df['buy_price']=0.0000
    df['sell_price']=0.0000
  
    df['buy_time']=None
    df['sell_time']=None
    exit_reason_field=positional_field+'_exit_flag'
    df[positional_field]=0
    df[exit_reason_field]=''
    pos=0

    last_buy_price=0.00
    last_sell_price=0.00
    trail_stoploss_exit=False
    
    for d in range(0,len(df)):
        entry_flag=lst_entry_criteria[d]
        exit_flag=lst_exit_criteria[d]
        curr_price=df[price_field].iloc[d]
        curr_time=df.index[d]
        stoploss_exit=False
        target_exit=False
        only_profit_exit=False
        exit_reason=''
        max_curr_price=0
        
        if stoploss_pct!=0:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)<stoploss_pct:
                    stoploss_exit=True
                    exit_reason='SLM'
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)<stoploss_pct:
                    stoploss_exit=True
                    exit_reason='SLM'
        
        
        if trail_stoploss_pct != 0:
            if (strategy_type == 'long') & (last_buy_price > 0):
                if max_curr_price == 0:
                    max_curr_price = last_buy_price
                if curr_price > max_curr_price:
                    max_curr_price = curr_price
                if ((curr_price - max_curr_price) * 100 / max_curr_price) < trail_stoploss_pct:
                    trail_stoploss_exit = True
                    exit_reason = 'TSLM'
                    max_curr_price = 0
            elif (strategy_type == 'short') & (last_sell_price > 0):
                if max_curr_price == 0:
                    max_curr_price = last_buy_price
                if curr_price < max_curr_price:
                    max_curr_price = curr_price
                if ((max_curr_price - curr_price) * 100 / curr_price) < trail_stoploss_pct:
                    trail_stoploss_exit = True
                    exit_reason = 'TSLM'

        
        if target_pct!=0:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)>target_pct:
                    target_exit=True
                    exit_reason='TRM'
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)>target_pct:
                    target_exit=True
                    exit_reason='TRM'

        if only_profit==True:
            if (strategy_type=='long')&(last_buy_price>0):
                if ((curr_price-last_buy_price)*100/last_buy_price)>0:
                    only_profit_exit=True
            elif (strategy_type=='short')&(last_sell_price>0):    
                if ((last_sell_price-curr_price)*100/curr_price)>0:
                    only_profit_exit=True
        else:
            only_profit_exit=True

        if exit_flag:
            exit_reason='ECM'
            
        if entry_flag&(pos==0) :
             if strategy_type=='long':
                df['buy_price'].iat[d]= df[price_field].iloc[d]
                last_buy_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=1
             elif strategy_type=='short':
                df['sell_price'].iat[d]= df[price_field].iloc[d]
                last_sell_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=-1
             pos=1
        elif (exit_flag|stoploss_exit|target_exit|trail_stoploss_exit)& only_profit_exit & (pos==1) :
             df[exit_reason_field].iat[d]=exit_reason
             
             if strategy_type=='long':
                df['sell_price'].iat[d]= df[price_field].iloc[d]
                last_sell_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=-1
             elif strategy_type=='short':
                df['buy_price'].iat[d]= df[price_field].iloc[d]
                last_buy_price=df[price_field].iloc[d]
                df[positional_field].iat[d]=1
             pos=0

    df_temp=df[df[positional_field]!=0].copy()
    
    df_temp['buy_time']=df_temp.index
    df_temp['sell_time']=df_temp.index

    df_buy=df_temp[df_temp.buy_price>0][['buy_price','buy_time']]
    df_buy.reset_index(drop=True,inplace=True)
    
    df_sell=df_temp[df_temp.sell_price>0][['sell_price','sell_time']]
    df_sell.reset_index(drop=True,inplace=True)
    
    long= pd.concat([df_buy,df_sell],axis=1,copy=True)
    
    if len(long)>0:
        if ~(long['sell_price'].iloc[-1]>0):
            long['sell_price'].iat[-1]=curr_price
            long['sell_time'].iat[-1]=curr_time
            
    
    long["returns"]=(long["sell_price"]-long["buy_price"])*100/long["buy_price"]
    long['cum_returns']=(long['sell_price']/long['buy_price']).cumprod()
    long['investment_period']=(long['sell_time']-long['buy_time'])

    short= pd.concat([df_buy,df_sell],axis=1,copy=True)
    
    if len(short)>0:
        if ~(short['buy_price'].iloc[-1]>0):
            short['buy_price'].iat[-1]=curr_price
            short['buy_time'].iat[-1]=curr_time
            
    short["returns"]=(short["sell_price"]-short["buy_price"])*100/short["buy_price"]
    short['cum_returns']=(short['sell_price']/short['buy_price']).cumprod()
    short['investment_period']=(short['buy_time']-short['sell_time'])
    
    if strategy_type=='long':
        return df,long
    else:
        return df,short


def backtest_strategy_v4(df, strategy_type, lst_entry_criteria, lst_exit_criteria, positional_field, price_field,
                      stoploss_pct, trail_stoploss_pct, target_pct, only_profit,trx_cost):
    df['buy_price'] = 0.0000
    df['sell_price'] = 0.0000

    df['buy_time'] = None
    df['sell_time'] = None
    exit_reason_field = positional_field + '_exit_flag'
    df[positional_field] = 0
    df[exit_reason_field] = ''
    pos = 0

    last_buy_price = 0
    last_sell_price = 0
    max_curr_price = 0

    for d in range(0, len(df)):
        entry_flag = lst_entry_criteria[d]
        exit_flag = lst_exit_criteria[d]
        curr_price = df[price_field].iloc[d]
        trail_stoploss_exit = False
        stoploss_exit = False
        target_exit = False
        only_profit_exit = False
        exit_reason = ''

        if stoploss_pct != 0:
            if (strategy_type == 'long') & (last_buy_price > 0):
                if ((curr_price - last_buy_price) * 100 / last_buy_price) < stoploss_pct:
                    stoploss_exit = True
                    exit_reason = 'SLM'
            elif (strategy_type == 'short') & (last_sell_price > 0):
                if ((last_sell_price - curr_price) * 100 / curr_price) < stoploss_pct:
                    stoploss_exit = True
                    exit_reason = 'SLM'

        if trail_stoploss_pct != 0:
            if (strategy_type == 'long') & (last_buy_price > 0):
                if max_curr_price == 0:
                    max_curr_price = last_buy_price
                if curr_price > max_curr_price:
                    max_curr_price = curr_price
                if ((curr_price - max_curr_price) * 100 / max_curr_price) < trail_stoploss_pct:
                    trail_stoploss_exit = True
                    exit_reason = 'TSLM'
                    max_curr_price = 0
            elif (strategy_type == 'short') & (last_sell_price > 0):
                if max_curr_price == 0:
                    max_curr_price = last_buy_price
                if curr_price < max_curr_price:
                    max_curr_price = curr_price
                if ((max_curr_price - curr_price) * 100 / curr_price) < trail_stoploss_pct:
                    trail_stoploss_exit = True
                    exit_reason = 'TSLM'

        if target_pct != 0:
            if (strategy_type == 'long') & (last_buy_price > 0):
                if ((curr_price - last_buy_price) * 100 / last_buy_price) > target_pct:
                    target_exit = True
                    exit_reason = 'TRM'
            elif (strategy_type == 'short') & (last_sell_price > 0):
                if ((last_sell_price - curr_price) * 100 / curr_price) > target_pct:
                    target_exit = True
                    exit_reason = 'TRM'

        if only_profit == True:
            if (strategy_type == 'long') & (last_buy_price > 0):
                if ((curr_price - last_buy_price) * 100 / last_buy_price) > 0:
                    only_profit_exit = True
            elif (strategy_type == 'short') & (last_sell_price > 0):
                if ((last_sell_price - curr_price) * 100 / curr_price) > 0:
                    only_profit_exit = True
        else:
            only_profit_exit = True

        if exit_flag:
            exit_reason = 'ECM'

        if entry_flag & (pos == 0):
            if strategy_type == 'long':
                df['buy_price'].iat[d] = df[price_field].iloc[d]
                last_buy_price = df[price_field].iloc[d]
                df[positional_field].iat[d] = 1
            elif strategy_type == 'short':
                df['sell_price'].iat[d] = df[price_field].iloc[d]
                last_sell_price = df[price_field].iloc[d]
                df[positional_field].iat[d] = -1
            pos = 1
        elif (exit_flag | stoploss_exit | target_exit | trail_stoploss_exit) & only_profit_exit & (pos == 1):
            df[exit_reason_field].iat[d] = exit_reason

            if strategy_type == 'long':
                if stoploss_exit:
                    df['sell_price'].iat[d] = last_buy_price * (1+stoploss_pct/100)
                elif target_exit:
                    df['sell_price'].iat[d] = last_buy_price * (1+target_pct/100)
                elif trail_stoploss_exit:
                    df['sell_price'].iat[d] = last_buy_price * (1+trail_stoploss_pct/100)
                else:
                    df['sell_price'].iat[d] = df[price_field].iloc[d]
                last_sell_price = df[price_field].iloc[d]
                df[positional_field].iat[d] = -1
            elif strategy_type == 'short':
                df['buy_price'].iat[d] = df[price_field].iloc[d]
                last_buy_price = df[price_field].iloc[d]
                df[positional_field].iat[d] = 1
            pos = 0

    df_temp = df[df[positional_field] != 0].copy()

    df_temp['buy_time'] = df_temp.index
    df_temp['sell_time'] = df_temp.index

    df_buy = df_temp[df_temp.buy_price != 0][['buy_price', 'buy_time']]
    df_buy.reset_index(drop=True, inplace=True)

    df_sell = df_temp[df_temp.sell_price != 0][['sell_price', 'sell_time']]
    df_sell.reset_index(drop=True, inplace=True)

    long = pd.concat([df_buy, df_sell], axis=1, copy=True)

    long["returns"] = ((long["sell_price"] - long["buy_price"] - trx_cost*long["buy_price"] - trx_cost*long["sell_price"])/long["buy_price"]) * 100
    long['cum_returns'] = ((long["sell_price"] - long["buy_price"] - trx_cost * long["buy_price"] - trx_cost * long[
        'sell_price']) / long["buy_price"]).cumsum()
    long['investment_period'] = (long['sell_time'] - long['buy_time'])
    long.index = long['buy_time'].copy()
    short = pd.concat([df_buy, df_sell], axis=1, copy=True)

    short["returns"] = ((short["sell_price"] - short["buy_price"] - trx_cost*short["sell_price"] - trx_cost*short["buy_price"])/short["sell_price"]) * 100
    short['cum_returns'] = ((short["sell_price"] - short["buy_price"] - trx_cost*short["sell_price"] - trx_cost*short["buy_price"])/short["sell_price"]).cumsum()
    short['investment_period'] = (short['sell_time'] - short['buy_time'])
    short.index = short['sell_time'].copy()
    if strategy_type == 'long':
        return df, long
    else:
        return df, short


def backtest_reports(df_summary):
    #print("investment period", df_summary['investment_period'].sum())
    #print("number of transactions", df_summary['investment_period'].count())
    print("Sum of returns", df_summary['returns'].sum())
    print("Average returns per transaction", df_summary['returns'].mean())

    df_summary['investment_period2']=(np.int64(df_summary['investment_period']/2))*2
    df_summary['returns2']=(np.int64(df_summary['returns']/0.5))*0.5
    
    g1=df_summary[['returns']].cumsum().plot()
    plt.show(g1)
    
    plt.title("Investment period vs Count of transactions")

    g2=sns.countplot(x="investment_period2",
     data=df_summary)
    plt.show(g2)
    
    plt.title("Percentage Returns vs Count of transactions")
    g3=sns.countplot(x="returns2",
     data=df_summary)

    plt.show(g3)

#**********************************************************************************************************************************
'''
OHLCInterval.
    Minute_1 = '1MINUTE'
    Minute_5 = '5MINUTE'
    Minute_10 = '10MINUTE'
    Minute_30 = '30MINUTE'
    Minute_60 = '60MINUTE'
    Day_1 = '1DAY'
    Week_1 = '1WEEK'
    Month_1 = '1MONTH'

TransactionType.
    Buy = 'B'
    Sell = 'S'

OrderType.
    Market = 'M'
    Limit = 'L'
    StopLossLimit = 'SL'
    StopLossMarket = 'SL-M'

ProductType.
    Intraday = 'I'
    Delivery = 'D'
    CoverOrder = 'CO'
    OneCancelsOther = 'OCO'

DurationType.
    DAY = 'DAY'
    IOC = 'IOC'

LiveFeedType.
    LTP = 'LTP'
    Full = 'Full'
'''
#**********************************************************************************************************************************

import glob
path='/Users/sunilguglani/Downloads/BHAVCOPY/*'
path2='/Users/sunilguglani/Downloads/BHAVCOPY/'
def consolidate_files(path):
    prices_consol=pd.DataFrame()   
    
    for g in glob.glob(path):
        print(g)
        prices=pd.read_csv(g)
        if len(prices)>0:
            #prices.index=prices['TIMESTAMP']
            prices_consol=prices_consol.append(prices)
    
    prices['TIMESTAMP']=pd.to_datetime(prices['TIMESTAMP'])
    prices.index=prices['TIMESTAMP']
    
    return prices_consol


def update_bhavcopy():
    
    df_bhavcopy=Fetch_Historical_Data_db('bhavcopy_view')
    df_bhavcopy['TIMESTAMP2']=pd.to_datetime(df_bhavcopy['TIMESTAMP'])
    from_period=df_bhavcopy['TIMESTAMP2'].max().date()#+datetime.timedelta(days=1)
    to_period=datetime.datetime.now().date()+datetime.timedelta(days=-1)
    from_period=to_period
    while from_period<to_period:
        try:
            prices = get_price_list(dt=from_period)
            prices.to_sql('bhavcopy',dbconn,if_exists='append')    
        except:
            print (" update_bhavcopy Unexpected error:", sys.exc_info())
            pass
    
        from_period=from_period+datetime.timedelta(days=1)
        print(from_period)

def update_bhavcopy_df():
    
    to_period=datetime.datetime.now().date()
    from_period=to_period+datetime.timedelta(days=-4)
    prices_all=pd.DataFrame()
    while from_period<=to_period:
        try:
            prices = get_price_list(dt=from_period)
            prices_all=prices_all.append(prices)
            from_period=from_period+datetime.timedelta(days=1)
            print(from_period)

        except:
            print (" update_bhavcopy Unexpected error:", sys.exc_info())
            pass
    
    return

#df=update_bhavcopy_df()

def sector_performance():
    dbconn=Connect_db()
    
    #df_bhavcopy=Fetch_Historical_Data_db('bhavcopy_view')
    df_bhavcopy=consolidate_files(path)

    #df_symbols=Fetch_Historical_Data_db('view_bse_scripts')
    df_symbols=pd.read_csv(path2+'ListOfScrips.csv')
    df_bhavcopy['TIMESTAMP2']=pd.to_datetime(df_bhavcopy['TIMESTAMP'])
    df_bhavcopy=df_bhavcopy.merge(df_symbols[['SYMBOL','Industry']],how='left',left_on='SYMBOL', right_on='SYMBOL')
    df_bhavcopy['Industry']=df_bhavcopy['Industry_y']
    df_bhavcopy.head()
    to_period=df_bhavcopy['TIMESTAMP2'].max().date() #+datetime.timedelta(days=-4)
    df_bhavcopy['daily_returns']=round(df_bhavcopy['CLOSE']/df_bhavcopy['PREVCLOSE'],3)
    df_bhavcopy=df_bhavcopy[df_bhavcopy.TIMESTAMP2<=to_period].copy()
    df_bhavcopy_3=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-3)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_1=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-1)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_5=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-5)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_12=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-13)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_29=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-29)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()

    df_perf_1=df_bhavcopy_1.groupby(by=['Industry'])['daily_returns'].mean().copy()

    df_perf_3=df_bhavcopy_3.groupby(by=['Industry'])['daily_returns'].mean().copy()
    
    df_perf_5=df_bhavcopy_5.groupby(by=['Industry'])['daily_returns'].mean().copy()

    df_perf_12=df_bhavcopy_12.groupby('Industry')['daily_returns'].mean().copy()
    
    df_perf_29=df_bhavcopy_29.groupby(by=['Industry'])['daily_returns'].mean().copy()   

    df_perf_consol=pd.DataFrame()

    df_perf_consol=pd.DataFrame(df_perf_1,copy=True)
    
    df_perf_consol.rename(columns={'daily_returns':'daily_returns_1'}, inplace=True)
    df_perf_consol['daily_returns_3']=pd.DataFrame(df_perf_3,copy=True)
    df_perf_consol['daily_returns_5']=pd.DataFrame(df_perf_5,copy=True)
    df_perf_consol['daily_returns_12']=pd.DataFrame(df_perf_12,copy=True)
    df_perf_consol['daily_returns_29']=pd.DataFrame(df_perf_29,copy=True)

    #filter_bull1=(df_perf_consol.daily_returns_1>=1.02)&(df_perf_consol.daily_returns_3>=1.01)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_5)&(df_perf_consol.daily_returns_5>=1.0)&(df_perf_consol.index!='')
    filter_bull1=(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_5)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_12)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_1)&(df_perf_consol.daily_returns_1>1)&(df_perf_consol.index!='')
    lst_bullish_sectors=list(df_perf_consol[filter_bull1].sort_values(by='daily_returns_1', ascending=False ).index)

    filter_bull2=((df_symbols.Script_Group=='A')|(df_symbols.Script_Group=='B'))
    filter_bull2=(df_symbols.Script_Group=='A')
    
    lst_bullish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bullish_sectors)))&filter_bull2].SYMBOL)

    #filter_bear1=(df_perf_consol.daily_returns_1<=0.99)&(df_perf_consol.daily_returns_3<=0.99)&(df_perf_consol.daily_returns_5>df_perf_consol.daily_returns_3)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_1)&(df_perf_consol.daily_returns_5<1.0)&(df_perf_consol.index!='')
    filter_bear1=(df_perf_consol.daily_returns_5>df_perf_consol.daily_returns_3)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_1)&(df_perf_consol.index!='')
    filter_bear3=(df_perf_consol.daily_returns_12>df_perf_consol.daily_returns_3)
    filter_bear1=filter_bear1&filter_bear3
    lst_bearish_sectors=list(df_perf_consol[filter_bear1].sort_values(by='daily_returns_1', ascending=True ).index)

    filter_bear2=(df_symbols.Script_Group=='A')|(df_symbols.Script_Group=='B')
    filter_bear2=(df_symbols.Script_Group=='A')
    

    lst_bearish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bearish_sectors)))&filter_bear2].SYMBOL)
    
    #@jit
    rmv_lst=[]
    for b in lst_bullish_stocks:
        lookback_period=12
        todate=datetime.datetime.now().date()
        fromdate=todate-datetime.timedelta(days=lookback_period)
        mode='nsepy'
        #b='PVR'
        df_script_1=Fetch_Historical_Data_nsepy(b,'NSE_EQ',fromdate,todate,mode)
        if (len(df_script_1)>0):
    
            if (df_script_1['%Deliverble'].iloc[-1])<(df_script_1['%Deliverble'].rolling(5).mean().iloc[-1]):
                rmv_lst.append(b)
            
            df_script_1=candle_df2(df_script_1,'bull')
            
            #if ~((df_script_1['candle_score'].iloc[-1]>=0)&(df_script_1['candle_score'].iloc[-2]>=0)&(df_script_1['candle_score'].iloc[-3]>=0)&(df_script_1['candle_cumsum'].iloc[-1]>0)):
    
            #if (df_script_1['candle_cumsum'].iloc[-1]<=0)| (df_script_1['candle_score'].iloc[-1]<0):
            if  (df_script_1['candle_score'].iloc[-1]<1):
                #lst_bullish_stocks.remove(b)
                print(b,'remove')
                rmv_lst.append(b)
        else:
            rmv_lst.append(b)
    for r in rmv_lst:
        try:
            lst_bullish_stocks.remove(r)
        except:
            pass

    rmv_lst=[]
    for b in lst_bearish_stocks:
        lookback_period=12
        todate=datetime.datetime.now().date()
        fromdate=todate-datetime.timedelta(days=lookback_period)
        mode='nsepy'
        
        df_script_1=Fetch_Historical_Data_nsepy(b,'NSE_EQ',fromdate,todate,mode)
        if (len(df_script_1)>0):

            df_script_1=candle_df(df_script_1)
            
            #if ~((df_script_1['candle_score'].iloc[-1]>=0)&(df_script_1['candle_score'].iloc[-2]>=0)&(df_script_1['candle_score'].iloc[-3]>=0)&(df_script_1['candle_cumsum'].iloc[-1]>0)):
    
            if (df_script_1['candle_cumsum'].iloc[-1]>0)| (df_script_1['candle_score'].iloc[-1]>=0):
                
                #lst_bullish_stocks.remove(b)
                print(b)
                rmv_lst.append(b)
        else:
            rmv_lst.append(b)
    
    for r in rmv_lst:
        try:
            lst_bearish_stocks.remove(r)
        except:
            pass

            
    #filter_bear2=True
    #lst_bearish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bearish_sectors)))&filter_bear2].SYMBOL)
    
    '''    
    df_bullish_stocks=df_symbols[(df_symbols.Industry.isin(lst_bullish_sectors))].copy()
    df_bullish_stocks=df_bullish_stocks[df_bullish_stocks['Script_Group'].isin(['A','B'])].copy()
    df_bearish_stocks=df_symbols[(df_symbols.Industry.isin(lst_bearish_sectors))].copy()
    #df_bullish_stocks=df_bullish_stocks[df_bullish_stocks]
    '''
    return lst_bullish_stocks,lst_bearish_stocks
    
    '''
    lst_bear,lst_bull=sector_performance()
    
    df_param['buy']=np.where(df_param.script.isin(lst_bear),'N',df_param['buy'])
    df_param['buy']=np.where(df_param.script.isin(lst_bull),'Y',df_param['buy'])
    '''

def sector_performance_v3():
    dbconn=Connect_db()
    
    df_bhavcopy=Fetch_Historical_Data_db('bhavcopy_view')
    df_symbols=Fetch_Historical_Data_db('view_bse_scripts')
    
    df_bhavcopy['TIMESTAMP2']=pd.to_datetime(df_bhavcopy['TIMESTAMP'])

    to_period=df_bhavcopy['TIMESTAMP2'].max().date()
    df_bhavcopy['daily_returns']=round(df_bhavcopy['CLOSE']/df_bhavcopy['PREVCLOSE'],3)
    
    df_bhavcopy_3=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-4)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_1=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-1)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_5=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-6)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_12=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-13)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_29=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-29)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()

    df_perf_1=df_bhavcopy_1.groupby(by=['Industry'])['daily_returns'].mean().copy()

    df_perf_3=df_bhavcopy_3.groupby(by=['Industry'])['daily_returns'].mean().copy()
    
    df_perf_5=df_bhavcopy_5.groupby(by=['Industry'])['daily_returns'].mean().copy()

    df_perf_12=df_bhavcopy_12.groupby('Industry')['daily_returns'].mean().copy()
    
    df_perf_29=df_bhavcopy_29.groupby(by=['Industry'])['daily_returns'].mean().copy()   

    df_perf_consol=pd.DataFrame()

    df_perf_consol=pd.DataFrame(df_perf_3,copy=True)
    
    df_perf_consol.rename(columns={'daily_returns':'daily_returns_3'}, inplace=True)
    df_perf_consol['daily_returns_1']=pd.DataFrame(df_perf_1,copy=True)
    df_perf_consol['daily_returns_5']=pd.DataFrame(df_perf_5,copy=True)
    df_perf_consol['daily_returns_12']=pd.DataFrame(df_perf_12,copy=True)
    df_perf_consol['daily_returns_29']=pd.DataFrame(df_perf_29,copy=True)

    #filter_bull1=(df_perf_consol.daily_returns_1>=1.02)&(df_perf_consol.daily_returns_3>=1.01)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_5)&(df_perf_consol.daily_returns_5>=1.0)&(df_perf_consol.index!='')
    filter_bull1=(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_5)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_12)&(df_perf_consol.daily_returns_1>1)&(df_perf_consol.index!='')
    lst_bullish_sectors=list(df_perf_consol[filter_bull1].sort_values(by='daily_returns_3', ascending=False ).index)

    filter_bull2=(df_symbols.Script_Group=='A')
    lst_bullish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bullish_sectors)))&filter_bull2].SYMBOL)

    #filter_bear1=(df_perf_consol.daily_returns_1<=0.99)&(df_perf_consol.daily_returns_3<=0.99)&(df_perf_consol.daily_returns_5>df_perf_consol.daily_returns_3)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_1)&(df_perf_consol.daily_returns_5<1.0)&(df_perf_consol.index!='')
    filter_bear1=(df_perf_consol.daily_returns_5>df_perf_consol.daily_returns_3)&(df_perf_consol.daily_returns_3>df_perf_consol.daily_returns_1)&(df_perf_consol.index!='')

    lst_bearish_sectors=list(df_perf_consol[filter_bear1].sort_values(by='daily_returns_3', ascending=True ).index)

    filter_bear2=(df_symbols.Script_Group=='A')
    lst_bearish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bearish_sectors)))&filter_bear2].SYMBOL)
    
    #@jit
    rmv_lst=[]
    for b in lst_bullish_stocks:
        lookback_period=12
        todate=datetime.datetime.now().date()
        fromdate=todate-datetime.timedelta(days=lookback_period)
        mode='nsepy'
        b='PVR'
        df_script_1=Fetch_Historical_Data_nsepy(b,'NSE_EQ',fromdate,todate,mode)
        if (len(df_script_1)>0):
    
            if (df_script_1['%Deliverble'].iloc[-1])<(df_script_1['%Deliverble'].rolling(5).mean().iloc[-1]):
                rmv_lst.append(b)
            
            df_script_1=candle_df(df_script_1)
            
            #if ~((df_script_1['candle_score'].iloc[-1]>=0)&(df_script_1['candle_score'].iloc[-2]>=0)&(df_script_1['candle_score'].iloc[-3]>=0)&(df_script_1['candle_cumsum'].iloc[-1]>0)):
    
            if (df_script_1['candle_cumsum'].iloc[-1]<=0)| (df_script_1['candle_score'].iloc[-1]<0):
                
                #lst_bullish_stocks.remove(b)
                print(b)
                rmv_lst.append(b)
        else:
            rmv_lst.append(b)
    for r in rmv_lst:
        try:
            lst_bullish_stocks.remove(r)
        except:
            pass

    rmv_lst=[]
    for b in lst_bearish_stocks:
        lookback_period=12
        todate=datetime.datetime.now().date()
        fromdate=todate-datetime.timedelta(days=lookback_period)
        mode='nsepy'
        
        df_script_1=Fetch_Historical_Data_nsepy(b,'NSE_EQ',fromdate,todate,mode)
        if (len(df_script_1)>0):

            df_script_1=candle_df(df_script_1)
            
            #if ~((df_script_1['candle_score'].iloc[-1]>=0)&(df_script_1['candle_score'].iloc[-2]>=0)&(df_script_1['candle_score'].iloc[-3]>=0)&(df_script_1['candle_cumsum'].iloc[-1]>0)):
    
            if (df_script_1['candle_cumsum'].iloc[-1]>0)| (df_script_1['candle_score'].iloc[-1]>=0):
                
                #lst_bullish_stocks.remove(b)
                print(b)
                rmv_lst.append(b)
        else:
            rmv_lst.append(b)
    
    for r in rmv_lst:
        try:
            lst_bearish_stocks.remove(r)
        except:
            pass

            
    #filter_bear2=True
    #lst_bearish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bearish_sectors)))&filter_bear2].SYMBOL)
    
    '''    
    df_bullish_stocks=df_symbols[(df_symbols.Industry.isin(lst_bullish_sectors))].copy()
    df_bullish_stocks=df_bullish_stocks[df_bullish_stocks['Script_Group'].isin(['A','B'])].copy()
    df_bearish_stocks=df_symbols[(df_symbols.Industry.isin(lst_bearish_sectors))].copy()
    #df_bullish_stocks=df_bullish_stocks[df_bullish_stocks]
    '''
    return lst_bullish_stocks,lst_bearish_stocks
    
    '''
    lst_bear,lst_bull=sector_performance()
    
    df_param['buy']=np.where(df_param.script.isin(lst_bear),'N',df_param['buy'])
    df_param['buy']=np.where(df_param.script.isin(lst_bull),'Y',df_param['buy'])
    '''
        
def sector_performance_v2():
    dbconn=Connect_db()
    
    df_bhavcopy=Fetch_Historical_Data_db('bhavcopy_view')
    df_symbols=Fetch_Historical_Data_db('view_bse_scripts')
    
    df_bhavcopy['TIMESTAMP2']=pd.to_datetime(df_bhavcopy['TIMESTAMP'])

    to_period=df_bhavcopy['TIMESTAMP2'].max().date()
    df_bhavcopy['daily_returns']=round(df_bhavcopy['CLOSE']/df_bhavcopy['PREVCLOSE'],3)
    
    df_bhavcopy['3_stk']=df_bhavcopy
    df_bhavcopy_3=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-3)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_1=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-1)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_5=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-5)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_12=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-12)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()
    df_bhavcopy_29=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-29)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()

    df_perf_1=df_bhavcopy_1.groupby(by=['Industry'])['daily_returns'].mean().copy()

    df_perf_3=df_bhavcopy_3.groupby(by=['Industry'])['daily_returns'].mean().copy()
    
    df_perf_5=df_bhavcopy_5.groupby(by=['Industry'])['daily_returns'].mean().copy()

    df_perf_12=df_bhavcopy_12.groupby('Industry')['daily_returns'].mean().copy()
    
    df_perf_29=df_bhavcopy_29.groupby(by=['Industry'])['daily_returns'].mean().copy()   

    df_perf_consol=pd.DataFrame()

    df_perf_consol=pd.DataFrame(df_perf_3,copy=True)
    
    df_perf_consol.rename(columns={'daily_returns':'daily_returns_3'}, inplace=True)
    df_perf_consol['daily_returns_1']=pd.DataFrame(df_perf_1,copy=True)
    df_perf_consol['daily_returns_5']=pd.DataFrame(df_perf_5,copy=True)
    df_perf_consol['daily_returns_12']=pd.DataFrame(df_perf_12,copy=True)
    df_perf_consol['daily_returns_29']=pd.DataFrame(df_perf_29,copy=True)

    filter_bull1=(df_perf_consol.daily_returns_1>=1.01)&(df_perf_consol.daily_returns_3>=1.005)&(df_perf_consol.daily_returns_5>=1.0)&(df_perf_consol.index!='')
    lst_bullish_sectors=list(df_perf_consol[filter_bull1].sort_values(by='daily_returns_3', ascending=False ).index)

    filter_bull2=(df_symbols.Script_Group=='A')
    lst_bullish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bullish_sectors)))&filter_bull2].SYMBOL)

    filter_bear1=(df_perf_consol.daily_returns_1<=0.99)&(df_perf_consol.daily_returns_3>=0.99)&(df_perf_consol.daily_returns_5<1.0)&(df_perf_consol.index!='')
    lst_bearish_sectors=list(df_perf_consol[filter_bear1].sort_values(by='daily_returns_3', ascending=True ).index)

    filter_bear2=(df_symbols.Script_Group=='A')
    #filter_bear2=True
    #lst_bearish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bearish_sectors)))&filter_bear2].SYMBOL)
    lst_bearish_stocks=list(df_symbols[((df_symbols.Industry.isin(lst_bearish_sectors)))&filter_bear2].SYMBOL)
    return lst_bearish_stocks,lst_bullish_stocks
    


def bullish_stocks():
    update_bhavcopy()
    df_bhavcopy=Fetch_Historical_Data_db('bhavcopy_view')
    df_symbols=Fetch_Historical_Data_db('view_bse_scripts')
    
    
    df_bhavcopy['TIMESTAMP2']=pd.to_datetime(df_bhavcopy['TIMESTAMP'])

    df_bhavcopy.sort_values(['SYMBOL','TIMESTAMP2'],ascending=True,inplace=True)
    
    df_bhavcopy['Open-Low']=False
    df_bhavcopy['Close-High']=False
    

    df_bhavcopy['Volume_increased']=(df_bhavcopy['TOTTRDVAL']*0.8>df_bhavcopy['TOTTRDVAL'].shift(1).rolling(3).mean())

    df_bhavcopy['Open-Low']=(((df_bhavcopy['OPEN']-df_bhavcopy['LOW'])*100/df_bhavcopy['OPEN'])<0.01)
    df_bhavcopy['Close-High']=(((df_bhavcopy['HIGH']-df_bhavcopy['CLOSE'])*100/df_bhavcopy['CLOSE'])<0.01)
    df_bhavcopy['Price-Up']=((((df_bhavcopy['CLOSE']-df_bhavcopy['PREVCLOSE'])*100/df_bhavcopy['CLOSE']))>1.1)
    df_bhavcopy['NEXT_DAY_CLOSE'],df_bhavcopy['NEXT_DAY_OPEN']=df_bhavcopy['CLOSE'].shift(-1),df_bhavcopy['OPEN'].shift(-1)
    #df_bhavcopy['next_day_returns']=np.where( ((df_bhavcopy['Volume_increased']==True)&(df_bhavcopy['Open-Low']==True)&(df_bhavcopy['Close-High']==True))  &(df_bhavcopy['Price-Up']==True)&(df_bhavcopy['Price-Up']==True)&((df_bhavcopy['SYMBOL']==df_bhavcopy['SYMBOL'].shift(-1))),(df_bhavcopy['CLOSE'].shift(-1)-df_bhavcopy['OPEN'].shift(-1))*100/df_bhavcopy['OPEN'].shift(-1),0)
    df_bhavcopy['next_day_returns']=np.where((df_bhavcopy['Volume_increased']==True)
    &(df_bhavcopy['Close-High']==True)
    &(df_bhavcopy['NEXT_DAY_OPEN']>df_bhavcopy['CLOSE'])
    &(df_bhavcopy['Open-Low']==True) 
    &(df_bhavcopy['Price-Up']==True)
    &(df_bhavcopy['SYMBOL']==df_bhavcopy['SYMBOL'].shift(-1)),
    (df_bhavcopy['CLOSE'].shift(-1)-df_bhavcopy['OPEN'].shift(-1))*100/df_bhavcopy['OPEN'].shift(-1),0)
    
    df_bhavcopy.groupby(by=['Script_Group'])['next_day_returns'].sum()
    df_bhavcopy.to_csv('df_bhavcopy.csv')
    to_period=df_bhavcopy['TIMESTAMP2'].max().date()
    df_bhavcopy['daily_returns']=round(df_bhavcopy['CLOSE']/df_bhavcopy['PREVCLOSE'],3)
    
    df_bhavcopy=df_bhavcopy[(df_bhavcopy.TIMESTAMP2==(to_period))].copy()
    
    df_bhavcopy_3=df_bhavcopy[(df_bhavcopy.TIMESTAMP2>=(to_period+datetime.timedelta(days=-3)))&(df_bhavcopy.TIMESTAMP2<=to_period)].copy()


    year_change_pct=(df_bhavcopy.groupby(by=['SYMBOL'])['HIGH'].max()-df_bhavcopy.groupby(by=['SYMBOL'])['LOW'].min())*100/df_bhavcopy.groupby(by=['SYMBOL'])['LOW'].min()
    
    df_temp=df_bhavcopy['year_change_pct']
    

        
def sector_performance2():
    #update_bhavcopy()
    filename='stocktrend_'+str(datetime.datetime.now().strftime('%Y-%m-%d'))+".txt"
    dbconn=Connect_db()
    df_bhavcopy=Fetch_Historical_Data_db('bhavcopy_view')
    df_bhavcopy['PCT_CHANGE']=((df_bhavcopy['CLOSE']-df_bhavcopy['PREVCLOSE'])*100/df_bhavcopy['PREVCLOSE'])
    
    df_symbols=Fetch_Historical_Data_db('view_bse_scripts')
    df_derivatives=Fetch_Historical_Data_db('view_derivatives_stocks')
    
    df_bhavcopy['Date']=pd.to_datetime(df_bhavcopy['TIMESTAMP'])
    df_bhavcopy.sort_values(by=['SYMBOL','Date'],ascending=True,inplace=True)

    df_bhavcopy['VOL_PCT_CHANGE']=((df_bhavcopy['TOTTRDVAL']-df_bhavcopy['TOTTRDVAL'].shift(1))*100/df_bhavcopy['TOTTRDVAL'].shift(1))
    #df2=df_bhavcopy[df_bhavcopy['SYMBOL'].isin(list(df_derivatives.SYMBOL))&(df_bhavcopy['Date'].max()==df_bhavcopy['Date']) ]


    df_bhavcopy.sort_values(by='Date',ascending=True,inplace=True)
    df_bhavcopy.index=df_bhavcopy['Date'].copy()

    c = dbconn.cursor()

    
    lst_indust=list(df_bhavcopy['Industry'].unique())
    
    from_period=datetime.datetime.now().date()-datetime.timedelta(days=50)
    to_period=datetime.datetime.now().date()
        
    while from_period<to_period:
        df_temp= df_bhavcopy[df_bhavcopy.Date<=from_period].copy()
        print(df_temp['Date'].max())
        
        c.execute('delete from sector_algo_summary2')
        #c.execute('delete from sector_algo_daily2')
        #c.execute('delete from sector_algo_daily1')
        c.execute('delete from sector_algo_summary1')

        
        for j in lst_indust:
            #print(j)
            #df_temp= df_bhavcopy.copy()
            df_temp= df_bhavcopy[df_bhavcopy.Date<=from_period].copy()
            df_temp= first_letter_upper_v2(df_temp)
            df_temp=df_temp[df_temp.Industry==j].copy()
            
            df_temp['Open_pct_change']=df_temp['Open'].pct_change()
            df_temp['Close_pct_change']=df_temp['Close'].pct_change()
            df_temp['High_pct_change']=df_temp['High'].pct_change()
            df_temp['Low_pct_change']=df_temp['Low'].pct_change()
                    
            df_temp_O=df_temp.groupby(by=['Date'])['Open_pct_change'].mean().copy()
    
            df_temp_H=df_temp.groupby(by=['Date'])['High_pct_change'].mean().copy()
            
            df_temp_L=df_temp.groupby(by=['Date'])['Low_pct_change'].mean().copy()
        
            df_temp_C=df_temp.groupby('Date')['Close_pct_change'].mean().copy()
            
            df_temp_ind=pd.concat([df_temp_O,df_temp_H,df_temp_L,df_temp_C],axis=1)
            
            df_temp_ind['Open']=df_temp_ind['Open_pct_change'] 
            df_temp_ind['High'] =df_temp_ind['High_pct_change'] 
            df_temp_ind['Low']=df_temp_ind['Low_pct_change']
            df_temp_ind['Close']=df_temp_ind['Close_pct_change']
            df_temp_ind['script']=j
            
            price_field="Close"
            #df_temp =ema_crossover(df_temp,13,5,price_field)
            #df_temp=supertrend(df_temp,14,3)
            df_temp_ind.sort_values(by='Date',inplace=True)
            df_temp_ind=SuperTrend(df_temp_ind, 7, 1.2, ohlc=['Open', 'High', 'Low', 'Close'])
            #df_temp[['Close','ST_7_3']].plot()
            strategy_type='long'
            
            df_temp_ind.dropna(inplace=True)
            df_temp_ind=df_temp_ind[df_temp_ind['ST_7_1.2']!=0].copy()
    
            #lst_entry_criteria,lst_exit_criteria=supertrend_setup(df_temp_ind,strategy_type)
            lst_entry_criteria=(df_temp_ind['Close']>df_temp_ind['ST_7_1.2'])&(df_temp_ind['Close'].shift(1)<=df_temp_ind['ST_7_1.2'].shift(1))
            lst_exit_criteria=(df_temp_ind['Close']<df_temp_ind['ST_7_1.2'])&(df_temp_ind['Close'].shift(1)>=df_temp_ind['ST_7_1.2'].shift(1))
        
            final_entry=lst_entry_criteria
            final_exit=lst_exit_criteria
            
            positional_field='pos_stoch_strategy'
            price_field='Close'
            stoploss_pct=-1
            target_pct=30
            only_profit=False
            
            df_temp_ind,summary_min=backtest_strategy_stoploss_v3(df_temp_ind, strategy_type,list(final_entry),list(final_exit), positional_field,price_field,stoploss_pct,target_pct,only_profit)
            #backtest_reports(summary_min)
            df_temp_ind['Script']=j
            summary_min['script']=j
            #(summary_min.groupby(['pos_HA_stoch_strategy_exit_flag'])['returns'].sum()).hist(bins=30)
            
            #df_temp_ind.to_sql('sector_algo_daily1',dbconn,if_exists='append')
            summary_min.to_sql('sector_algo_summary1',dbconn,if_exists='append')
            
    
        df_bullish_sectors=Fetch_Historical_Data_db('view_sector_algo_summary')
        
        
        lst_bull_indust=list(df_bullish_sectors['script'].unique())
        
        lst_bull_symbols=list(df_derivatives[df_derivatives.Industry.isin(lst_bull_indust)]['SYMBOL'])
        #j='PEL'
        #SRTRANSFIN,SREINFRA
    
        for bs in lst_bull_symbols:
            j=bs
            #print(j)
            #df_temp= df_bhavcopy.copy()
            df_temp= df_bhavcopy[df_bhavcopy.Date<=from_period].copy()
            df_temp= first_letter_upper_v2(df_temp)
            df_temp=df_temp[df_temp.Symbol==j].copy()
    
            price_field="Close"
    
            param1=7
            str_param1=str(param1)
            
            param2=1.1
            str_param2=str(param2)
            super_field_name='ST_'+str_param1+'_'+str_param2
            df_temp.sort_values(by='Date',ascending=True, inplace=True)
            df_temp=SuperTrend(df_temp, 7, param2, ohlc=['Open', 'High', 'Low', 'Close'])
            strategy_type='long'
            
            df_temp.dropna(inplace=True)
            df_temp=df_temp[df_temp[super_field_name]!=0].copy()
            
            lst_entry_criteria=(df_temp['Close']>df_temp[super_field_name])&(df_temp['Close'].shift(1)<=df_temp[super_field_name].shift(1))
            lst_exit_criteria=(df_temp['Close']<df_temp[super_field_name])&(df_temp['Close'].shift(1)>=df_temp[super_field_name].shift(1))
            
            if strategy_type=='long':
                final_entry=lst_entry_criteria
                final_exit=lst_exit_criteria
            else:
                final_entry=lst_exit_criteria
                final_exit=lst_entry_criteria
                        
            positional_field='pos_stoch_strategy'
            price_field='Close'
            stoploss_pct=-100
            target_pct=300
            only_profit=False
            
            df_temp,summary_min=backtest_strategy_stoploss_v3(df_temp, strategy_type,list(final_entry),list(final_exit), positional_field,price_field,stoploss_pct,target_pct,only_profit)
            #backtest_reports(summary_min)
            df_temp['Script']=j
            summary_min['script']=j
            #(summary_min.groupby(['pos_HA_stoch_strategy_exit_flag'])['returns'].sum()).hist(bins=30)
            #lstcols=list(df_temp.columns).remove('Date')
            df_temp.reset_index(drop=True,inplace=True)
            #df_temp.to_sql('sector_algo_daily2',dbconn,if_exists='append')
            summary_min.to_sql('sector_algo_summary2',dbconn,if_exists='append')
            
        lstbullish=list(Fetch_Historical_Data_db('view_sector_algo_summary_stocks')['script'])
        #print(from_period)        
        from_period=from_period+datetime.timedelta(days=1)

        file_write=str(df_bhavcopy[(df_bhavcopy.SYMBOL.isin(lstbullish))&(df_bhavcopy.Date==from_period)][['Date','SYMBOL','PCT_CHANGE']])
        
        with open(filename, 'a') as file:
            file.write(file_write)
        
#sector_performance2()
import yfinance as yf

def Correlation_score(script_1,script_2,timeframe='D',lookback_period=25,fieldname='Close'):
    corr_score=0
    todate=datetime.datetime.now().date()
    fromdate=todate-datetime.timedelta(days=lookback_period)
    mode='nsepy'
    
    if timeframe=='D':
        lookback_period=2500
        todate=datetime.datetime.now().date()
        fromdate=todate-datetime.timedelta(days=lookback_period)
        
        df_script_1=yf.download(str(script_1), start=fromdate, end=todate)
        df_script_2=yf.download(str(script_2), start=fromdate, end=todate)
        corr_score=df_script_1[fieldname].corr(df_script_2[fieldname])
        df_script_1['Close_y']=df_script_2['Close']
        df_script_1[['Close','Close_y']].plot(y=['Close','Close_y'],secondary_y=['Close_y'])
    return corr_score
'''
script_1='^NSEI'
script_2='^NSEBANK'
corr_score= Correlation_score(script_1,script_2,timeframe='D',lookback_period=25,fieldname='Close')
print(corr_score)
'''
'''
script_1='UNIONBANK'
script_cat_1='NSE_EQ'
script_2='BANKINDIA' 
script_cat_2='NSE_EQ'
fieldname='Close'
timeframe='D'
corr_score= Correlation_score(script_1,script_cat_1,script_2,script_cat_2,timeframe='D',lookback_period=250,fieldname='Close')
'''
'''
def levels(script_1,script_cat_1,timeframe='D',lookback_period=200,fieldname='Close'):
    script_1='BAJFINANCE'
    script_cat_1='NSE_EQ'
    lookback_period=200
    todate=datetime.datetime.now().date()
    timeframe='D'
    fromdate=todate-datetime.timedelta(days=lookback_period)
    mode='nsepy'
    
    if timeframe=='D':
        df_script_1=Fetch_Historical_Data_nsepy(script_1,script_cat_1,fromdate,todate,mode)
    
    
    i=1
    df_script_1['HIGHER_HIGH']=0
    df_script_1['LOWEST_LOW']=0
    df_script_1['HIGH_CHANGE']=0
    df_script_1['HIGHER_HIGH'].iloc[0]=df_script_1['High'].iloc[0]
    df_script_1['LOWEST_LOW'].iloc[0]=df_script_1['Low'].iloc[0]

    while i<len(df_script_1):
        df_script_1['HIGHER_HIGH'].iloc[i]=max(df_script_1['HIGHER_HIGH'].iloc[i-1],df_script_1['High'].iloc[i])
        df_script_1['LOWEST_LOW'].iloc[i]=min(df_script_1['LOWEST_LOW'].iloc[i-1],df_script_1['Low'].iloc[i])

        i=i+1
    df_script_1['HIGH_CHANGE']=np.where(df_script_1['HIGHER_HIGH']==df_script_1['HIGHER_HIGH'].shift(-1),0,1)
    df_script_1['LOW_CHANGE']=np.where(df_script_1['LOWEST_LOW']==df_script_1['LOWEST_LOW'].shift(-1),0,-1)

    df_script_1[['Close','HIGH_CHANGE','LOW_CHANGE']].plot(y=['Close','HIGH_CHANGE','LOW_CHANGE'],secondary_y='Close',figsize=(12,9))

    df_script_1[['Close','HIGH_CHANGE']].plot(y=['Close','HIGH_CHANGE'],secondary_y='Close',figsize=(12,9))

    df_script_1[['Close','HIGHER_HIGH','LOWEST_LOW']].plot(y=['Close','HIGHER_HIGH','LOWEST_LOW'],secondary_y='Close',figsize=(12,9))
    df_script_1[['High','HIGHER_HIGH','LOWEST_LOW']].plot(y=['High','HIGHER_HIGH','LOWEST_LOW'],secondary_y='High',figsize=(12,9))

    return corr_score
'''

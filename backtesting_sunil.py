#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 22:45:03 2018

@author: sunilguglani
"""
from nsepy import get_history


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
import matplotlib.pyplot as plt
import seaborn as sns
from numba import jit
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import sys

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

import glob
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

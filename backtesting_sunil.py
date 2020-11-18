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
import yfinance as yf
import glob
import os

import sys
import requests
from nsepy.live import get_quote
from pandas.io.json import json_normalize 
import nsetools
import time
from nsepy import get_history,get_index_pe_history


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




warnings.filterwarnings('ignore')

def Combinations_6(lst_A,lst_B,lst_C,lst_D,lst_E,lst_F):
    lst_combination=[]
    for a in lst_A:
        for b in lst_B:
            for c in lst_C:
                for d in lst_D:
                    for e in lst_E:
                        for f in lst_F:
                            lst_combination.append([a,b,c,d,e,f])
    return lst_combination


def Combinations_4(lst_A,lst_B,lst_C,lst_D):
    lst_combination=[]
    for a in lst_A:
        for b in lst_B:
            for c in lst_C:
                for d in lst_D:
                    lst_combination.append([a,b,c,d])
    return lst_combination


def convert_to_string(list): 
    s=''
    for i in list:
        s=s+str(i)+' '
    
    s=s+'\n'
    return str(s)

def fetch_data_yf(script,lookback_period,interval='1d') :
    todate=datetime.datetime.now().date()
    fromdate=todate-datetime.timedelta(days=lookback_period)
    
    if (script=='NIFTY')|(script=='^NSEI'):
        script='^NSEI'
    elif (script=='BANKNIFTY')|(script=='^NSEBANK'):
        script='^NSEBANK'
    else:
        script=str(script)+'.NS'
        
    df = yf.download(str(script), start=fromdate, end=todate,interval=interval)

    #df["Date"]=pd.to_datetime(df.Date)
    print(script,str(script)[0:1])
    if str(script)[0:1]!='^':
        df=df[df['Volume']>0].copy()

    return df



def consolidate_files(path,filename):
    prices_consol=pd.DataFrame()   
    
    #for g in glob.glob("/Users/sunilguglani/Downloads/Nifty_minute/*/NIIT*.txt"):
    for g in glob.glob(path+filename):
        #print(g)
        prices=pd.DataFrame()
        prices=pd.read_csv(g,names=['Symbol','Date','timestamp','Open','High','Low','Close','OI','Volume'])
        prices.index=prices['Date'].astype(str)+' '+prices['timestamp'].astype(str)
        prices_consol=prices_consol.append(prices)
        prices=None
    
    return prices_consol


'''
# Function to generate backtest reports
'''
def backtest_reports_local(df_summary,lot_size,trx_charge):
    #print("investment period", df_summary['investment_period'].sum())
    #print("number of transactions", df_summary['investment_period'].count())
    print("Sum of returns in %", df_summary['returns'].sum())
    print("Average returns per transaction in %", df_summary['returns'].mean())
    print("Absolute returns", df_summary['returns_abs'].sum())
    print("Absolute returns per trx", df_summary['returns_abs'].sum()/df_summary['returns_abs'].count())
    print("Max drawdown for a trx", df_summary[df_summary.returns_abs<0]['returns_abs'].min())
    print("Max returns for a trx", df_summary[df_summary.returns_abs>0]['returns_abs'].max())
    print("Losing trx", df_summary[df_summary.returns_abs<0]['returns_abs'].count())
    print("Winning trx", df_summary[df_summary.returns_abs>0]['returns_abs'].count())
    print("Win/Lose ratio ", (df_summary[df_summary.returns_abs>0]['returns_abs'].count())/(df_summary[df_summary.returns_abs<0]['returns_abs'].count()))
    
    df_summary.index=df_summary.buy_time
    df_summary['returns2']=np.round((np.int64(df_summary['returns']/.5))*.5,0)
    
    g1=df_summary[['returns2']].cumsum().plot(figsize=(9,6))
    #fig.autofmt_xdate()
    #ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
    
    plt.tight_layout()

    plt.show(g1)

    plt.title("Percentage Returns vs Count of transactions")
    
    g3=sns.countplot(x="returns2",
     data=df_summary)
    
    plt.show(g3)

'''
invoking the main Function 
'''
def Combination(lstA,lstB,lstC):
    lstAll=[]
    for a in lstA:
        for b in lstB:
            for c  in lstC:
              lstAll.append([a,b,c])
    return lstAll

# Function to generate mini backtest reports
def backtest_reports_local2(roll,low,up,df_summary,lot_size,trx_charge):
    print(roll,low,up, round(df_summary['returns'].sum(),2),round(df_summary['returns'].mean(),2),round(df_summary[df_summary.returns_abs<0]['returns'].min(),2),
    round((df_summary[df_summary.returns_abs>0]['returns'].count())/
          (df_summary[df_summary.returns_abs<0]['returns'].count()),2))
    
    return (df_summary['returns'].sum(),
    df_summary['returns'].mean(),
    df_summary[df_summary.returns_abs<0]['returns'].min(),
    (df_summary[df_summary.returns_abs>0]['returns'].count())/(df_summary[df_summary.returns_abs<0]['returns'].count()))


lstDeriv=['ACC',	'ADANIENT',	'ADANIPORTS',	'ADANIPOWER',	'AJANTPHARM',	'ALBK',	'AMARAJABAT',	'AMBUJACEM',	'ANDHRABANK',	'APOLLOHOSP',	'APOLLOTYRE',	'ARVIND',	'ASHOKLEY',	'ASIANPAINT',	'AUROPHARMA',	'AXISBANK',	'BAJAJ-AUTO',	'BAJFINANCE',	'BAJAJFINSV',	'BALKRISIND',	'BALRAMCHIN',	'BANKBARODA',	'BANKINDIA',	'BATAINDIA',	'BEML',	'BERGEPAINT',	'BEL',	'BHARATFIN',	'BHARATFORG',	'BPCL',	'BHARTIARTL',	'INFRATEL',	'BHEL',	'BIOCON',	'BOSCHLTD',	'BRITANNIA',	'CADILAHC',	'CANFINHOME',	'CANBK',	'CAPF',	'CASTROLIND',	'CEATLTD',	'CENTURYTEX',	'CESC',	'CGPOWER',	'CHENNPETRO',	'CHOLAFIN',	'CIPLA',	'COALINDIA',	'COLPAL',	'CONCOR',	'CUMMINSIND',	'DABUR',	'DALMIABHA',	'DCBBANK',	'DHFL',	'DISHTV',	'DIVISLAB',	'DLF',	'DRREDDY',	'EICHERMOT',	'ENGINERSIN',	'EQUITAS',	'ESCORTS',	'EXIDEIND',	'FEDERALBNK',	'GAIL',	'GLENMARK',	'GMRINFRA',	'GODFRYPHLP',	'GODREJCP',	'GODREJIND',	'GRANULES',	'GRASIM',	'GSFC',	'HAVELLS',	'HCLTECH',	'HDFCBANK',	'HDFC',	'HEROMOTOCO',	'HEXAWARE',	'HINDALCO',	'HCC',	'HINDPETRO',	'HINDUNILVR',	'HINDZINC',	'ICICIBANK',	'ICICIPRULI',	'IDBI',	'IDEA',	'IDFCBANK',	'IDFC',	'IFCI',	'IBULHSGFIN',	'INDIANB',	'IOC',	'IGL',	'INDUSINDBK',	'INFIBEAM',	'INFY',	'INDIGO',	'IRB',	'ITC',	'JISLJALEQS',	'JPASSOCIAT',	'JETAIRWAYS',	'JINDALSTEL',	'JSWSTEEL',	'JUBLFOOD',	'JUSTDIAL',	'KAJARIACER',	'KTKBANK',	'KSCL',	'KOTAKBANK',	'KPIT',	'L&TFH',	'LT',	'LICHSGFIN',	'LUPIN',	'M&MFIN',	'MGL',	'M&M',	'MANAPPURAM',	'MRPL',	'MARICO',	'MARUTI',	'MFSL',	'MINDTREE',	'MOTHERSUMI',	'MRF',	'MCX',	'MUTHOOTFIN',	'NATIONALUM',	'NBCC',	'NCC',	'NESTLEIND',	'NHPC',	'NIITTECH',	'NMDC',	'NTPC',	'ONGC',	'OIL',	'OFSS',	'ORIENTBANK',	'PAGEIND',	'PCJEWELLER',	'PETRONET',	'PIDILITIND',	'PEL',	'PFC',	'POWERGRID',	'PTC',	'PNB',	'PVR',	'RAYMOND',	'RBLBANK',	'RELCAPITAL',	'RCOM',	'RNAVAL',	'RELIANCE',	'RELINFRA',	'RPOWER',	'REPCOHOME',	'RECLTD',	'SHREECEM',	'SRTRANSFIN',	'SIEMENS',	'SREINFRA',	'SRF',	'SBIN',	'SAIL',	'STAR',	'SUNPHARMA',	'SUNTV',	'SUZLON',	'SYNDIBANK',	'TATACHEM',	'TATACOMM',	'TCS',	'TATAELXSI',	'TATAGLOBAL',	'TATAMTRDVR',	'TATAMOTORS',	'TATAPOWER',	'TATASTEEL',	'TECHM',	'INDIACEM',	'RAMCOCEM',	'SOUTHBANK',	'TITAN',	'TORNTPHARM',	'TORNTPOWER',	'TV18BRDCST',	'TVSMOTOR',	'UJJIVAN',	'ULTRACEMCO',	'UNIONBANK',	'UBL',	'MCDOWELL-N',	'UPL',	'VEDL',	'VGUARD',	'VOLTAS',	'WIPRO',	'WOCKPHARMA',	'YESBANK',	'ZEEL']


def traders_hunt():
    traders_hunt_url='https://docs.google.com/spreadsheets/d/1aLKkL-nwJXtKoRCB3wKZMed0GMucP1p-S9eU2cEMqww/htmlview?usp=sharing&sle=true&pru=AAABaO97sT8*M0UrblxvAxiyCj07Kh1taw#'
    
    resp=requests.get(traders_hunt_url)
    
    df=pd.read_html(resp.content)
    
    df_screen=df[1]
    df_levels=df[3]
    df_hotdeals=df[6]
    df_top10=df[7]
    df_stockoptions=df[8]
    
    df_hotdeals=df[9]
    df_option_chain=df[10]
    
    
    df_screen.columns=df_screen.iloc[0]
    df_screen.drop(df_screen.columns[0],axis=1,inplace=True)
    df_screen.drop([0],axis=0, inplace=True)
    
    
    df_screen.dropna(how='all',axis=1, inplace=True)
    df_screen.dropna(how="all",axis=0, inplace=True)
    
    df_screen['OI_Inc P_Inc']=df_screen['OI_Inc P_Inc'].str.replace('%','')
    df_screen['OI_Dec P_Dec']=df_screen['OI_Dec P_Dec'].str.replace('%','')
    OI_Inc_param=5
    OI_Dec_param=-5

    df_screen_buy=df_screen[((df_screen['PA Signal']=='MUST BUY')|(df_screen['PA Signal']=='STRONG BUY'))&(df_screen['OI_Inc P_Inc'].astype(float)>OI_Inc_param)]
    df_screen_sell=df_screen[(((df_screen['PA Signal']=='MUST SELL')|(df_screen['PA Signal']=='STRONG SELL'))
                                            &(df_screen['OI_Dec P_Dec'].astype(float)<OI_Dec_param))]
    
    #df_screen_buy=df_screen_buy[df_screen_buy['Two Day Candle Pattern'].str.contains('Bullish')].copy()
    #df_screen_sell=df_screen_sell[df_screen_sell['Two Day Candle Pattern'].str.contains('Bear')].copy()
    
    df_hotdeals_1120_1300=df_hotdeals.iloc[:,0:7]
    df_hotdeals_1415_1500=df_hotdeals.iloc[:,8:14]
    
    return list(df_screen_buy[df_screen_buy.columns[0]]),list(df_screen_sell[df_screen_sell.columns[0]])



def market_hours():
    """
    Checks whether the market is open or not
    :returns: bool variable indicating status of market. True -> Open, False -> Closed
    """
    current_time = datetime.datetime.now().time()
    # Check if the current time is in the time bracket in which NSE operates.
    # The market opens at 9:15 am
    start_time = datetime.datetime.now().time().replace(hour=9, minute=15, second=0, microsecond=0)
    # And ends at 3:30 = 15:30
    end_time = datetime.datetime.now().time().replace(hour=15, minute=30, second=0, microsecond=0)

    if current_time > start_time and current_time < end_time:
        return True

    # In case the above condition does not satisfy, the default value (False) is returned
    return False

def  moneycontrol_industry_watch():
    import requests
    import pandas as pd
    
    url_ind='https://www.moneycontrol.com/stocks/marketstats/sector-scan/nse/today.html'
    resp_ind=requests.get(url_ind)
    ind_lst=pd.read_html(resp_ind.text)
    lst_cols=[0,1,2,3,4,5,6]
    df=pd.DataFrame(columns=lst_cols)
    i=0
    while (i<len(ind_lst)):
        if (lst_cols==list(ind_lst[i].columns)):
            df=df.append(pd.DataFrame(ind_lst[i]))
            print(i)
        i=i+1
    
    df.reset_index(drop=True,inplace=True)
    
    df.columns=['Sector','MarketCap','Perc_Change','3'	,'AD_Ratio'	,'Advance',	'Decline']
    df.drop(columns=['3'],inplace=True)
    df.sort_values(by='Sector',inplace=True)
    df.dropna(inplace=True)
    return df

def moneycontrol_fut_oi_watch(mode='Up'):
    import requests
    import pandas as pd
    #mode='Up'
    url_oi_inc_p_inc='https://www.moneycontrol.com/stocks/fno/marketstats/futures/oi_inc_p_inc/homebody.php?opttopic=allfut&optinst=allfut&sel_mth=all&sort_order=1'
    url_oi_inc_p_dec='https://www.moneycontrol.com/stocks/fno/marketstats/futures/oi_inc_p_dec/homebody.php?opttopic=allfut&optinst=allfut&sel_mth=all&sort_order=0'

    if mode=='Up':
        url=url_oi_inc_p_inc
        sort_ascending=False
    else:
        url=url_oi_inc_p_dec
        sort_ascending=True

    resp_ind=requests.get(url)
    ind_lst=pd.read_html(resp_ind.text)
    lst_cols=[0,1,2,3,4,5,6,7,8,9,10,11]
    df=pd.DataFrame(columns=lst_cols)
    i=0
    while (i<len(ind_lst)):
        if (lst_cols==list(ind_lst[i].columns)):
            df=df.append(pd.DataFrame(ind_lst[i]))
            print(i)
        i=i+1
    
    df.reset_index(drop=True,inplace=True)
    
    df.columns=['Symbol','Expiry_Date','Last_Price','Price_Change','Perc_Change','HighLow','AveragePrice','Open_Interest',
                'Increase_in_OI'	,'Perc_Increase_OI','Volume','Perc_change_volume'	]
    df.drop(0,axis=0,inplace=True)
    df.drop('HighLow',axis=1,inplace=True)
    df.replace('-',0.0, inplace=True)
    df.replace('%','', inplace=True)

    df['Expiry_Date']=pd.to_datetime(df['Expiry_Date'])
    
    for col in df.columns:
        if ((col=='Symbol')| (col=='Expiry_Date')|('Perc_' in col  )):
            pass
        else:
            df[col]=pd.to_numeric(df[col])
            
    df.sort_values(by=['Perc_Increase_OI','Perc_Change'], ascending=sort_ascending ,inplace=True)
    df.dropna(inplace=True)
    return df

def nse_index_watch(symbol):

    from nsetools import Nse
    nse = Nse()
    
    '''
    q = nse.get_quote('infy') # it's ok to use both upper or lower case for codes.
    from pprint import pprint # just for neatness of display
    pprint(q)
    '''
    '''
    help(nse)
    get_top_fno_losers
    nse.get_top_fno_gainers()
    '''
    #fno_gainer = json_normalize(nse.get_fno_lot_sizes()).T
    fno_gainer = json_normalize(nse.get_advances_declines())
    fno_gainer['adratio']=fno_gainer['advances']/fno_gainer['declines']
    
    return round(float(fno_gainer[fno_gainer.indice==symbol]['adratio']),2)

def plot_corr(df,size=15):
    '''Function plots a graphical correlation matrix for each pair of columns in the dataframe.

    Input:
        df: pandas DataFrame
        size: vertical and horizontal size of the plot'''

    corr = df.corr()
    fig, ax = plt.subplots(figsize=(size, size))
    plt=ax.matshow(corr)
    plt.xticks(range(len(corr.columns)), corr.columns);
    plt.yticks(range(len(corr.columns)), corr.columns);

#plot_corr(df_nifty)


def get_latest_bhavcopy():
    
    from_period=datetime.datetime.now().date()+datetime.timedelta(days=-3)
    to_period=datetime.datetime.now().date()
    prices=None
    while from_period<=to_period:
        try:
            prices = get_price_list(dt=from_period)
            
        except:
            print (" update_bhavcopy Unexpected error:", sys.exc_info())
            pass
    
        from_period=from_period+datetime.timedelta(days=1)
        print(from_period)
    return prices

def get_high_volatility_stocks():
    df=get_latest_bhavcopy()
    df['VOLAT']=((df['HIGH']-df['LOW'])/df['LOW'])*100
    cond1=df['VOLAT']>7
    cond2=df['TOTTRDVAL']>400000000
    cond3=df['TOTALTRADES']>40000
    cond4=df['CLOSE']>100

    df2=df[cond1&cond2&cond3&cond4].copy()
    return df3



def get_top_gainer_losers_stocks():
    df=get_latest_bhavcopy()
    df['VOLAT']=((df['CLOSE']-df['PREVCLOSE'])/df['CLOSE'])*100
    cond1=((df['VOLAT']>-80)&(df['VOLAT']<80))
    cond2=df['TOTTRDVAL']>100000000
    cond3=df['TOTALTRADES']>10000
    cond4=df['CLOSE']>100

    df2=df[cond1&cond2&cond3&cond4].copy()
    df3=df2[df2['SYMBOL'].isin(lstDeriv)]
    df3=df3[(df3['VOLAT']==np.max(df3['VOLAT']))|(df3['VOLAT']==np.min(df3['VOLAT']))]
    return df3

import pandas as pd
import numpy as np
import datetime
import yfinance as yf
import glob
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
import os
#import investpy
os.chdir('/Users/sunilguglani/opt/anaconda3/lib/Algos/')
from TechnicalIndicator_Sunil import *


'''********************************'''

warnings.filterwarnings('ignore')
'''
# Function to backtest any strategy by using parameter         
    #Dataframe, 
    #strategy_type(long/short),entry_criteria,exit_criteria
    #positional_field ,price_field(example Close price),stoploss_pct(stop loss pct),target_pct,only_profit(should wait for trade to be in profit True/False)
'''

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



def backtest_strategy_stoploss(df, strategy_type,lst_entry_criteria,lst_exit_criteria, positional_field,price_field,stoploss_pct,target_pct,only_profit):
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
    
    for d in range(0,len(df)):
        entry_flag=lst_entry_criteria[d]
        exit_flag=lst_exit_criteria[d]
        curr_price=df[price_field].iloc[d]
        curr_time=df.index[d]
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
    #long['investment_period']=(long['sell_time']-long['buy_time'])

    short= pd.concat([df_buy,df_sell],axis=1,copy=True)
    
    if len(short)>0:
        if ~(short['buy_price'].iloc[-1]>0):
            short['buy_price'].iat[-1]=curr_price
            short['buy_time'].iat[-1]=curr_time
            
    short["returns"]=(short["sell_price"]-short["buy_price"])*100/short["buy_price"]
    short['cum_returns']=(short['sell_price']/short['buy_price']).cumprod()
    #short['investment_period']=(short['buy_time']-short['sell_time'])
    
    if strategy_type=='long':
        return df,long
    else:
        return df,short

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

def Data_Fetch(g,filen):
    df=consolidate_files(g,filen)
    print(df.columns)
    df.index=pd.to_datetime(df['Date'].astype(str)+' '+df['timestamp'].astype(str),format='%Y%m%d %H:%M')
    
    df.sort_index(ascending=True,inplace=True)
    
    return df

def resample_df2(df,targ_time_interval):

    df2=df.resample(targ_time_interval).agg({'Open': 'first', 
                                 'High': 'max', 
                                 'Low': 'min', 
                                 'Close': 'last',
                                 'Volume': 'sum',
                                 'OI': 'last'
                                 })
    df2.dropna(inplace=True,axis=0)
    return df2


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

def fetch_data_file(script,lookback_period=0) :
    filepath='/Users/sunilguglani/Historical_Data/j_data/'
    script='agridex'
    script=str(script)+'.NSE.csv'
    script=filepath+script
    print(script)
    df = pd.read_csv(str(script),parse_dates=True)
    df.dropna(inplace=True)

    df["Date"]=pd.to_datetime(df['Date'])
    df.index=df['Date']

    return df


def fetch_data_ncdex_file(script,lookback_period=0) :
    filepath='/Users/sunilguglani/Historical_Data/j_data/agridexanditscomponentsdata/'
    #script='AGRIDEX'
    script=str(script)+'.csv'
    script=filepath+script
    print(script)
    
    df = pd.read_csv(str(script),parse_dates=['Date'],index_col='Date')
    df.columns
    
    
    df['Open']=df['Opening Price'].copy()
    df['High']=df['HighPrice'].copy()
    df['Low']=df['LowPrice'].copy()
    df['Close']=df['Closing Price'].copy()
    df['Adjusted_close']=df['Closing Price'].copy()
    
    df.drop(columns=['Closing Price', 'Opening Price', 'HighPrice', 'LowPrice','Commodity','Expiry Date','Traded Value in lacs', 'Quantity Traded Today', 'Open Interest','Previous Closing Price'],inplace=True)
    df.dropna(inplace=True)

    return df


def fetch_data_intra_file(script):
    filen=script+'_F1.txt'
    #filen=script+'.txt'

    #filepath='//Users/sunilguglani/Downloads/IntradayData/IntradayData_FEB2020/ACC_F1.txt'
    filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2020/IntradayData_JAN2020/*'
    filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2019/*/'
    filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2019/*/'
    filepath='/Users/sunilguglani/Historical_Data/IntradayData/*/'

    g=filepath
    df=Data_Fetch(filepath,filen)
    #df=pd.read_csv(filepath+filen, engine='python')
    #df=df[df.Date>='2020-02-20']
    targ_time_interval='5min'
    df= resample_df2(df,targ_time_interval)

    return df


def fetch_data_intra_file_v2(script,freq='1min'):
    filen=script+'_F1.txt'
    #filen=script+'.txt'

    #filepath='//Users/sunilguglani/Downloads/IntradayData/IntradayData_FEB2020/ACC_F1.txt'
    #filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2020/IntradayData_MAY2020/*/'
    filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2019/*/'
    #filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2020/IntradayData_MAY2020/*'
    filepath='/Users/sunilguglani/Historical_Data/IntradayData/IntradayData_JAN_JUN2019/'

    g=filepath
    df=Data_Fetch(filepath,filen)
    #df=pd.read_csv(filepath+filen, engine='python')
    #df=df[df.Date>='2020-02-20']
    targ_time_interval=freq
    df= resample_df2(df,targ_time_interval)

    return df

def fetch_data_intra_file_v3(script,filepath,freq='1min'):
    #filen=script+'_F1.txt'
    filen=script+'.txt'

    #filepath='//Users/sunilguglani/Downloads/IntradayData/IntradayData_FEB2020/ACC_F1.txt'
    #filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2020/IntradayData_MAY2020/*/'
    #filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2020/IntradayData_MAR2020/*'
    #filepath='/Users/sunilguglani/Historical_Data/IntradayData/latest/IntradayData_2019/*/'

    g=filepath
    df=Data_Fetch(filepath,filen)
    #df=pd.read_csv(filepath+filen, engine='python')
    #df=df[df.Date>='2020-02-20']
    targ_time_interval=freq
    #df= resample_df2(df,targ_time_interval)

    return df

#pd.read_csv(filepath+filen)
   

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

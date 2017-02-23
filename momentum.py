import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
from pandas_datareader import data as d, wb as wb
import math
import datetime

end = datetime.date.today() #-datetime.timedelta(days=90)
start = end - datetime.timedelta(days=250)

listeISIN=pd.read_csv('liste.csv')
cols=listeISIN.columns
stockdata=pd.DataFrame(columns=['Code','Price','MA50','MA150','3M','6M','Ulcer'])

for index, row in listeISIN.iterrows():
    print(row['Code'])
    try:
        df=d.DataReader(row['Code'],"yahoo",start,end)
    except Exception:
        print("Error getting data from Yahoo for "+row['Code']+" between "+start+" and "+end)
    else:
        df['MA50'] = df['Close'].rolling(window=50).mean()
        df['MA150'] = df['Close'].rolling(window=150).mean()
        s1M=df.asfreq('M',method='pad')['Close'].pct_change(periods=1)
        s3M=df.asfreq('M',method='pad')['Close'].pct_change(periods=3)
        df['3M']=s3M.reindex(df.index,method='ffill')
        s6M=df.asfreq('M',method='pad')['Close'].pct_change(periods=6)
        df['6M']=s6M.reindex(df.index,method='ffill')
            
        #Ulcer 14 days
        df['max14']=df['Close'].rolling(window=14,min_periods=1).max()
        df['drawdown_perct']=100*(df['Close']-df['max14'])/df['max14']
        df['avg_sq']=df['drawdown_perct']*df['drawdown_perct']
        df['Ulcer']=df['avg_sq'].rolling(window=14).mean().apply(math.sqrt)
        #Ulcer 3 months (50 days)
        df['max50']=df['Close'].rolling(window=50,min_periods=1).max()
        df['drawdown_perct50']=100*(df['Close']-df['max50'])/df['max50']
        df['avg_sq50']=df['drawdown_perct50']*df['drawdown_perct50']
        df['Ulcer50']=df['avg_sq50'].rolling(window=50).mean().apply(math.sqrt)
        #Ulcer 1 year (200 days)
        df['max200']=df['Close'].rolling(window=200,min_periods=1).max()
        df['drawdown_perct200']=100*(df['Close']-df['max200'])/df['max200']
        df['avg_sq200']=df['drawdown_perct200']*df['drawdown_perct200']
        df['Ulcer200']=df['avg_sq200'].rolling(window=200).mean().apply(math.sqrt)
        
        stockdata=stockdata.append(
            {'Code': row['Code'],
             'Price': df['Close'].iloc[-1],
             'MA50': round(df['MA50'].iloc[-1],3),
             'MA150': round(df['MA150'].iloc[-1],3),
             '1M': round(100*s1M.iloc[-1],2),
             '3M': round(100*s3M.iloc[-1],2),
             '6M': round(100*s6M.iloc[-1],2),
             'Ulcer':round(df['Ulcer'].iloc[-1],2),
             'Ulcer50':round(df['Ulcer50'].iloc[-1],2),
             'Ulcer200':round(df['Ulcer200'].iloc[-1],2),
            },ignore_index=True)

listeISIN=listeISIN.combine_first(stockdata)[cols]
listeISIN['Note']=0.1*listeISIN['1M']+0.8*listeISIN['3M']+0.1*(1-listeISIN['Ulcer50'])
#print(listeISIN.head(3))
#print(stockdata.head(3))
#print(listeISIN.combine_first(stockdata)[cols].head(3))
print("Writing to Excel")
filename=datetime.date.today().strftime('output-%y%m%d.xlsx')
writer=pd.ExcelWriter(filename)
listeISIN.sort_values(by=['Note'],ascending=[False]).to_excel(writer,'Momentum')
writer.save()

#result=pd.merge(listeISIN,stockdata,on='Code',how='left')
#print(result.head(3))
#print(listeISIN.update(stockdata))

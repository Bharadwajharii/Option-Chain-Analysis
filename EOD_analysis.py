import datetime
import sys
flag=0
if (len(sys.argv)-1 == 0):                                                  
    datet = datetime.datetime.now()
    yester = datet - datetime.timedelta(days=1)
    flag=1
elif (len(sys.argv)-1 == 1):
    datet = datetime.datetime.now()
    x=sys.argv[1]
    d=x.split("/")
    D=int(d[0])
    M=int(d[1])
    Y=int(d[2])
    yester=datetime.datetime(Y,M,D)
    flag=1
else:
    x=sys.argv[1]
    d=x.split("/")
    D=int(d[0])
    M=int(d[1])
    Y=int(d[2])
    datet=datetime.datetime(Y,M,D)
    x=sys.argv[2]
    d=x.split("/")
    D=int(d[0])
    M=int(d[1])
    Y=int(d[2])
    yester=datetime.datetime(Y,M,D)



url1 = 'https://archives.nseindia.com/content/historical/DERIVATIVES/'+datet.strftime("%Y")+'/'+datet.strftime("%b").upper()+'/fo'+datet.strftime("%d")+''+datet.strftime("%b").upper()+''+datet.strftime("%Y")+'bhav.csv.zip'
url2 = 'https://archives.nseindia.com/content/historical/DERIVATIVES/'+yester.strftime("%Y")+'/'+yester.strftime("%b").upper()+'/fo'+yester.strftime("%d")+''+yester.strftime("%b").upper()+''+yester.strftime("%Y")+'bhav.csv.zip'
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15","Accept-Language": "en-gb","Accept-Encoding":"br, gzip, deflate","Accept":"test/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Referer":"http://www.google.com/"}
 

r = requests.get(url1,headers=headers)                          # Download Bhavcopy from NSE website
with open("fodaily.zip", "wb") as zip:
    zip.write(r.content)
    
from zipfile import ZipFile 
file_name="fodaily.zip"
with ZipFile(file_name,'r') as zip:
    zip.extractall()
    
r = requests.get(url2,headers=headers)
with open("fodaily.zip", "wb") as zip:
    zip.write(r.content)
    
file_name="fodaily.zip"
with ZipFile(file_name,'r') as zip:
    zip.extractall()

  
    
csvfile1='fo'+datet.strftime("%d")+''+datet.strftime("%b").upper()+''+datet.strftime("%Y")+'bhav.csv'
csvfile2='fo'+yester.strftime("%d")+''+yester.strftime("%b").upper()+''+yester.strftime("%Y")+'bhav.csv' 
csvout='raw'+datet.strftime("%d")+datet.strftime("%m")+datet.strftime("%y")+'.csv'

import pandas as pd
test1=pd.read_csv(csvfile1)
test2=pd.read_csv(csvfile2)
test11=test1.groupby(['SYMBOL'])
test21=test2.groupby(['SYMBOL'])
if(test11.groups.keys()!=test21.groups.keys()):                             #Cheaking for change the stock list form both days
    input(test11.groups.keys()-test21.groups.keys())
    input(test21.groups.keys()-test11.groups.keys())

import csv
item = []
with open (csvfile1) as inp1,open (csvfile2) as inp2,open(csvout,'a',newline='')as out:
    writer= csv.writer(out)
    for row in csv.reader(inp1):
        if ((row[0] != 'OPTIDX') and (row[0] != 'FUTIDX')):
            item.append(row)    
    for row in csv.reader(inp2):
        if ((row[0] != 'OPTIDX') and (row[0] != 'FUTIDX')and(row[0] != 'INSTRUMENT')):
            item.append(row)
    writer.writerows(item)

from datetime import date
from nsepy import get_history

enddata = pd.DataFrame(columns=['INSTRUMENT','DATE', 'CHANGE_IN_CE', 'CHANGE_IN_PE','CHANGE_IN_FUT_OI','CHANGE_IN_PRICE','PRICE','CHANGE_IN_VOLUME','VOLUME','DELIVERABLE','OPTION_IND','FUT_OI&PRICE_IND'])
dateparse= lambda x: pd.datetime.strptime(x, '%d-%b-%Y')
data=pd.read_csv(csvout,parse_dates=['EXPIRY_DT','TIMESTAMP'],date_parser=dateparse)
temp=data[data.INSTRUMENT=="FUTSTK"]
temp2=temp.groupby(['SYMBOL'])
tt=temp2.get_group('ACC')
import numpy
listtest=list()
for x in temp2.groups.keys():
    if(x=='PVR' or x=='M&MFIN'):
        t5=0
        t3=0
        ch_vol=0
        ch_pri=0
        dele=0
        t1=numpy.sum(tt.loc[(tt['TIMESTAMP']==datet.strftime("%x")),'CHG_IN_OI'].values)
        t2=numpy.sum(tt.loc[(tt['TIMESTAMP']==yester.strftime("%x")),'OPEN_INT'].values)
        fut=(t1/t2)
    else:
        listtest.append('No')
        d1=date(int(yester.strftime("%Y")),int(yester.strftime("%m")),int(yester.strftime("%d")))
        d2=date(int(datet.strftime("%Y")),int(datet.strftime("%m")),int(datet.strftime("%d")))
        sbin = get_history(symbol=x,start=d1,end=d2)
        fir = sbin.loc[d1]
        las = sbin.loc[d2]
        tt=temp2.get_group(x)
        t1=numpy.sum(tt.loc[(tt['TIMESTAMP']==datet.strftime("%x")),'CHG_IN_OI'].values)
        t2=numpy.sum(tt.loc[(tt['TIMESTAMP']==yester.strftime("%x")),'OPEN_INT'].values)
        fut=(t1/t2)
        t3=las['Close']
        t4=fir['Close']
        t5=las['Volume']
        t6=fir['Volume']
        ch_vol=((t5-t6)/t6)
        ch_pri=((t3-t4)/t4)
        dele=las['%Deliverble']
    if((fut>0.03) and (ch_pri>0)):
        enddata = enddata.append({'DATE' : datet.strftime("%x"),'INSTRUMENT': x, 'CHANGE_IN_CE':0.0 , 'CHANGE_IN_PE':0.0 , 'CHANGE_IN_FUT_OI': fut, 'CHANGE_IN_PRICE':ch_pri, 'PRICE':t3,'CHANGE_IN_VOLUME':ch_vol,'VOLUME':t5,'DELIVERABLE':dele ,'OPTION_IND':' ','FUT_OI&PRICE_IND':'Long'}, ignore_index=True)
    elif((fut<=-0.03) and (ch_pri<=0)):
        enddata = enddata.append({'DATE' : datet.strftime("%x"),'INSTRUMENT': x, 'CHANGE_IN_CE':0.0 , 'CHANGE_IN_PE':0.0 , 'CHANGE_IN_FUT_OI': fut, 'CHANGE_IN_PRICE':ch_pri, 'PRICE':t3,'CHANGE_IN_VOLUME':ch_vol,'VOLUME':t5,'DELIVERABLE':dele ,'OPTION_IND':' ','FUT_OI&PRICE_IND':'Long Unwinding'}, ignore_index=True)
    elif((fut>0.03) and (ch_pri<=0)):
        enddata = enddata.append({'DATE' : datet.strftime("%x"),'INSTRUMENT': x, 'CHANGE_IN_CE':0.0 , 'CHANGE_IN_PE':0.0 , 'CHANGE_IN_FUT_OI': fut, 'CHANGE_IN_PRICE':ch_pri, 'PRICE':t3,'CHANGE_IN_VOLUME':ch_vol,'VOLUME':t5,'DELIVERABLE':dele,'OPTION_IND':' ','FUT_OI&PRICE_IND':'Short'}, ignore_index=True)
    elif((fut<=-0.03) and (ch_pri>0)):
        enddata = enddata.append({'DATE' : datet.strftime("%x"),'INSTRUMENT': x, 'CHANGE_IN_CE':0.0 , 'CHANGE_IN_PE':0.0 , 'CHANGE_IN_FUT_OI': fut, 'CHANGE_IN_PRICE':ch_pri, 'PRICE':t3,'CHANGE_IN_VOLUME':ch_vol,'VOLUME':t5,'DELIVERABLE':dele ,'OPTION_IND':' ','FUT_OI&PRICE_IND':'Short Covering'}, ignore_index=True)
    else:
        enddata = enddata.append({'DATE' : datet.strftime("%x"),'INSTRUMENT': x, 'CHANGE_IN_CE':0.0 , 'CHANGE_IN_PE':0.0 , 'CHANGE_IN_FUT_OI': fut, 'CHANGE_IN_PRICE':ch_pri, 'PRICE':t3,'CHANGE_IN_VOLUME':ch_vol,'VOLUME':t5,'DELIVERABLE':dele,'OPTION_IND':' ','FUT_OI&PRICE_IND':'NA'}, ignore_index=True)
temp3=data.loc[(data['INSTRUMENT']=='OPTSTK') & (data['OPTION_TYP']=='CE')]
temp4=data.loc[(data['INSTRUMENT']=='OPTSTK') & (data['OPTION_TYP']=='PE')]
temp5=temp3.groupby(['SYMBOL'])
temp6=temp4.groupby(['SYMBOL'])
i=0
for x in temp5.groups.keys():
    tc=temp5.get_group(x)
    tp=temp6.get_group(x)
    sc1=numpy.sum(tc.loc[(tc['TIMESTAMP']==datet.strftime("%x")),'CHG_IN_OI'].values)
    sc2=numpy.sum(tc.loc[(tc['TIMESTAMP']==yester.strftime("%x")),'OPEN_INT'].values)
    sp1=numpy.sum(tp.loc[(tp['TIMESTAMP']==datet.strftime("%x")),'CHG_IN_OI'].values)
    sp2=numpy.sum(tp.loc[(tp['TIMESTAMP']==yester.strftime("%x")),'OPEN_INT'].values)
    t1=(sc1/sc2)
    t2=(sp1/sp2)
    fut=enddata.loc[(enddata['INSTRUMENT']==x),'CHANGE_IN_FUT_OI'].iloc[0]
    enddata.at[i,'CHANGE_IN_CE']=t1
    enddata.at[i,'CHANGE_IN_PE']=t2
    if((t1>t2) and (fut>=0.1)):
        enddata.at[i,'OPTION_IND']='Short Build'
    elif((t2>t1) and (fut>=0.1)):
        enddata.at[i,'OPTION_IND']='Long Build'
    else:
        enddata.at[i,'OPTION_IND']='NA'
    i=i+1
enddata.set_index("INSTRUMENT", inplace = True)
enddata.insert(11,'INDUSTRY',"No")
enddata.insert(12,'NIFTY',"No")
enddata.insert(13,'BANKNIFTY',"No")
df4=pd.read_csv('Industry.csv')
for index, row in df4.iterrows(): 
    enddata.at[row['INSTRUMENT'],'INDUSTRY']=row['INDUSTRY']
    enddata.at[row['INSTRUMENT'],'NIFTY']=row['NIFTY']
    enddata.at[row['INSTRUMENT'],'BANKNIFTY']=row['BANKNIFTY']
enddata["NIFTY"].fillna( 'NA', inplace = True) 
enddata["BANKNIFTY"].fillna('NA', inplace = True)

enddata.to_csv('enddata'+datet.strftime("%d")+datet.strftime("%m")+datet.strftime("%y")+'.csv') 
enddata.to_csv('summerycomp.csv',mode='a',header=False)    



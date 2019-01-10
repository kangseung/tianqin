#/usr/bin/env python
#  -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
from contextlib import closing
from tqsdk.api import TqApi
from tqsdk.tools.downloader import DataDownloader

api = TqApi("SIM")
df=pd.read_csv("E:/maincontract.csv")
def getdata(begindate,enddate,symbol='T1903',exchange='CFFEX'):
    InstrumentID=exchange+'.'+symbol
    startDT=datetime.strptime(begindate,'%Y-%m-%d')
    endDT=datetime.strptime(enddate,'%Y-%m-%d')
    print (InstrumentID)
    print (startDT)
    print (endDT)
    td = DataDownloader(api, symbol_list=InstrumentID, dur_sec=0,start_dt=startDT, end_dt=endDT, csv_file_name=(begindate+"_"+InstrumentID+".csv"))
    # 使用with closing机制确保下载完成后释放对应的资源
    while not td.is_finished():
        api.wait_update()
        print("progress: tick:%.2f%%" % ( td.get_progress()))
        
            
def getMainContract(symboltype):
    DF=df.loc[df["symboltype"]==symboltype,:]
    DF=DF.set_index("trading_day")
    DF.index=pd.to_datetime(DF.index,format="%Y%m%d")
    return DF

def getRange(df):
    for i in range(len(df.index)-1):
        bf=datetime.strftime(df.index[i],"%Y-%m-%d")
        aft=datetime.strftime(df.index[i+1],"%Y-%m-%d")
        
        symboltype=df.loc[bf,'symboltype']
        InstrumentID=df.loc[bf,'InstrumentID']
        print ("before:%s after:%s instrument:%s exchange:%s"%(bf,aft,symboltype,InstrumentID))
        getdata(bf,aft,'IF1812','CFFEX')
    
if __name__=='__main__':
    df=getMainContract("IF")
    getRange(df)

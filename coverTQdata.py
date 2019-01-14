# -*- coding: utf-8 -*-
"""
Spyder Editor
This is a temporary script file.
"""
import datetime as dt
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from sqlalchemy import MetaData
ipaddress="""127.0.0.1"""
engine=create_engine('postgresql+psycopg2://postgres:Bboy123456!!@%s:5432/ticks_timescale_full'%ipaddress)
''' cover TQdata to my data'''
"""
    path="E:/2016-01-05_CFFEX.T1603.csv"
    name="2016-01-05_CFFEX.T1603.csv"
"""

InstrumentIDMAPsymbol={}

def convertTQdata(path,filename):
    instrument_id=filename.split(".")[1]
    InstrumentIDMAPsymbol[instrument_id]=instrument_id
    exchangeid=filename.split(".")[0].split("_")[1]
    df=pd.read_csv(path)
    df["datetime"]=pd.to_datetime(df["datetime"])   
    df.columns=["datetime","lastprice","highest","lowest","bidprice1","bidvolume1","askprice1","askvolume1","volume","turnover","openinterest"]
    df["instrument_id"]=instrument_id
    df["exchange"]=exchangeid
    df["bidvolume5"]=0
    df["bidvolume4"]=0
    df["bidvolume3"]=0
    df["bidvolume2"]=0
    df["askvolume5"]=0
    df["askvolume4"]=0
    df["askvolume3"]=0
    df["askvolume2"]=0
    df["bidprice5"]=0
    df["bidprice4"]=0
    df["bidprice3"]=0
    df["bidprice2"]=0
    df["askprice5"]=0
    df["askprice4"]=0
    df["askprice3"]=0
    df["askprice2"]=0
    df["upperlimit"]=0.0
    df["lowerlimit"]=0.0
    df=df[["datetime","instrument_id","exchange","lastprice","volume","bidprice1","askprice1","bidvolume1","askvolume1","bidvolume5",'bidvolume4', 'bidvolume3', 'bidvolume2','bidprice5', 'bidprice4','bidprice3', 'bidprice2', 'askvolume5', 'askvolume4', 'askvolume3','askvolume2', 'askprice5', 'askprice4', 'askprice3','askprice2','openinterest','turnover', 'lowerlimit', 'upperlimit']]
    return df,instrument_id[0:2]

def insertdf(df,item):
    df=df.dropna()
    new_num =len(df.index)
    try:
        print "before insert"
        df=df.drop_duplicates('datetime')
        pd.io.sql.to_sql(df,item,con=engine,if_exists='append',index=False)
        print "%s data are inserted to %s ...."%(new_num,item)
    except Exception as e:
        try:
            print "error:%s"%e
            if df.iloc[20, 0].time().strftime("%H:%M:%S") >= "20:00:00":
                deletetrading_day = df.iloc[20, 0].date()
                print "有夜盘 %s"%deletetrading_day
            else:
                deletetrading_day = df.iloc[20, 0].date() + dt.timedelta(days=-1)
                print "没有夜盘 %s"%deletetrading_day
            command = "delete from \"%s\" where datetime>='%s 20:00:00' and datetime<='%s 15:30:00' " % (
            item, deletetrading_day,df.iloc[-20, 0].date())
            print command
            engine.execute(command)
            pd.io.sql.to_sql(df, item, con=engine, if_exists='append', index=False)
            print "%s data are inserted to %s ...." % (new_num, item)
        except Exception as e:
            print "error again"
            print e
            print df
            print "-----------------------"
            print df.iloc[20, 0]
            assert(False)

if __name__=="__main__":
    dirpath="/home/jiangsheng/Documents/IFALL"
    pathlist=os.listdir(dirpath)
    pathlist=sorted(pathlist)
    for i in range(0,len(pathlist)):
        path=os.path.join(dirpath,pathlist[i])
        if not os.path.isdir(path):
            df,item=convertTQdata(path,pathlist[i])
            insertdf(df,item)
        else:
            filelist=os.listdir(path)
            for j in range(0,len(filelist)):
                targetpath = os.path.join(path, filelist[j])
                name=os.path.basename(targetpath)
                df,item=convertTQdata(targetpath,name)
                insertdf(df,item)
print "all data finished!!"

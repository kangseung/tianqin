# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd
import numpy as np
import os
''' cover TQdata to my data'''
"""
    path="E:/2016-01-05_CFFEX.T1603.csv"
    name="2016-01-05_CFFEX.T1603.csv"
"""
def convertTQdata(path,filename):
    instrument_id=filename.split(".")[1]
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
    return df

if __name__=="__main__":
    dirpath="E:/"
    pathlist=os.listdir(dirpath)
    pathlist=sorted(pathlist)
    for i in range(0,len(pathlist)):
        path=os.path.join(dirpath,pathlist[i])
        if not os.path.isdir(path):
            df=convertTQdata(path,pathlist[i])
            print df
            assert(False)

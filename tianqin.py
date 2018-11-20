#!/usr/bin/env python
#  -*- coding: utf-8 -*-
from datetime import datetime
from contextlib import closing
from tqsdk.api import TqApi
from tqsdk.tools.downloader import DataDownloader

api = TqApi("SIM")

def getdata(begindate,enddate,symbol='T1903',exchange='CFFEX'):
    InstrumentID=exchange+'.'+symbol
    startDT=datetime.strptime(begindate,'%Y-%m-%d')
    endDT=datetime.strptime(enddate,'%Y-%m-%d')
    print (InstrumentID)
    print (startDT)
    print (endDT)
    td = DataDownloader(api, symbol_list=InstrumentID, dur_sec=0,start_dt=startDT, end_dt=endDT, csv_file_name=(InstrumentID+"tick.csv"))
    # 使用with closing机制确保下载完成后释放对应的资源
    with closing(api):
        while not td.is_finished():
            api.wait_update()
            print("progress: tick:%.2f%%" % ( td.get_progress()))
            
if __name__=='__main__':
    getdata('2018-11-18','2018-11-20','jd1901','DCE')
# /usr/bin/env python
#  -*- coding: utf-8 -*-
import os
import pandas as pd
from datetime import datetime
from contextlib import closing
from tqsdk.api import TqApi
from tqsdk.tools.downloader import DataDownloader
from sqlalchemy import create_engine
from sqlalchemy import MetaData
ipaddress="192.168.1.118"
engine=create_engine('postgresql+psycopg2://postgres:Bboy123456!!@%s:5432/indexdb'%ipaddress)
api = TqApi("SIM")
exchangemap = {"a": "DCE",
               "ag": "SHFE",
               "al": "SHFE",
               "au": "SHFE",
               "b": "DCE",
               "bb": "DCE",
               "bu": "SHFE",
               "c": "DCE",
               "CF": "CZCE",
               "cs": "DCE",
               "cu": "SHFE",
               "fb": "DCE",
               "FG": "CZCE",
               "fu": "SHFE",
               "hc": "SHFE",
               "i": "DCE",
               "IC": "CFFEX",
               "IF": "CFFEX",
               "IH": "CFFEX",
               "j": "DCE",
               "jd": "DCE",
               "jm": "DCE",
               "JR": "CZCE",
               "l": "DCE",
               "LR": "CZCE",
               "m": "DCE",
               "MA": "CZCE",
               "ni": "SHFE",
               "OI": "CZCE",
               "p": "DCE",
               "pb": "SHFE",
               "PM": "CZCE",
               "pp": "DCE",
               "rb": "SHFE",
               "RI": "CZCE",
               "RM": "CZCE",
               "RS": "CZCE",
               "ru": "SHFE",
               "SF": "CZCE",
               "SM": "CZCE",
               "sn": "SHFE",
               "SR": "CZCE",
               "T": "CFFEX",
               "TA": "CZCE",
               "TS": "CFFEX",
               "TF": "CFFEX",
               "v": "DCE",
               "WH": "CZCE",
               "wr": "DCE",
               "y": "DCE",
               "ZC": "CZCE",
               "zn": "SHFE",
               "AP": "CZCE",
               "sc": "INE",
               "sp": "SHFE",
               "eg": "DCE"}

command="""SELECT trading_day,  symboltype,symbol FROM  public."data_sep_1D" where trading_day='20181121' group by trading_day,symboltype,symbol order by trading_day; """
df = pd.read_sql(command,engine)
trading_dayDF = pd.read_csv("E:/tradingday.csv")
trading_dayDF = trading_dayDF.set_index("trading_day")
trading_dayDF.index = pd.to_datetime(trading_dayDF.index, format='%Y%m%d')
trading_dayDF = trading_dayDF.loc["2018-11-20":"2018-11-21"]
symbollist = ['JR',
              'WH',
              'FG',
              'cs',
              'cu',
              'pp',
              'RS',
              'pb',
              'RM',
              'RI',
              'AP',
              'CF',
              'T',
              'hc',
              'b',
              'sc',
              'OI',
              'SR',
              'j',
              'l',
              'p',
              'SM',
              'v',
              'TA',
              'SF',
              'zn',
              'ag',
              'eg',
              'al',
              'au',
              'ru',
              'TS',
              'LR',
              'rb',
              'TF',
              'PM',
              'fu',
              'bb',
              'ni',
              'ZC',
              'bu',
              'fb',
              'wr',
              'jd',
              'IC',
              'IF',
              'a',
              'c',
              'MA',
              'IH',
              'i',
              'jm',
              'sp',
              'm',
              'sn',
              'y']

CZCElist=["AP","CF","FG","MA","OI","RM","SF","SM","SR","TA","ZC"]

def getdata(startDT, endDT, InstrumentID='T1903', exchange='CFFEX', symboltype='T'):
    startDT = datetime.strptime(startDT, "%Y-%m-%d %H:%M:%S")
    endDT = datetime.strptime(endDT, "%Y-%m-%d %H:%M:%S")
    if not os.path.exists(symboltype):
        os.mkdir(symboltype)
    print("symboltype:%s startDT%s endDT%s" % (symboltype, startDT, endDT))
    td = DataDownloader(api, symbol_list=InstrumentID, dur_sec=0, start_dt=startDT, end_dt=endDT, csv_file_name=(
            symboltype + "/" + datetime.strftime(endDT, "%Y-%m-%d") + "_" + InstrumentID + ".csv"))

    while not td.is_finished():
        api.wait_update()
        # print("progress: tick:%.2f%%" % (td.get_progress()))


def getMainContract(symboltype):
    DF = df.loc[df["symboltype"] == symboltype, :]
    DF = DF.set_index("trading_day")
    DF.index = pd.to_datetime(DF.index, format="%Y%m%d")
    return DF


def getRange(df):
    for i in range(len(df.index) - 1):
        bf = datetime.strftime(df.index[i], "%Y-%m-%d")
        trading_day = datetime.strftime(df.index[i + 1], "%Y-%m-%d")
        for symboltype in symbollist:
            symbolMainDF = getMainContract(symboltype)
            if symbolMainDF.empty:
                print("symbolMainDFempty! symbol:%s" % symboltype)
            else:
                try:
                    InstrumentIDSeries = symbolMainDF.loc[trading_day, "symbol"]
                    InstrumentIDSerieslist=[]
                    for j in range(len(InstrumentIDSeries.index)):
                        symbol=InstrumentIDSeries[j]
                        exchange=exchangemap[symboltype]
                        InstrumentID = exchange + '.' +  symbol
                        if symboltype in CZCElist:
                            InstrumentID = exchange + '.' + symbol[0:2] + symbol[3:]
                        InstrumentIDSerieslist.append(InstrumentID)
                    for InstrumentID in InstrumentIDSerieslist:
                        getdata(bf + " 20:59:59", trading_day + " 15:15:00", InstrumentID, exchangemap[symboltype],
                            symboltype)
                except Exception as e:
                    print (e)
                    print("trading_day%s not in DF!!!!!!!!!!!!!!!!!!!!!!!!!!!! symbol:%s" % (trading_day, symboltype))


if __name__ == '__main__':
    getRange(trading_dayDF)

# /usr/bin/env python
#  -*- coding: utf-8 -*-
import os
import re
import pandas as pd
from datetime import datetime
from contextlib import closing
from tqsdk.api import TqApi
from tqsdk.tools.downloader import DataDownloader

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

df = pd.read_csv("D:/TQDATA/maincontract.csv")
df=df[["trading_day","instrument_id","symboltype"]]
trading_dayDF = pd.read_csv("tradingday.csv")
trading_dayDF = trading_dayDF.set_index("trading_day")
trading_dayDF.index = pd.to_datetime(trading_dayDF.index, format='%Y%m%d')
# trading_dayDF = trading_dayDF.loc["2018-11-09":"2019-02-18"]
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


def getdata(startDT, endDT, symbol='T1903', exchange='CFFEX', symboltype='T'):
    startDT = datetime.strptime(startDT, "%Y-%m-%d %H:%M:%S")
    endDT = datetime.strptime(endDT, "%Y-%m-%d %H:%M:%S")
    InstrumentID = exchange + '.' + symbol
    if not os.path.exists(symboltype):
        os.mkdir(symboltype)
    print("symboltype:%s startDT%s endDT%s" % (symboltype, startDT, endDT))
    td = DataDownloader(api, symbol_list=InstrumentID, dur_sec=0, start_dt=startDT, end_dt=endDT, csv_file_name=(
            symboltype + "/" + datetime.strftime(endDT, "%Y-%m-%d") + "_" + InstrumentID + ".csv"))

    while not td.is_finished():
        api.wait_update()
        # print("progress: tick:%.2f%%" % (td.get_progress()))

def getpreTradingday(tradingday):
    return trading_dayDF[trading_dayDF.index<tradingday].index[-1]

def regexSymbolHead(symbol):
    p = re.compile('\\d')
    return p.sub("", symbol)
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
                    InstrumentID = symbolMainDF.loc[trading_day, "instrument_id"]
                    getdata(bf + " 20:59:59", trading_day + " 15:15:00", InstrumentID, exchangemap[symboltype],
                            symboltype)
                except:
                    print("trading_day%s not in DF!!!!!!!!!!!!!!!!!!!!!!!!!!!! symbol:%s" % (trading_day, symboltype))

def getdownloadlist():
    downloadlist=pd.read_csv("downloadlist.csv")
    downloadlist.columns = ['trading_day', 'symbol']
    downloadlist=downloadlist.dropna()
    for index in downloadlist.index:
        trading_day=downloadlist.loc[index,"trading_day"]
        bf = datetime.strftime(getpreTradingday(trading_day), "%Y-%m-%d")
        InstrumentID=downloadlist.loc[index,"symbol"]
        symboltype=regexSymbolHead(InstrumentID)
        getdata(bf + " 20:59:59", trading_day + " 15:15:00", InstrumentID, exchangemap[symboltype],
                symboltype)


if __name__ == '__main__':
    # getRange(trading_dayDF)
    getdownloadlist()

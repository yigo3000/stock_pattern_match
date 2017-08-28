import sqlite3 as lite
from pandas.io import sql
import pandas as pd
import numpy as np
import tushare as ts
from datetime import date
from datetime import timedelta
import talib as ta

PATTERN_DAYS  = 20 #取最近多少条日K线、周k线
DAY_PRICE_AVARAGES = (5,10,30,90) #取哪些日价格均线
DAY_VOLUME_AVARAGES = (5,90) #取哪些日成交量均线
WEEK_PRICE_AVARAGES = () #取哪些周均线
WEEK_VOLUME_AVARAGES = ()

def _date_to_str(date_python):#
    return date_python.strftime('%Y-%m-%d')

def _get_price(code, date):
    pass

def _compte_hash():
    pass
def main():
    #按照日期和code取数据
    one_code = "600050"
    one_date = date(year=2017,month=8,day=25)
    one_date_str = _date_to_str(one_date)
    data_valid = False
    #计算需要多少交易日的k线
    max_days = 0
    if(DAY_PRICE_AVARAGES!=()):
        max_days = DAY_PRICE_AVARAGES[-1]+PATTERN_DAYS
    if(WEEK_PRICE_AVARAGES!=()):
        tmp = (WEEK_PRICE_AVARAGES[-1] + PATTERN_DAYS) * 5
        if(max_days<tmp):
            max_days = tmp

    #从tushare取k线
    need_more_ks = max_days
    days_between_start_end = 0 #计数，累计往前取了多少天

    while (need_more_ks>0):
        days_between_start_end += need_more_ks * 2  # 计数，累计往前多取了多少天
        startday = one_date - timedelta(days_between_start_end)
        prices = ts.get_k_data(one_code, ktype='d', autype='qfq', index=False,
                               start=_date_to_str(startday), end=one_date_str)
        if (prices.iloc[-1, 0] != one_date_str):
            print("不是交易日")
            data_valid = False
            break
        need_more_ks = max_days - prices.index.size
        if(days_between_start_end>365):
            print("向前找不到数据了")
            data_valid = False
            break
    #计算价格移动均线
    price_MA = np.empty(0)
    for ma in DAY_PRICE_AVARAGES:
        price_MA = np.append(MA,ta.SMA(prices['close'].values, ma)[-1*PATTERN_DAYS:])
    mean = np.mean(price_MA)
    MA = price_MA<mean #得到了平均哈希向量

    volume_MA =  np.empty(0)
    for ma in DAY_VOLUME_AVARAGES:
        volume_MA = np.append(MA,ta.SMA(prices['volume'].values, ma)[-1*PATTERN_DAYS:])
    mean = np.mean(volume_MA)
    MA = volume_MA<mean #得到了平均哈希向量

    #input_k =

    #origin_data_price = _get_price()
    #origin_data_volume = _get_volume()
    #对数据进行平均哈希
    #hash_data = _compute_hash(origin_data_price)
    #保存pattern在数据库中

if __name__=="__main__":
    main()

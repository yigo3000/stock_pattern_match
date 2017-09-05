import sqlite3 as lite
from pandas.io import sql
import pandas as pd
import numpy as np
import tushare as ts
from datetime import date
from datetime import timedelta
import talib as ta

PATTERN_DAYS  = 20 #取最近多少条日K线、周k线
DAY_PRICE_AVARAGES = (5,10,30,60,90) #取哪些日价格均线
DAY_VOLUME_AVARAGES = (5,90) #取哪些日成交量均线
WEEK_PRICE_AVARAGES = () #取哪些周均线
WEEK_VOLUME_AVARAGES = ()
def _datelist(start, end, str=False):
    start_date = date(*start)
    end_date = date(*end)

    result = []
    curr_date = start_date
    while curr_date != end_date:
        if(str):
            result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
        else:
            result.append(curr_date)
        curr_date += timedelta(1)
    if(str):
        result.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
    else:
        result.append(curr_date)
    return result

def _date_to_str(date_python):#
    return date_python.strftime('%Y-%m-%d')

def _get_price_and_volume_ts(code, one_date):
    '''
    :param code: str，股票代码
    :param date: python date
    :return: 如果交易日不存在，返回（None，None）；如果部分数据不存在，也返回（None，None）。
    正常情况：返回两个numpy的ndarray
    [20日k最高价，
    20日k开盘价，
    20日k收盘价，
    20日k最低价，
    20日k的移动均线*len（DAY_PRICE_AVARAGES），周期由DAY_PRICE_AVARAGES设定
    20周k线最高价，
    20周k线开盘价，
    20周k线收盘价，
    20周k线最低价]

    [20日成交量，
    20日的成交量均线*len（DAY_VOLUME_AVARAGES)，周期有DAY_VOLUME_AVARAGES设定]
    '''
    result_prices = np.empty(0)
    result_volumes = np.empty(0)

    #按照日期和code取数据
    one_date_str = _date_to_str(one_date)
    data_valid = False
    #计算需要多少交易日的k线
    max_days = 0
    if(DAY_PRICE_AVARAGES!=()):
        max_days = DAY_PRICE_AVARAGES[-1]+PATTERN_DAYS

    #从tushare取k线
    need_more_ks = max_days
    days_between_start_end = 0 #计数，累计往前取了多少天

    while (need_more_ks>0):
        days_between_start_end += need_more_ks * 2  # 计数，累计往前多取了多少天
        startday = one_date - timedelta(days_between_start_end)
        tushare_raw_day = ts.get_k_data(code, ktype='d', autype='qfq', index=False,
                               start=_date_to_str(startday), end=one_date_str)
        if (tushare_raw_day.iloc[-1, 0] != one_date_str):
            print("不是交易日")
            #data_valid = False
            return None,None
        need_more_ks = max_days - tushare_raw_day.index.size
        if(days_between_start_end>365):
            print("向前找不到数据了")
            #data_valid = False
            return None,None
    #取价格向量
    result_prices = np.append(result_prices,tushare_raw_day['high'].values[-1*PATTERN_DAYS:])
    result_prices = np.append(result_prices,tushare_raw_day['open'].values[-1*PATTERN_DAYS:])
    result_prices = np.append(result_prices,tushare_raw_day['close'].values[-1*PATTERN_DAYS:])
    result_prices = np.append(result_prices,tushare_raw_day['low'].values[-1*PATTERN_DAYS:])
    #计算价格移动均线
    #price_MA = np.empty(0)
    for ma in DAY_PRICE_AVARAGES:
        result_prices = np.append(result_prices,ta.SMA(tushare_raw_day['close'].values, ma)[-1*PATTERN_DAYS:])

    #取周线
    #计算需要多少周的k线
    max_weeks = 0
    if(WEEK_PRICE_AVARAGES!=()):
        tmp = WEEK_PRICE_AVARAGES[-1] + PATTERN_DAYS
        if(max_weeks<tmp):
            max_weeks = tmp
    else:
        max_weeks = PATTERN_DAYS
    #从tushare取k线
    need_more_ks = max_weeks*7
    days_between_start_end = 0 #计数，累计往前取了多少天

    while (need_more_ks>0):
        days_between_start_end += need_more_ks * 2  # 计数，累计往前多取了多少天
        startday = one_date - timedelta(days_between_start_end)
        tushare_raw_week = ts.get_k_data(code, ktype='w', autype='qfq', index=False,
                               start=_date_to_str(startday), end=one_date_str)
        need_more_ks = max_days - tushare_raw_week.index.size
        if(days_between_start_end>730):
            print("向前找不到数据了")
            #data_valid = False
            return None,None
    #取价格向量
    result_prices = np.append(result_prices,tushare_raw_week['high'].values[-1*PATTERN_DAYS:])
    result_prices = np.append(result_prices,tushare_raw_week['open'].values[-1*PATTERN_DAYS:])
    result_prices = np.append(result_prices,tushare_raw_week['close'].values[-1*PATTERN_DAYS:])
    result_prices = np.append(result_prices,tushare_raw_week['low'].values[-1*PATTERN_DAYS:])
    #计算价格移动均线
    for ma in WEEK_PRICE_AVARAGES:
        result_prices = np.append(result_prices,ta.SMA(tushare_raw_day['close'].values, ma)[-1*PATTERN_DAYS:])

    result_volumes = np.append(result_volumes,tushare_raw_day['volume'].values)
    for ma in DAY_VOLUME_AVARAGES:
        result_volumes = np.append(result_volumes,ta.SMA(tushare_raw_day['volume'].values, ma)[-1*PATTERN_DAYS:])

    return result_prices,result_volumes

def _compte_hash():
    pass
def main():
    #按照日期和code取数据
    hs300s = ts.get_sme_classified()
    date_list = _datelist((2014,1,1),(2017,1,1))
    for one_code in hs300s['code']:
        for one_date in date_list:
            prices,volumes = _get_price_and_volume_ts(one_code,one_date)
            if(prices is not None):
                prices = prices>np.mean(prices)
                volumes = volumes>np.mean(volumes)

if __name__=="__main__":
    main()

#  -*- coding: utf-8 -*-


import tushare as ts
from datetime import datetime

from pymongo import MongoClient
from pymongo import UpdateOne
# 指定数据库的连接，quant_01是数据库名
DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_01']

"""
从tushare获取日K数据，保存到本地的MongoDB数据库中
"""
tushare_token = '8a384289ed0a733ad7a7bc4193429df33429fdca53435b4937f8cbc2'

class DailyCrawler:
    def __init__(self):
        """
        初始化
        """

        # 创建daily数据集
        self.daily = DB_CONN['daily']
        # 创建daily_hfq数据集
        self.daily_hfq = DB_CONN['daily_hfq']

    def crawl_index(self, begin_date=None, end_date=None):
        """
        抓取指数的日K数据。
        指数行情的主要作用：
        1. 用来生成交易日历
        2. 回测时做为收益的对比基准

        :param begin_date: 开始日期
        :param end_date: 结束日期
        """
        ts.set_token(tushare_token)
        pro = ts.pro_api()    # 初始化新接口
        # 指定抓取的指数列表，可以增加和改变列表里的值
        index_codes = ['000001', '000300', '399001', '399005', '399006']

        # 当前日期
        now = datetime.now().strftime('%Y-%m-%d')
        # 如果没有指定开始，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日，则默认为当前日期
        if end_date is None:
            end_date = now

        # 按照指数的代码循环，抓取所有指数信息
        for code in index_codes:
            # 老接口获取数据
            # # 抓取一个指数的在时间区间的数据
            # df_daily = ts.get_k_data(code, index=True, start=begin_date, end=end_date)
            # print(code,df_daily)
            # # 保存数据
            # # self.save_data(code, df_daily, self.daily, {'index': True})


            # pro = ts.pro_api()    # 新接口
            df_daily = pro.index_daily(ts_code=code, start_date=begin_date, end_date=end_date)
            print(code,df_daily)

    def save_data(self, code, df_daily, collection, extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        """

        # 数据更新的请求列表
        update_requests = []

        # 将DataFrame中的行情数据，生成更新数据的请求
        for df_index in df_daily.index:
            # 将DataFrame中的一行数据转dict
            doc = dict(df_daily.loc[df_index])
            # 设置股票代码
            doc['code'] = code

            # 如果指定了其他字段，则更新dict
            if extra_fields is not None:
                doc.update(extra_fields)

            # 生成一条数据库的更新请求
            # 注意：
            # 需要在code、date、index三个字段上增加索引，否则随着数据量的增加，
            # 写入速度会变慢，创建索引的命令式：
            # db.daily.createIndex({'code':1,'date':1,'index':1})
            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
                    {'$set': doc},
                    upsert=True)
            )

        # 如果写入的请求列表不为空，则保存都数据库中
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('保存日线数据，代码： %s, 插入：%4d条, 更新：%4d条' %
                  (code, update_result.upserted_count, update_result.modified_count),
                  flush=True)

    def crawl(self, begin_date=None, end_date=None):
        """
        抓取股票的日K数据，主要包含了不复权和后复权两种

        :param begin_date: 开始日期
        :param end_date: 结束日期
        """

        # 通过tushare的基本信息API，获取所有股票的基本信息
        stock_df = ts.get_stock_basics()
        # 将基本信息的索引列表转化为股票代码列表
        codes = list(stock_df.index)

        # 当前日期
        now = datetime.now().strftime('%Y-%m-%d')

        # 如果没有指定开始日期，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日期，则默认为当前日期
        if end_date is None:
            end_date = now

        for code in codes:
            # 抓取不复权的价格
            df_daily = ts.get_k_data(code, autype=None, start=begin_date, end=end_date)
            self.save_data(code, df_daily, self.daily, {'index': False})

            # 抓取后复权的价格
            df_daily_hfq = ts.get_k_data(code, autype='hfq', start=begin_date, end=end_date)
            self.save_data(code, df_daily_hfq, self.daily_hfq, {'index': False})


    def crawl_one(self, code,begin_date=None, end_date=None):
        # 当前日期
        now = datetime.now().strftime('%Y-%m-%d')

        # 如果没有指定开始日期，则默认为当前日期
        if begin_date is None:
            begin_date = now

        # 如果没有指定结束日期，则默认为当前日期
        if end_date is None:
            end_date = now
        pro = ts.pro_api(tushare_token)
        df_daily = pro.daily(ts_code=code, start_date=begin_date, end_date=end_date)
        print(type(df_daily))
        print(df_daily.info)
        # df_daily.to_csv("tzzs_data2.csv", index_label="index_label")
        # df_daily.to_json(code+'.json')
        df_daily.to_excel(code + '.xls')

    def handle_json_data(self,data):
        #data = {"ts_code":{"0":"600216.SH","1":"600216.SH","2":"600216.SH","3":"600216.SH","4":"600216.SH","5":"600216.SH","6":"600216.SH"},"trade_date":{"0":"20200901","1":"20200831","2":"20200828","3":"20200827","4":"20200826","5":"20200825","6":"20200824"},"open":{"0":18.5,"1":18.45,"2":17.92,"3":17.68,"4":18.5,"5":19.01,"6":18.05},"high":{"0":19.07,"1":18.68,"2":18.33,"3":18.02,"4":18.5,"5":19.29,"6":19.18},"low":{"0":18.22,"1":18.21,"2":17.78,"3":17.54,"4":17.63,"5":18.52,"6":18.05},"close":{"0":18.88,"1":18.25,"2":18.26,"3":17.83,"4":17.68,"5":18.8,"6":18.95},"pre_close":{"0":18.25,"1":18.26,"2":17.83,"3":17.68,"4":18.8,"5":18.95,"6":18.03},"change":{"0":0.63,"1":-0.01,"2":0.43,"3":0.15,"4":-1.12,"5":-0.15,"6":0.92},"pct_chg":{"0":3.4521,"1":-0.0548,"2":2.4117,"3":0.8484,"4":-5.9574,"5":-0.7916,"6":5.1026},"vol":{"0":379440.82,"1":267232.86,"2":186868.71,"3":129993.0,"4":439006.95,"5":305823.19,"6":446786.7},"amount":{"0":709945.115,"1":493568.484,"2":338760.169,"3":231598.842,"4":786573.735,"5":576512.073,"6":835461.26}}
        handle_data = {}
        index = []
        for k,v in data['trade_date'].items():
            index.append(k)
        for i in range(len(index)):
            tmp = {}
            for k,v in data.items():
                tmp[k] = v[str(i)]
            # print(tmp)
            trade_date = data['trade_date'][str(i)]
            handle_data[trade_date] = tmp
        print(handle_data)


# 抓取程序的入口函数
if __name__ == '__main__':
    dc = DailyCrawler()
    # 抓取指定日期范围的指数日行情
    # 这两个参数可以根据需求改变，时间范围越长，抓取时花费的时间就会越长
    # dc.crawl_index('2020-08-24', '2020-08-28')
    # # 抓取指定日期范围的股票日行情
    # # 这两个参数可以根据需求改变，时间范围越长，抓取时花费的时间就会越长
    # dc.crawl('2020-08-24', '2020-08-28')

    # 获取一只股票的行情数据
    # dc.crawl_one('600216.SH', '20200824','')
    data = {"ts_code":{"0":"600216.SH","1":"600216.SH","2":"600216.SH","3":"600216.SH","4":"600216.SH","5":"600216.SH","6":"600216.SH"},"trade_date":{"0":"20200901","1":"20200831","2":"20200828","3":"20200827","4":"20200826","5":"20200825","6":"20200824"},"open":{"0":18.5,"1":18.45,"2":17.92,"3":17.68,"4":18.5,"5":19.01,"6":18.05},"high":{"0":19.07,"1":18.68,"2":18.33,"3":18.02,"4":18.5,"5":19.29,"6":19.18},"low":{"0":18.22,"1":18.21,"2":17.78,"3":17.54,"4":17.63,"5":18.52,"6":18.05},"close":{"0":18.88,"1":18.25,"2":18.26,"3":17.83,"4":17.68,"5":18.8,"6":18.95},"pre_close":{"0":18.25,"1":18.26,"2":17.83,"3":17.68,"4":18.8,"5":18.95,"6":18.03},"change":{"0":0.63,"1":-0.01,"2":0.43,"3":0.15,"4":-1.12,"5":-0.15,"6":0.92},"pct_chg":{"0":3.4521,"1":-0.0548,"2":2.4117,"3":0.8484,"4":-5.9574,"5":-0.7916,"6":5.1026},"vol":{"0":379440.82,"1":267232.86,"2":186868.71,"3":129993.0,"4":439006.95,"5":305823.19,"6":446786.7},"amount":{"0":709945.115,"1":493568.484,"2":338760.169,"3":231598.842,"4":786573.735,"5":576512.073,"6":835461.26}}
    dc.handle_json_data(data)

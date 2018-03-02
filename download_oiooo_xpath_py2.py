#!/usr/bin/env python
#-*- coding:utf-8 -*-
#Author:GuoLikai@2018-02-08 13:49:53

import requests
import json
from lxml import etree


def download():
    url = 'http://www.oiooo.net/19/19941/index.html'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.73 Safari/537.36'}
    response = requests.get(url,headers)
    html = response.content
    selector = etree.HTML(html)
    #result_lists = selector.xpath('//div[@class="chapterlist"]/ul/li/a')
    href_lists = selector.xpath('//div[@class="chapterlist"]/ul/li/a/@href')
    theme_lists = selector.xpath('//div[@class="chapterlist"]/ul/li/a/text()')
    items = []
    for href,theme in zip(href_lists,theme_lists):
        item = {}
        themeurl = "http://www.oiooo.net/19/19941/" + href
        item['href'] = themeurl
        item['theme'] = theme
        print json.dumps(items,ensure_ascii=False)
        items.append(item)
    output =open('test.json','w')
    data = json.dumps(items,ensure_ascii=False)
    output.write(data.encode('utf-8'))
    output.close()

if __name__ == "__main__":
    download()

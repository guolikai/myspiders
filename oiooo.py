#!/usr/bin/env python
#-*- coding:utf-8 -*-
#Author:GuoLikai@2018-02-08 13:49:53

import requests
import json
import re
from bs4 import BeautifulSoup



def download():
    url = 'http://www.oiooo.net/17/17222/index.html'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.73 Safari/537.36'}
    response = requests.get(url,headers)
    #html_parser = HTMLParser.HTMLParser()
    #txt = html_parser.unescape(res.content)
    #print response.headers['X-Powered-By']
    #print respose.headers
    #print response.json
    html = response.content
    bs = BeautifulSoup(html,'lxml')
    #print bs.prettify()
    #print bs.title
    #print bs.a
    #print bs.a.name
    #print bs.a.attrs['href']
    #print bs.a.string
    result_lists = bs.find_all('li')
    #result_lists = bs.select('li a')
    print result_lists
#    items = {}
#    for site in result_lists[10]:
#        item = {}
#        href = site.get('href')
#        themeurl = "http://www.oiooo.net/17/17222/" + href
#        theme = site.get_text()
#        item['href'] = themeurl
#        item['theme'] = theme
#        print json.dumps(item,ensure_ascii=False)
    
if __name__ == "__main__":
    download()

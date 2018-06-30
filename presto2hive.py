#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat May  5 08:07:15 2018

@author: newchama
"""
import re
#

import time
import datetime

ts = time.time()
st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H-%M-%S')

sql = ‘’’place your sql text here’’’

def cast_mapping(func,pam_list):
    cast_mapping = {'varchar':'string'}
    data = pam_list[0] # 被转换对象
    data_type = pam_list[1] # 目标抓换类型
    hive_func = str(func) 
    for item in cast_mapping.keys():
        if item in data_type:
            data_type = cast_mapping[item]
    hive_pam_txt = data + ' as ' + data_type
    return new_func_builder(hive_func,hive_pam_txt)  

def date_trunc_mapping(func,pam_list):
    print pam_list
    unit = pam_list[0].replace("'","")
    print unit
    time_data = pam_list[1]
    if unit == 'week':
        hive_func = 'date_sub'
        if 'substr'not in time_data and 'date' not in time_data:
            time_data = 'date('+time_data+')'
        hive_pam_txt = "{},pmod(datediff({},'1900-01-07'),7)".format(time_data,time_data)
    elif unit == 'hour':
        hive_func = 'from_unixtime'
        hive_pam_txt = "cast({} as timestamp),'yyyy-MM-dd HH'"
    elif unit == 'minute':
        hive_func = 'from_unixtime'
        hive_pam_txt = "cast({} as timestamp),'yyyy-MM-dd HH:mm'"
    elif unit == 'month':
        hive_func = 'date_add'
        hive_pam_txt = '{},-dayofmonth({})+1'.format(time_data,time_data)
    else :
        hive_func = 'date_trunc'
        hive_pam_txt = "'{}',{}".format(unit,time_data)
    return new_func_builder(hive_func,hive_pam_txt)  

def date_diff_mapping(func,pam_list):
   unit = pam_list[0].replace("'","")
   start_dte = pam_list[1]
   end_dte = pam_list[2]
   if unit == 'day':
       hive_func = 'datediff'
       hive_pam_txt = 'substr(cast({} as string),1,10),substr(cast({} as string),1,10)'.format(end_dte,start_dte)
   elif unit == 'month':
       hive_func = ''
       hive_pam_txt = "int(substr(cast({} as string),6,2)) - int(substr(cast({} as string),6,2))  + 12 * (int(substr(cast({} as string),1,4)) - int(substr(cast({} as string),1,4)))".format(end_dte,start_dte,end_dte,start_dte)
   return new_func_builder(hive_func,hive_pam_txt)  

def date_add_mapping(func,pam_list):
   unit = pam_list[0].replace("'","")
   print unit
   plus_num= pam_list[1]
   date_data = pam_list[2]
   if unit == 'day':
       hive_func = 'date_add'
       hive_pam_txt = '{},{}'.format(date_data,plus_num)
   elif unit == 'month':
       hive_func = 'add_months'
       hive_pam_txt = '{},{}'.format(date_data,plus_num)
   elif unit == 'week':
       hive_func = 'date_add'
       hive_pam_txt = '{},{}'.format(date_data,str(int(plus_num)*7))
   else:
       hive_func = func
       hive_pam_txt = ''
       for item in pam_list:
           hive_pam_txt += item +','
       hive_pam_txt = hive_pam_txt[:-1]
   return new_func_builder(hive_func,hive_pam_txt)  

func_nam_list = {'cast':1,
                'date_diff':3,
                'date_trunc':2,
                'date_add':2}

def func_mapping(func,pam_list): # 根据函数名匹配hive to presto的映射函数
    func_txt = ''
    if func == 'cast':
        func_txt = cast_mapping(func,pam_list)
    elif func == 'date_trunc':
        func_txt = date_trunc_mapping(func,pam_list)
    elif func == 'date_diff':
        func_txt = date_diff_mapping(func,pam_list)
    elif func == 'date_add':
        func_txt = date_add_mapping(func,pam_list)
    else:
        pam_txt = ''
        for item in pam_list:
            pam_txt += item + ','
            func_txt = new_func_builder(func,pam_txt[:-1])
    return func_txt



def new_func_builder(func,pam_text): # 通过函数与参数拼凑一个函数
    return func + '(' + pam_text + ')'

def func_nam_parser(func_txt): # 通过正则表达式匹配出函数名
     if '(' in func_txt:
         function_name = re.search('(.*?)\(', func_txt).groups()[0]
     else:
         function_name = func_txt
     return function_name 

def pam_list_txt_parser(func_txt):# 通过找到第一个括号，找出函数括号内的文字
    start = 0
    for i in range(0,len(func_txt)):
        if func_txt[i]== '(':
            start = i
            break
    pam_list_txt = func_txt[start+1:-1]
    return pam_list_txt
    
def pam_parser(pam_list_txt,func):# 对括号内的文字分离出n个参数
    score = 0
    pam = ''
    pam_list = []
    if func != 'cast':
        for ele in pam_list_txt:
            if ele == '(':
                score -=1
                pam += ele
            elif ele == ')':
                score +=1
                pam += ele
            elif ele == ',':
                if score <0:
                    pam += ele
                elif score ==0:
                    pam_list.append(pam)
                    pam = ''
            else:
                pam += ele
        pam_list.append(pam)
    elif func == 'cast':
            as_num = max(pam_list_txt.rfind('as'),pam_list_txt.rfind('AS'),pam_list_txt.rfind('As'),pam_list_txt.rfind('AA'))
            pam_list.append(pam_list_txt[:as_num-1])
            pam_list.append(pam_list_txt[as_num+2:])
    return pam_list

                   
def func_txt_parser(sql_txt):
    temp_sql_txt= ''
    func_signal = -1 #信号量初始值设为-1，用以控制循环中的模式
    func_txt_list = []
    for ele in sql_txt:
        if func_signal <0: #信号量小于0，说明之前的文字中没有需替换的函数
            temp_sql_txt += ele
            for func in func_nam_list.keys():
                if func in temp_sql_txt:
                    func_signal += 1
                    temp_sql_txt = ''
                    func_txt = func
        elif func_signal >= 0: #信号量大于等于0，说明出现了需要替换的函数，进入函数寻找模式
            if ele == '(':# 左括号+1
                func_signal +=1
                func_txt +=ele
            elif ele == ')':# 右括号-1 信号量重置为0点时候，说明函数寻找完成
                func_txt +=ele
                func_signal -=1
                if func_signal == 0:
                    if func_txt not in func_txt_list:# 如果函数之前没有出现过，则写入待替换列表
                        func_txt_list.append(func_txt)
                    func_txt = ''
                    func_signal -= 1 # 退出函数寻找模式
            else:
                func_txt +=ele
    return func_txt_list

def func_nam_pam_parser(func_txt): #对函数名、函数参数进行分离
    func_nam = func_nam_parser(func_txt)
    if func_nam in func_nam_list.keys():
        pam_text = pam_list_txt_parser(func_txt)
        pam_list = []
        pam_list = pam_parser(pam_text,func_nam)
        for i in range(0,len(pam_list)):
            pam_list[i] = func_nam_pam_parser(pam_list[i])
        func_parse_list = {func_nam:pam_list}
    else:
        func_parse_list = func_txt
    return func_parse_list 


def sql_parser(func_txt_list):
    sql_parse_list = []
    for i in range(0,len(func_txt_list)): # 对解析出来的函数进行解析，拆解成函数+参数的模式
            sql_parse_list.append(func_nam_pam_parser(func_txt_list[i]))
    return sql_parse_list 


def func_translator(sql_parse_list): # 利用递归从最底层翻译函数，逐层上升
    func = ''
    pam_list = []
    func_mapping_list = func_nam_list 
    for i in range(0,len(sql_parse_list)) :
        item = sql_parse_list[i]
        if type(item) is dict:
            func_txt = ''
            func = item.keys()[0]
            pam_list = item[func]
            func_translator(pam_list)
            if func in func_mapping_list:
                func_txt = func_mapping(func,pam_list)
                sql_parse_list[i] = func_txt
            else:
                pam_txt = ''
                for item in pam_list:
                    pam_txt += item + ','
                sql_parse_list[i] = new_func_builder(func,pam_txt[:-1])
    return sql_parse_list

def func_txt_translator(sql_parse_list):
    sql_translated_list = list(sql_parse_list)
    sql_translated_list = func_translator(sql_translated_list) # 建立对应关系
    return sql_translated_list

def sql_translator(sql_txt,adress=''): # 翻译SQL，打印原始的SQL，目标的SQL，还有过程中的替换项
    print('The Presto SQL:\n{}'.format(sql_txt))
    if adress !='':
        print adress+str(st)+'.txt'
        txt_file = open(adress+str(st)+'.txt','w')
        txt_file.write('The Presto SQL:\n{}'.format(sql_txt))
    sql_translated_txt = str(sql_txt)
    func_txt_list = func_txt_parser(sql_txt)
    sql_parse_list = sql_parser(func_txt_list)
    sql_translated_list = func_txt_translator(sql_parse_list)
    source = ''
    target = ''
    for i in range(0,len(func_txt_list)):
        source = func_txt_list[i]
        target = sql_translated_list[i]
        print('\n{}. replace {} with {}'.format(i+1,source,target))
        if adress !='':
            txt_file.write('\n{}. replace {} with {}'.format(i+1,source,target))
        sql_translated_txt = sql_translated_txt.replace(source,target)
    print('\n\n\n【The Hive SQL】: \n{}'.format(sql_translated_txt))
    if adress !='':
        txt_file.write('\n\n\n【The Hive SQL】: \n{}'.format(sql_translated_txt))
        txt_file.close()
    return sql_translated_txt

sql_translated_txt = sql_translator(sql,'/Users/zhangjue/Desktop/') # 执行翻译

#调试用语句
#sql_txt  = sql
#print '【sql】' + sql
#sql_translated_txt = str(sql_txt)
#print '【sql_translated_txt】' + sql_translated_txt 
#func_txt_list = func_txt_parser(sql_txt)
#print '【func_txt_list】'
#print func_txt_list 
#print func_nam_pam_parser(func_txt_list[0])
#sql_parse_list = sql_parser(func_txt_list)
#print '【sql_parse_list】'
#print sql_parse_list 
#sql_translated_list = func_txt_translator(sql_parse_list)
#print '【sql_translated_list】'
#print sql_translated_list 
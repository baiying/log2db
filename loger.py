#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
'''
loger
将nginx日志转存到MySQL数据库中
version 1.0.0
author cn.baiying@gmail.com
last modify 2016-04-14
'''
import os
import sys
import re
import datetime
import time
import requests
import pymysql

''' 入口方法，程序会自动查找上一时间段的日志文件，也可以通过timeFlay参数指定某一时间段的日志 '''
def log2db(timeFlag = ""):
	logDir = r"/alidata/log/nginx/access/api_log_hour/"
	oneHourBefore = datetime.datetime.now() + datetime.timedelta(hours = -1)
	if timeFlag == "":
		timeFlag = oneHourBefore.strftime("%Y%m%d%H")

	ymd = timeFlag[:8]
	logDir += ymd + "/"
	logFile = logDir + "api.66pei.com.log." + timeFlag
	if os.path.exists(logFile):
		getLog(logFile, timeFlag)
	else:
		runlog("log file " + logFile + " not exists.")

''' 记录程序运行时异常日志，日志记录存入python_run.log文件中 '''
def runlog(log = ""):
	runLogFile = "/alidata/python/python_run.log"
	now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	log = "[" + now + "] " + log
	try:
		with open(runLogFile, 'a') as rlf:
			print(log, file = rlf)
	except IOError as err:
		print(log, file = rlf)

''' 读取选定的日志文件内容，并将日志数据经过处理后存储到数据库中 '''
def getLog(logFile, timeFlag = ""):
	data = []
	try:
		with open(logFile) as logs:
			for log in logs:
				if log != "":
					logDic = convertLog(log)
					data.append(logDic)
		res = save2db(data, timeFlag)
		if res:
			runlog(logFile + " success.")

	except IOError as err:
		runlog(str(error))

''' 将日志数据转换为数据字典 '''
def convertLog(log = ""):
	ip = r"?P<ip>[\d.]*"
	date = r"?P<date>\d+/\w+/\d+"
	log_time = r"?P<time>\S+"
	method = r"?P<method>\S+"
	request = r"?P<request>\S+"
	status = r"?P<status>\d+"
	bodyBytesSent = r"?P<bodyBytesSent>\d+"
	refer = r"?P<refer>[^\"]*"
	userAgent=r"?P<userAgent>.*"
	p =  re.compile(r"(%s)\ [0-9\-]\ [0-9\-]\ \[(%s)\:(%s)\ [\S]+\]\ \"(%s)?[\s]?(%s)?.*?\"\ (%s)\ (%s)\ \"(%s)\"\ \"(%s).*?\"" % (ip, date, log_time, method, request, status, bodyBytesSent, refer, userAgent), re.VERBOSE)
	m = re.findall(p, log)[0]
	res = {}
	res['ip'] = m[0]
	accTime = convertTime(" ".join(m[1:3]))
	res['time_str'] = accTime['string']
	res['time_int'] = accTime['int']
	if len(m[3]) > 16:
		res['method'] = ""
	else:
		res['method'] = m[3]
	res['request'] = m[4]
	res['status'] = m[5]
	res['body'] = m[6]
	res['refer'] = m[7]
	res['agent'] = m[8]

	params = convertParams(m[4])
	res['url'] = params['url']
	if params['customer_id'] == "":
		res['customer_id'] = 0
	else:
		res['customer_id'] = params['customer_id']

	res['appid'] = params['_appid']
	res['appversion'] = params['_appversion']
	res['os'] = params['_os']
	res['func'] = requests.utils.unquote(params['_func'])
	res['sku'] = requests.utils.unquote(params['sku'])
	res['brand_id'] = requests.utils.unquote(params['brand_id'])
	return res

''' 格式化日志中的时间字段 '''
def convertTime(timeStr = ""):
	arr = timeStr.split()
	dt = datetime.datetime.strptime(" ".join(arr[:2]), '%d/%b/%Y %H:%M:%S')
	res = {}
	res['string'] = dt.strftime("%Y-%m-%d %H:%M:%S")
	res['int'] = int(time.mktime(time.strptime(res['string'], '%Y-%m-%d %H:%M:%S')))
	return res

''' 将日志中的请求参数转换为数据字典，供convertLog方法调用 '''
def convertParams(request = ""):
	columns = ['url', 'customer_id', '_appid', '_appversion', '_os', '_func', 'sku', 'brand_id']
	res = {}
	res['url'] = ""
	res['customer_id'] = 0
	res['_appid'] = ""
	res['_appversion'] = ""
	res['_os'] = ""
	res['_func'] = ""
	res['sku'] = ""
	res['brand_id'] = ""
	if('?' in request):
		arr = request.split('?')
		res['url'] = arr[0]
		for item in arr[1].split('&'):
			tmp = item.split('=')
			if tmp[0] in columns:
				res[tmp[0]] = tmp[1]
	return res

''' 将整理出来的日志数据字典保存到数据库中 '''
def save2db(data = [], timeFlag = ""):
	try:
		conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db='log', port=3306, charset='utf8')
		cur = conn.cursor()
		# 判断此批数据是否已经入过库了，如果已入过，则删除重入
		sql = "DELETE FROM 66_api WHERE time_flag = '%s'" % timeFlag
		cur.execute(sql)

		# 生成入库sql
		for item in data:
			sql = r'''INSERT INTO 66_api SET '''
			spl = ""
			for (key, val) in item.items():
				if isinstance(val, str) and "'" in val:
					val = val.replace("'", "‘")
				sql += spl + "%s = '%s'" % (key, val)
				spl = ", "
			sql += ", dt = '%s'" % time.strftime('%Y-%m-%d %H-%M-%S', time.localtime(time.time()))
			sql += ", time_flag = '%s'" % timeFlag
			cur.execute(sql)

		conn.commit()
		cur.close
		conn.close
		return True
	except pymysql.Error as err:
		runlog(str(err))
		return False

# 执行程序
if len(sys.argv) == 1:
	log2db()
else:
	log2db(sys.argv[1])


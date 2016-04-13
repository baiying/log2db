#! /usr/bin/env python3   
# -*- coding: utf-8 -*- 
import os
import re
import datetime
import time
import urllib
import pymysql

''' find log file '''
def log2db(timeFlag = ""):
	logDir = r"/Users/test/github/loger/"
	oneHourBefore = datetime.datetime.now() + datetime.timedelta(hours = -1)
	ymd = oneHourBefore.strftime("%Y%m%d")
	if timeFlag == "":
		timeFlag = oneHourBefore.strftime("%Y%m%d%H")
	logDir += ymd + "/"
	logFile = logDir + "log." + timeFlag
	if os.path.exists(logFile):
		getLog(logFile, timeFlag)
	else:
		runlog("log file " + logFile + " not exists.")

''' write run log '''
def runlog(log = ""):
	runLogFile = "python_run.log"
	now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	log = "[" + now + "] " + log
	try:
		with open(runLogFile, 'a') as rlf:
			print(log, file = rlf)
	except IOError as err:
		print(log, file = rlf)

''' read log file and save to db '''
def getLog(logFile, timeFlag = ""):
	data = []
	try:
		with open(logFile) as logs:
			for log in logs:
				if log != "":
					logDic = convertLog(log)
					data.append(logDic)
	except IOError as err:
		runlog(str(error))
	save2db(data, timeFlag)

''' convert log to dict '''
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
	p =  re.compile(r"(%s)\ -\ -\ \[(%s)\:(%s)\ [\S]+\]\ \"(%s)?[\s]?(%s)?.*?\"\ (%s)\ (%s)\ \"(%s)\"\ \"(%s).*?\"" % (ip, date, log_time, method, request, status, bodyBytesSent, refer, userAgent), re.VERBOSE)
	m = re.findall(p, log)[0]
	res = {}
	res['ip'] = m[0]
	accTime = convertTime(" ".join(m[1:3]))
	res['time_str'] = accTime['string']
	res['time_int'] = accTime['int']
	res['method'] = m[3]
	res['request'] = m[4]
	res['status'] = m[5]
	res['body'] = m[6]
	res['refer'] = m[7]
	res['agent'] = m[8]

	params = convertParams(m[4])
	res['url'] = params['url']
	res['customer_id'] = params['customer_id']
	res['appid'] = params['_appid']
	res['appversion'] = params['_appversion']
	res['os'] = params['_os']
	res['func'] = urllib.parse.unquote(params['_func'])
	res['sku'] = urllib.parse.unquote(params['sku'])
	res['brand_id'] = params['brand_id']
	return res

''' format time string '''
def convertTime(timeStr = ""):
	arr = timeStr.split()
	dt = datetime.datetime.strptime(" ".join(arr[:2]), '%d/%b/%Y %H:%M:%S')
	res = {}
	res['string'] = dt.strftime("%Y-%m-%d %H:%M:%S")
	res['int'] = int(time.mktime(time.strptime(res['string'], '%Y-%m-%d %H:%M:%S')))
	return res

''' convert request params to dict '''
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
	res['brand_id'] = 0
	if('?' in request):
		arr = request.split('?')
		res['url'] = arr[0]
		for item in arr[1].split('&'):
			tmp = item.split('=')
			if tmp[0] in columns:
				res[tmp[0]] = tmp[1]
	return res

''' save data to db '''
def save2db(data = [], timeFlag = ""):
	try:
		conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db='log', port=3306, charset='utf8')
		cur = conn.cursor()
		# 生成sql
		for item in data:
			sql = "INSERT INTO log SET "
			spl = ""
			for (key, val) in item.items():
				sql += spl + "%s = '%s'" % (key, val)
				spl = ", "
			sql += ", dt = '%s'" % time.strftime('%Y-%m-%d %H-%M-%S', time.localtime(time.time()))
			sql += ", time_flag = '%s'" % timeFlag
			cur.execute(sql)

		conn.commit()
		cur.close
		conn.close
	except pymysql.Error as err:
		runlog(str(err))

log2db()
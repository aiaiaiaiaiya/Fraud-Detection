#!/usr/bin/python

from pyspark import SparkContext, SparkConf
import sys
import math
import json

conf = SparkConf().setAppName("sd_cal").setMaster("local[1]")
sc = SparkContext(conf=conf)

inputcsv = sc.textFile("hdfs://localhost/user/training/csv/*.csv")
inputjson = sc.textFile("hdfs://localhost/user/training/json_log_data/*")
dataparse = inputjson.map(json.loads)

csv = inputcsv.map(lambda arr : arr.split(",")).map(lambda arr: (arr[3],float(arr[5])))
json = dataparse.map(lambda j:(j['userid'],j['amount']))

sumcsv = csv.reduceByKey(lambda a,b:a+b)
sumjson = json.reduceByKey(lambda a,b:a+b)

countcsv = csv.map(lambda arr: (arr[0],1)).reduceByKey(lambda a,b:a+b)
countjson = json.map(lambda arr: (arr[0],1)).reduceByKey(lambda a,b:a+b)

sumall = sumcsv.join(sumjson).map(lambda word: ( word[0], word[1][0] + word[1][1] ) )
count = countcsv.join(countjson).map(lambda word: ( word[0], word[1][0] + word[1][1] ) )

avg = sumall.join(count).map(lambda word: ( word[0], word[1][0]/word[1][1] ) )
avgpow = avg.map(lambda a: ( a[0], a[1]**2 ))

sigmaXpowcsv = csv.map(lambda word: ( word[0], word[1]**2 )).reduceByKey(lambda a,b:a+b)
sigmaXpowjson = json.map(lambda word: ( word[0], word[1]**2 )).reduceByKey(lambda a,b:a+b)
sigmaXpow = sigmaXpowcsv.join(sigmaXpowjson).map(lambda word: ( word[0], (word[1][0] + word[1][1]) ) )

sigmaXpowdiv = sigmaXpow.join(count).map(lambda word: ( word[0], word[1][0]/word[1][1] ) )

sd = sigmaXpowdiv.join(avgpow).map(lambda x: ( x[0], math.sqrt( x[1][0] - x[1][1] ) ) )

fraud = avg.join(sd).map(lambda x: (x[0] , x[1][0]+(x[1][1]*2) ) ).sortByKey(True)

fraud.collect()
fraud.saveAsTextFile("hdfs://localhost/user/training/fraud_result")

#!/usr/bin/python

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json
import pprint
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def alert(id,value):
	me = "sender@gmail.com"
	you = "receiver@gmail.com"

	msg = MIMEMultipart('alternative')
	msg['Subject'] = "Link"
	msg['From'] = me
	msg['To'] = you

	html = """\
	<html>
	  <head></head>
	  <body>
	    <p>Dear, User ID : """ + id +"""<br>
	       Please check your balance on your account.<br>
		   We detected abnormal withdrawal transaction. (""" + str(value)  + """ THB)
	    </p>
	  </body>
	</html>
	"""

	part = MIMEText(html, 'html')

	msg.attach(part)

	s = smtplib.SMTP('smtp.gmail.com:587')

	s.starttls()
	s.login('sender@gmail.com','senderpassword')
	s.sendmail('sender@gmail.com','receiver@gmail.com', msg.as_string())
	s.quit()


conf = SparkConf().setAppName("alert").setMaster("local[1]")
sc = SparkContext(conf=conf)
fraudFile = sc.textFile("hdfs://localhost/user/training/fraud_result/*")
fraudData = fraudFile.map(lambda x: re.sub("[\(u' )]","", x)).map(lambda x: x.split(",")).map(lambda x: (x[0],x[1])).collectAsMap()
sc.stop()

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 3222)


counts = lines.map(lambda x: json.loads(x)).map(lambda x: (x["userid"],x["amount"])).filter(lambda x: x[1] >= float(fraudData[x[0]])).map(lambda x: alert(x[0],x[1]))
counts.pprint()

ssc.start()
ssc.awaitTermination()

import socket
import time
import datetime
import sys
import json

# Configuration
inputFile = "test_data_full.json"
bindRemoteAddress = "localhost"
bindRemotePort = 3222

fo = open(inputFile, "r+")
str = fo.read();
line = str.split("\n")
print "Read file Complete!"
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((bindRemoteAddress,bindRemotePort))
s.listen(1)

print "Connection is open at :" + datetime.datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
print ""
c,address = s.accept()
while(1):
    for i in line:
        c.send(i)
        txn = json.loads(i)
        print "Send: {0}".format(txn['id'])
        time.sleep(0.01)
c.close()

print "Streaming trensfer finish :" + datetime.datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
print "Socket "+ str(bindRemotePort) +" has been close."
print ""

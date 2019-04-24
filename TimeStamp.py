#!/usr/lib/python2.6
import os
import datetime
import sys
import re
import json 
from pprint import pprint
#print "hello world";
#Tkt_No = input("Enter the ticket number: ")

#Acc_name = input("Enter the Account name: ")
#from pathlib import path

#os.chdir("/nfs/old_home/sthapa/work/REISSUES/LIN-5867_1/logs")
#print os.getcwd()


#filename = 'Analytic.Partners.DVRMarket15Minute.v1.0_Final'
#pattern = 'Program Ended.* for exports'

"""
#destdir = 'pathtofile'
files = [ f for f in os.listdir(destdir) if 
os.path.isfile(os.path.join(destdir,f)) ]
for f in files:
    with open(f, 'r') as var1:
        for line in var1: 
            if re.match('(.*)exception(.*)', line):
                print line
destdir = Path('/nfs/old_home/sthapa/work/REISSUES/LIN-5867_1/logs/Analytic.Partners.DVRMarket15Minute.v1.0_Final')
files = [p for p in destdir.iterdir() if p.is_file()]
for p in files:
    with p.open() as f:
       for line in var1: 
            if re.match('Program Ended', line):
                print line

source ="/nfs/old_home/sthapa/work/REISSUES/LIN-5867_1/logs"
for root, dirs, filenames in os.walk(source):
    for file in filenames:
        print file
"""
#shakes = open("Analytic.Partners.DVRMarket15Minute.v1.0_Final", "r")

#for line in shakes:
#    if re.match("(.*)Program Ended(.*)", line):
#        print line
"""
src_dict = ("/nfs/old_home/sthapa/work/REISSUES/LIN-5867_1/logs") 
pattern = re.compile ('(.*)for exports(.*)')
for passed_files in os.listdir(src_dict): 
    files = os.path.join(src_dict, passed_files) 
    strng = open(files) 
    for lines in strng.readlines(): 
        if re.search(pattern, lines): 
            print lines 
"""
tkt_name = raw_input("Enter The Ticket Number :")
user_acc = raw_input("Enter The User Acc ID :")
dir_path = "/nfs/old_home/{0}/work/REISSUES/{1}/logs".format(user_acc,tkt_name) 
#ctr_dir = ("/nfs/old_home/sthapa/work/REISSUES/{0}/1.control_file.txt")
pattern = re.compile ('(.*)for exports(.*)')
n = 0
sum_seconds = 0

for filename in os.listdir(dir_path):
    with open(os.path.join(dir_path, filename)) as f:
        for line in f:
            if re.search(pattern, line):
            #    print(line)

                # remove newline at end, split by spaces
                parts = line.strip().split()
                if len(parts) > 0:
                    n += 1
                    duration_str = parts[-1]
                 #   print(duration_str)
                    h, m, s = duration_str.split(':')
                    sum_seconds += (int(h) * 3600 + int(m) * 60 + int(s))
                    tot_time = sum_seconds / 4

#print('Total (in seconds):', str(sum_seconds))
print('Total Time Taken:',str(datetime.timedelta(seconds=tot_time)))
if n > 0:
    avg_seconds = round(sum_seconds / n)
    #print('Avg (in seconds):', avg_seconds)
    print('Avg Time:',str(datetime.timedelta(seconds=avg_seconds)))

try:
    with open('/nfs/old_home/{0}/work/REISSUES/{1}/1.control_file.txt'.format(user_acc,tkt_name)) as data_file:
        data=len(json.load(data_file)["market_nos"])
        s="Market No's Ran For: "
        pprint("{0}{1}".format(s,data))
except KeyError:
       print("No market numbers used for this Reissue ticket")
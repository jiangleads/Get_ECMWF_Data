# -*- coding: cp936 -*-
## python script to download selected files from rda.ucar.edu
##
import sys
import os
import urllib2
import cookielib
import datetime

##
if (len(sys.argv) != 2):
  print "usage: "+sys.argv[0]+" [-q] password_on_RDA_webserver"
  print "-q suppresses the progress message for each file that is downloaded"
  sys.exit(1)
##
passwd_idx=1
verbose=True
if (len(sys.argv) == 3 and sys.argv[1] == "-q"):
  passwd_idx=2
  verbose=False
##
cj=cookielib.MozillaCookieJar()
opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
##
## check for existing cookies file and authenticate if necessary
do_authentication=False
if (os.path.isfile("auth.rda.ucar.edu")):
  cj.load("auth.rda.ucar.edu",False,True)
  for cookie in cj:
    if (cookie.name == "sess" and cookie.is_expired()):
      do_authentication=True
else:
  do_authentication=True
if (do_authentication):
  login=opener.open("https://rda.ucar.edu/cgi-bin/login","email=????&password="+sys.argv[1]+"&action=login")
##
## save the authentication cookies for future downloads
## NOTE! - cookies are saved for future sessions because overly-frequent authentication to our server can cause your data access to be blocked
  cj.clear_session_cookies()
  cj.save("auth.rda.ucar.edu",True,True)
##


#起始日期
begin = datetime.date(2018,1,1)  
end = datetime.date(2018,12,31)
d=begin
delta = datetime.timedelta(days=1)
   
#建立下载日期序列
links = []
while d <= end:
    riqi=d.strftime("%Y%m%d")
    yue=d.strftime("%m")
    nian=d.strftime("%Y")
    filename="grib2/"+nian+"/"+nian+"."+yue+"/fnl_"+riqi+"_00_00.grib2"
    links.append(filename)
##    filename="grib2/"+nian+"/"+nian+"."+yue+"/fnl_"+riqi+"_06_00.grib2"
##    links.append(filename)
    filename="grib2/"+nian+"/"+nian+"."+yue+"/fnl_"+riqi+"_12_00.grib2"
    links.append(filename)
##    filename="grib2/"+nian+"/"+nian+"."+yue+"/fnl_"+riqi+"_18_00.grib2"
##    links.append(filename)
    d += delta

  
## download the data file(s)
listoffiles=links
  
for file in listoffiles:
  fileb=str(file)
  filec=fileb[-24:]
  idx=file.rfind("/")
  #如果存在文件名则返回
  if(os.path.isfile(filec)):
    print("ok",filec)
    continue
  if (idx > 0):
    ofile=file[idx+1:]
  else:
    ofile=file
  if (verbose):
    sys.stdout.write("downloading "+ofile+"...")
    sys.stdout.flush()
  infile=opener.open("http://rda.ucar.edu/data/ds083.2/"+file)
  outfile=open(ofile,"wb")
  outfile.write(infile.read())
  outfile.close()
  if (verbose):
    sys.stdout.write("done.\n")

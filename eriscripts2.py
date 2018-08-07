# This Python file uses the following encoding: utf-8
from multiprocessing import Pool
from threading import Thread
from ecmwfapi import ECMWFDataServer
from time import time
import datetime
import calendar
import os

#https://blog.csdn.net/seetheworld518/article/details/49639651   
def downloadonefile(riqi):
    ts = time()
    print(riqi)
    return
    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/u10/u10."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levtype": "sfc",
        "param": "165.128", #u10
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "type": "an",
        "target": filename,
      })

    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/v10/v10."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levtype": "sfc",
        "param": "166.128", #v10
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "type": "an",
        "target": filename,
      })

    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/2t/2t."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levelist": "1000/975/950/925/900/875/850/825/800/775/750/700/650/600/550/500/450/400/350/300/250/225/200/175/150/125/100/70",
        "levtype": "sfc",
        "param": "167.128", #2m气温
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "type": "an",
        "target": filename,
      })

    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/sst/sst."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levtype": "sfc",
        "param": "34.128", #sst
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "type": "an",
        "target": filename,
      })   

    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/sp/sp."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levelist": "1000/975/950/925/900/875/850/825/800/775/750/700/650/600/550/500/450/400/350/300/250/225/200/175/150/125/100/70",
        "levtype": "sfc",
        "param": "134.128", #Surface pressure
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "type": "an",
        "target": filename,
      })

    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/mslp/mslp."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levtype": "sfc",
        "param": "151.128", #mslp
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "type": "an",
        "target": filename,
      })

    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/tcw/tcw."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levtype": "sfc",
        "param": "136.128", #tcw
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "type": "an",
        "target": filename,
      })

    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/tcwv/tcwv."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levtype": "sfc",
        "param": "137.128", #tcwv
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "type": "an",
        "target": filename,
      })

    filename="/mnt/HD/HD_a2/Public/ERA-Interim/SURF/cape/cape."+riqi+".grb"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      server = ECMWFDataServer()
      server.retrieve({
        "class": "ei",
        "dataset": "interim",
        "date": riqi,
        "expver": "1",
        "grid": "0.75/0.75",
        "levtype": "sfc",
        "param": "59.128",  #cape
        "step": "3/6/9/12",
        "stream": "oper",
        "time": "00:00:00/12:00:00",
        "type": "fc",
        "target": filename,
      })
 

 
#主程序 
def main():
   #起始时间
   ts = time()
   nowyear=datetime.datetime.now().year
   nowmonth=datetime.datetime.now().month
   nowfirstdaymonth=datetime.date(nowyear,nowmonth,1) #本月1号
##   print(nowfirstdaymonth)
   pretime=nowfirstdaymonth-datetime.timedelta(days=70)#大约调到三个月前
   preyear=pretime.year
   premonth=pretime.month
##   print (pretime)
   premonthrange=calendar.monthrange(preyear,premonth)[1] #返回该月一共多少天

   
   #起始日期
   begin = datetime.date(preyear,premonth,1)  
   end = datetime.date(preyear,premonth,premonthrange)
   d=begin
   delta = datetime.timedelta(days=1)
   
   #建立下载日期序列
   links = []
   while d <= end:
       riqi=d.strftime("%Y%m%d")
       links.append(str(riqi))

       d += delta

   pool = Pool(5) # 创建拥有5个进程数量的进程池
   
   rl=pool.map(downloadonefile,links)
   pool.close()#关闭进程池，不再接受新的进程
   pool.join()#主进程阻塞等待子进程的退出
   
   print('Took {}'.format(time() - ts))

if __name__ == '__main__':
   main()

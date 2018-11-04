from queue import Queue
from threading import Thread
import cdsapi
from time import time
import datetime
import os

def downloadonefile(riqi):
    ts = time()
    filename="G:/ERA5/SURF2/u10/u10."+riqi+".grb2"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      c = cdsapi.Client()
      c.retrieve('reanalysis-era5-complete', {
        "class": "ea",
        "dataset": "era5",
        "date": riqi,
        "expver": "1",
        "grid": "0.25/0.25",
        "levtype": "sfc",
        "param": "165.128", #u10
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00\
/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00\
/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00\
/20:00:00/21:00:00/22:00:00/23:00:00",
        "type": "an",
        
      },filename)

    filename="G:/ERA5/SURF2/v10/v10."+riqi+".grb2"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      c = cdsapi.Client()
      c.retrieve('reanalysis-era5-complete', {
        "class": "ea",
        "dataset": "era5",
        "date": riqi,
        "expver": "1",
        "grid": "0.25/0.25",
        "levtype": "sfc",
        "param": "166.128", #v10
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00\
/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00\
/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00\
/20:00:00/21:00:00/22:00:00/23:00:00",
        "type": "an",
        
      },filename)

    filename="G:/ERA5/SURF2/2t/2t."+riqi+".grb2"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      c = cdsapi.Client()
      c.retrieve('reanalysis-era5-complete', {
        "class": "ea",
        "dataset": "era5",
        "date": riqi,
        "expver": "1",
        "grid": "0.25/0.25",
        "levelist": "1000/975/950/925/900/875/850/825/800/775/750/700/650/600/550/500/450/400/350/300/250/225/200/175/150/125/100/70",
        "levtype": "sfc",
        "param": "167.128", #2m气温
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00\
/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00\
/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00\
/20:00:00/21:00:00/22:00:00/23:00:00",
        "type": "an",
        
      },filename)

    filename="G:/ERA5/SURF2/sst/sst."+riqi+".grb2"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      c = cdsapi.Client()
      c.retrieve('reanalysis-era5-complete', {
        "class": "ea",
        "dataset": "era5",
        "date": riqi,
        "expver": "1",
        "grid": "0.25/0.25",
        "levtype": "sfc",
        "param": "34.128", #sst
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00\
/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00\
/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00\
/20:00:00/21:00:00/22:00:00/23:00:00",
        "type": "an",
        
      },filename)   

    filename="G:/ERA5/SURF2/sp/sp."+riqi+".grb2"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      c = cdsapi.Client()
      c.retrieve('reanalysis-era5-complete', {
        "class": "ea",
        "dataset": "era5",
        "date": riqi,
        "expver": "1",
        "grid": "0.25/0.25",
        "levelist": "1000/975/950/925/900/875/850/825/800/775/750/700/650/600/550/500/450/400/350/300/250/225/200/175/150/125/100/70",
        "levtype": "sfc",
        "param": "134.128", #Surface pressure
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00\
/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00\
/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00\
/20:00:00/21:00:00/22:00:00/23:00:00",
        "type": "an",
        
      },filename)

    filename="G:/ERA5/SURF2/mslp/mslp."+riqi+".grb2"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      c = cdsapi.Client()
      c.retrieve('reanalysis-era5-complete', {
        "class": "ea",
        "dataset": "era5",
        "date": riqi,
        "expver": "1",
        "grid": "0.25/0.25",
        "levtype": "sfc",
        "param": "151.128", #mslp
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00\
/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00\
/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00\
/20:00:00/21:00:00/22:00:00/23:00:00",
        "type": "an",
        
      },filename)

    filename="G:/ERA5/SURF2/tcw/tcw."+riqi+".grb2"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      c = cdsapi.Client()
      c.retrieve('reanalysis-era5-complete', {
        "class": "ea",
        "dataset": "era5",
        "date": riqi,
        "expver": "1",
        "grid": "0.25/0.25",
        "levtype": "sfc",
        "param": "136.128", #tcw
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00\
/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00\
/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00\
/20:00:00/21:00:00/22:00:00/23:00:00",
        "type": "an",
        
      },filename)

    filename="G:/ERA5/SURF2/tcwv/tcwv."+riqi+".grb2"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      c = cdsapi.Client()
      c.retrieve('reanalysis-era5-complete', {
        "class": "ea",
        "dataset": "era5",
        "date": riqi,
        "expver": "1",
        "grid": "0.25/0.25",
        "levtype": "sfc",
        "param": "137.128", #tcwv
        "step": "0",
        "stream": "oper",
        "time": "00:00:00/01:00:00/02:00:00/03:00:00/04:00:00/05:00:00\
/06:00:00/07:00:00/08:00:00/09:00:00/10:00:00/11:00:00/12:00:00\
/13:00:00/14:00:00/15:00:00/16:00:00/17:00:00/18:00:00/19:00:00\
/20:00:00/21:00:00/22:00:00/23:00:00",
        "type": "an",
        
      },filename)


    
#下载脚本 
class DownloadWorker(Thread):
   def __init__(self, queue):
       Thread.__init__(self)
       self.queue = queue
 
   def run(self):
       while True:
           # 从队列中获取任务并扩展tuple
           riqi = self.queue.get()
           downloadonefile(riqi)
           self.queue.task_done()

#主程序 
def main():
   #起始时间
   ts = time()

   #起始日期
   begin = datetime.date(2018,7,21)  
   end = datetime.date(2018,7,25)
   d=begin
   delta = datetime.timedelta(days=1)
   
   #建立下载日期序列
   links = []
   while d <= end:
       riqi=d.strftime("%Y%m%d")
       links.append(str(riqi))
       d += delta

   #创建一个主进程与工作进程通信
   queue = Queue()
   #创建四个工作线程
   for x in range(1):
       worker = DownloadWorker(queue)
       #将daemon设置为True将会使主线程退出，即使所有worker都阻塞了
       worker.daemon = True
       worker.start()
       
   #将任务以tuple的形式放入队列中
   for link in links:
       queue.put((link))

   #让主线程等待队列完成所有的任务
   queue.join()
   print('Took {}'.format(time() - ts))

if __name__ == '__main__':
   main()

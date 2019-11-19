from queue import Queue
from threading import Thread
import cdsapi
from time import time
import datetime
import os

def downloadonefile(riqi):
    ts = time()
    filename="M:/ERA5/SURF/mslp/era5.mslp."+riqi+".grib"
    if(os.path.isfile(filename)): #如果存在文件名则返回
      print("ok",filename)
    else:
      print(filename)
      c = cdsapi.Client()
      c.retrieve(
          'reanalysis-era5-single-levels',
          {
              'product_type' : 'reanalysis',
              'format'       : 'grib', # Supported format: grib and netcdf. Default: grib
              'variable'     : 'mean_sea_level_pressure', #其它变量名参见 https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels
              'year'         : riqi[0:4],
              'month'        : riqi[-4:-2],
              'day'          : riqi[-2:],
              'time':[
                  '00:00','01:00','02:00',
                  '03:00','04:00','05:00',
                  '06:00','07:00','08:00',
                  '09:00','10:00','11:00',
                  '12:00','13:00','14:00',
                  '15:00','16:00','17:00',
                  '18:00','19:00','20:00',
                  '21:00','22:00','23:00'
                  ]
##              'area'         : [60, -10, 50, 2], # North, West, South, East. Default: global
##              'grid'         : [1.0, 1.0], # Latitude/longitude grid: east-west (longitude) and north-south resolution (latitude). Default: 0.25 x 0.25
          },
          filename)


    
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
   begin = datetime.date(1979,1,1)  
   end = datetime.date(2018,12,31)
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

   # 20191119更新# 新的请求规则 https://cds.climate.copernicus.eu/live/limits
   # 注意，每个用户同时最多接受4个request https://cds.climate.copernicus.eu/vision
   #创建四个工作线程
   for x in range(4):
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

#coding=utf-8

from __future__ import print_function
import traceback
import sys
from eccodes import *

#判断字符串Str是否包含序列SubStrList中的每一个子字符串
def IsSubString(SubStrList,Str):
    flag=True
    for substr in SubStrList:
        if not(substr in Str):
            flag=False
    return flag 

#获取当前目录所有指定类型的文件
def GetFileList(FindPath,FlagStr=[]):
    import os
    FileList=[]
    #print FileList
    FileNames=os.listdir(FindPath)
    #print FileNames
    for fn in FileNames:
        if (len(FlagStr)>0):
            #返回指定类型的文件名
            if (IsSubString(FlagStr,fn)):
                fullfilename=os.path.join(FindPath,fn)
                FileList.append(fullfilename)
        else:
            #默认直接返回所有文件名
            fullfilename=os.path.join(FindPath,fn)
            FileList.append(fullfilename)
    #对文件名排序
    if (len(FileList)>0):
        FileList.sort()
        #print FileList
    return FileList


 
def example(filename):
    f = open(filename, 'rb')
 
    keys = [
##        'Ni',
##        'Nj',
##        'latitudeOfFirstGridPointInDegrees',
##        'longitudeOfFirstGridPointInDegrees',
##        'latitudeOfLastGridPointInDegrees',
##        'longitudeOfLastGridPointInDegrees',
        'dataDate',
##        'dataTime'
    ]

    dataDate=0
    while 1:
        gid = codes_grib_new_from_file(f)
        if gid is None:
            break

        for key in keys:
            try:
##                print('  %s: %s' % (key, codes_get(gid, key)))
                dataDate2=codes_get(gid, key)
                if dataDate != dataDate2:
                    print(key,dataDate,dataDate2)
                    dataDate=dataDate2
##                    input()
                continue
            except KeyValueNotFoundError as err:
                # Full list of exceptions here:
                #   https://confluence.ecmwf.int/display/ECC/Python+exception+classes
                print('  Key="%s" was not found: %s' % (key, err.msg))
            except CodesInternalError as err:
                print('Error with key="%s" : %s' % (key, err.msg))
 
##        print('There are %d values, average is %f, min is %f, max is %f' % (
##            codes_get_size(gid, 'values'),
##            codes_get(gid, 'average'),
##            codes_get(gid, 'min'),
##            codes_get(gid, 'max')
##        ))
 
        codes_release(gid)
    
    f.close()

    #改名
    newname=os.path.dirname(filename)+"/era5.CHV.levels."+str(dataDate)+".grib"
    os.rename(filename,newname)
    print(filename,'======>',newname)
 

import os
from eccodes import *


Path='/mnt/d/Downloads/' #
SubStrList=['.grib']

FileList=GetFileList(Path,SubStrList) #得到指定类型(grib)文件名列表
##
##
for eachfile in FileList: #对每个文件操作
    print(eachfile)
    example(eachfile)


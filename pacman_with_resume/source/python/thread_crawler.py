# -*- coding: utf-8 -*-
import threading
import time
import urllib2
from lxml import etree
import MySQLdb

class crawler_thread(threading.Thread): #The crawler_thread class is derived from the class threading.Thread
   def __init__(self, num, sub_number, top_number):
      threading.Thread.__init__(self)
      self.thread_num = num
      self.sub_number = sub_number
      self.top_number = top_number
      self.thread_stop = False

   def run(self): #Overwrite run() method, put what you want the thread do here
      while not self.thread_stop:
         for number in range(self.sub_number, self.top_number):
            try:
               link = 'http://vblog.people.com.cn/index/playvideo/contentid/' + str(number)
               req = urllib2.Request(link)
               response = urllib2.urlopen(req)
               myPage = response.read()
               tree = etree.HTML(myPage)

               title = tree.xpath("//div[@class='base']/div[@class='base_info']/h1[@class='title']")[0].text.strip()
               ding = tree.xpath('//div[@class="share tr"]/span/a[@class="zf"]/i[@class="nding"]')[0].text.strip()
               last_time = tree.xpath('//div[@class="share tr"]/span/a[2]/text()')[0].strip()
               upload_date = tree.xpath('//div[@class="share tr"]/span/a[3]/text()')[0].strip()
               category = tree.xpath('//span[@class="v_channel"]/a')[0].text.strip()
               bofang = filter(lambda x: x.isdigit(), tree.xpath('//span[@class="v_tags"]/span')[0].text.strip())
               brief = tree.xpath('//p[contains(@class,"v_desc")]/text()')[0].strip()

               sql = "insert into video (title,ding,last_time,upload_date, link, category, bofang, brief)" \
                     "values('%s','%s', '%s','%s', '%s','%s', '%s', '%s');" \
                     % (title, ding, last_time,upload_date, link, category, bofang, brief)
               # sql_query(sql)  #insert result into database,
               print sql       #print crawl result if you just wanna test
            except StandardError:
               continue
         print '---------Thread:%d End time:%s----------\n' % (self.thread_num, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
         self.thread_stop = True

   def stop(self):
      self.thread_stop = True

def sql_query(sql_statement):
   conn = MySQLdb.connect(host='localhost', user='root', passwd='admin', db='renmin_vedio', port=3306,charset='utf8')
   cur = conn.cursor()
   sql = sql_statement
   try:
      cur.execute(sql)
      conn.commit()
   except StandardError, e:
      conn.rollback()

def start_crawl():
   # below thread 1 and 2 for test
   thread1 = crawler_thread(1, 10000,10005)
   thread2 = crawler_thread(2, 20000,20005)
   #  block below for real crawling
   # thread1 = crawler_thread(1, 10000, 19999)
   # thread2 = crawler_thread(2, 20000, 29999)
   # thread3 = crawler_thread(3, 30000,39999)
   # thread4 = crawler_thread(4, 40000,49999)
   # thread5 = crawler_thread(5, 50000,59999)
   # thread6 = crawler_thread(6, 60000,69999)
   # thread7 = crawler_thread(7, 70000,79999)
   # thread8 = crawler_thread(8, 80000,89999)
   # thread9 = crawler_thread(9, 90000,99999)
   # thread0 = crawler_thread(0, 0,9999)

   thread1.start()
   thread2.start()
   # thread3.start()
   # thread4.start()
   # thread5.start()
   # thread6.start()
   # thread7.start()
   # thread8.start()
   # thread9.start()
   # thread0.start()

   return

if __name__ == '__main__':
   print 'Start time:%s\n' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
   start_crawl()

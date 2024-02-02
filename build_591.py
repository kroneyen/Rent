from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import random
import requests
import time
import re
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import datetime
from fake_useragent import UserAgent


start_time = datetime.datetime.now()


url = "https://newhouse.591.com.tw/housing-list.html?rid=8&sids=103,104,105&build_type=4&room=3"
###591 
short_url = "https://newhouse.591.com.tw"
###　section　西屯區:104 北屯:102 南屯:105
### label 車位:7 陽台:9


options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--no-sandbox")
options.add_argument("--disable-gpu")
web = webdriver.Chrome(options=options)

user_agent = UserAgent()


### try to instantiate a client instance for local
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )




def insert_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection
    collection.insert_many(_values,ordered=False)


def update_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection
    collection.update_many(_values,{"$set" :_values}, upsert=True)


def delete_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection
    collection.delete_many(_values)




def New_House_detail(detail_url):

  """
  headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\
  0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.5304.107 Safari/537.36'}
  """

  #res = requests.get(detail_url, headers=headers )
  res = requests.get(detail_url,  headers={ 'user-agent': user_agent.random } )
  detail_layout =None
  detail_level =None
  detail_exp =None
  detail_h3_list = []
  detail_info_list= []
  detail_platform =None
  detail_price =None
  detail_construction =None

  soup_detail = BeautifulSoup(res.text  , "html.parser")

  ###
  for detail_items in soup_detail.find_all("section",{"class":"newhouse-sub-detail"} ,limit =1) :

     for sub_detail_items in   detail_items.find_all("div",{'class':"sub-detail-contianer"},limit=1):
        for sub_item in sub_detail_items.find_all("div",{"class":re.compile("^sub-detail-item")}) :
           for h3_item in sub_item.find_all("h3") :

             detail_h3_list.append(h3_item.get_text())
             detail_info_temp=[]
             for li_item in sub_item.find_all("li") :

                try :

                  span_item = li_item.find("span").get_text()
                  p_item = li_item.find("p").get_text()

                  #print(span_item , ':' , p_item)
                  #print('')
                  #if span_item =="接待會館" and li_item.find("p",{"class": "address"}): 
                  if span_item =="接待會館" :
                    detail_platform = p_item.replace("導航 >","")

                  if span_item =="建案總價" : 
                    detail_price = p_item 

                  if span_item =="投資建設" : 
                    detail_construction = p_item   
                     
                except :

                   span_item = "暫無"
                   pass

  return detail_price ,detail_construction ,detail_platform




def getList(dict):
    list = []
    for key in dict.keys():
        list.append(key)

    return list





def New_House_Search_List(short_url,url) :
    ### id
    houseid = []
    ### detail_url
    detail_url_list = []
    ### title
    houseList_item_title=[]
    ### 型態
    houseList_item_attrs_shape=[]

    ### 格局
    info_floor_layout=[]
    ### 樓層
    info_floor_addr_level=[]

    ### 權狀
    houseList_item_attrs_area=[]
    ### 主建
    houseList_item_attrs_mainarea=[]

    ### 年限
    houseList_item_attrs_houseage=[]
    ### 社區
    houseList_item_community=[]

    ### 社區link
    houseList_item_community_link=[]
    ### 行政區
    houseList_item_section=[]
    ### 住址
    houseList_item_address=[]
    ### 售價
    price=[]
    ### 單價
    unitprice=[]
    ### tag
    tags=[]

    ### 有效期限
    info_floor_exp= []

    ### 建案名稱
    houseName = []

    ### 建案地址
    addr = []

    ### 建案格局
    layout=[]

    ### 建商
    construction = []

    ### 接待中心

    platform =[]

    web.get(url)
    time.sleep(random.randrange(1, 3, 1))

    soup = BeautifulSoup(web.page_source  , "html.parser")

    for vue_list in  soup.find_all("div",{"id": "vue-list"} ,limit =1 ) :

      for house_items in vue_list.find_all("a",{"class" :"housing-item"}) :
        for p_itmes in house_items.find_all("p"):

          ### list item for <p>
          items_name = p_itmes['class'][0]

          ### 建案名稱
          if items_name =="houseName" :
            houseList_item_community.append(p_itmes.get_text())
            for link in p_itmes.find_all('a' ,limit =1) :

             link_url = short_url + link.get('href') + '/detail'
             houseList_item_community_link.append(link_url)
             ### house_detail
             detail_price ,detail_construction ,detail_platform = New_House_detail(link_url)
                
             price.append(detail_price)
             construction.append(detail_construction)
             platform.append(detail_platform)


             houseid.append(link.get('href').replace("/","") )

          ### 建案住址
          elif items_name =="address" :
            houseList_item_address.append(p_itmes.get_text().split('區',1)[1])
            houseList_item_section.append(p_itmes.get_text().split('台中市',1)[1].split('區')[0] + '區' )

          ### 建案規格
          elif items_name =="otherinfo" :
            info_floor_layout.append(p_itmes.get_text().replace("\n                    ","").replace("\n                ",""))

          
        ### tags
        tag = None
        tags.append(tag)


    row_data = { 'houseid' :houseid,
            'houseList_item_community' : houseList_item_community,
            'houseList_item_community_link' : houseList_item_community_link,
            'info_floor_layout': info_floor_layout,
            'houseList_item_section' : houseList_item_section,
            'houseList_item_address' : houseList_item_address,
            'construction' : construction,
            'platform' : platform ,
            'price' : price,
            'tags': tags                 


            }
           

    """    
    keylist = getList(row_data)

    for key in keylist :
      print(key , ':',len(row_data.get(key)) ,';', row_data.get(key))
    """

    df = pd.DataFrame(row_data)

    return df






web.get(url)
time.sleep(random.randrange(3, 5, 1))
"""
soup_next_pages = BeautifulSoup(web.page_source  , "html.parser")

page_num =[]

for nav_item in soup_next_pages.find_all("nav",{"class":"pr-lg-3 py-3"} ,limit =1) :
      for page_items in nav_item.find_all("li",{"class":"page-item"} ) :
         
          for page_link in page_items.find_all("a" ,limit =1) : 
      
              page_num.append(page_link.get_text())
      
      for last_rows_item in nav_item.find_all("div",{"class":re.compile("^text-center")} ,limit =1 ) :
          
             total_rows = last_rows_item.get_text().split(" ",1)[1]
      
"""
today = datetime.date.today().strftime('%Y%m%d')



first_Page = 1
#last_Page= int(page_num[-2])
last_Page = 1

delete_many_mongo_db('newbuild','build_591',{})

while first_Page <=  last_Page :

      #req_url = url + '&page= '+ str(first_Page)
      req_url = url
      
      match_row = New_House_Search_List(short_url,req_url)
      
      records = match_row.copy()
      
      if not records.empty :
        
        records["last_modify"]= datetime.datetime.now()
        #records["last_modify"]= today
        records = records.to_dict(orient='records')
      
      
        try :
          
             ### db.build_591.createIndex({houseid:1},{ name : "houseid_1" ,unique : true, background: true}) 

             insert_many_mongo_db('newbuild','build_591',records)
      
        except BulkWriteError as e:
      
               pass ## when duplicate date riseup error  by pass  
      
      
      time.sleep(random.randrange(1, 3, 1))
      print('pages:',first_Page, 'total_Page:',last_Page, 'done')
      
      
      first_Page += 1
      
      
      time.sleep(random.randrange(1, 5, 1))
      



web.quit()
"""

detail_url='https://newhouse.591.com.tw/133758/detail'

detail_price ,detail_construction ,detail_platform = New_House_detail(detail_url)

print(detail_price ,detail_construction ,detail_platform)
"""

end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))

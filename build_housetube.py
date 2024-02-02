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


short_url = 'https://taichung.housetube.tw'
### 台中房地王
url ="https://taichung.housetube.tw/searchhouse?do=search&cid=162,165,163&unit=1&price_min=1&price_max=2000&schedule=1&houseKind=2&order=price&desc=asc&select1=2&room=3"
#"https://taichung.housetube.tw/searchhouse?do=search&cid=162,165,163&unit=1&price_min=1&price_max=2000&schedule=1&houseKind=1,2&order=price&desc=asc&select1=2&room=3"
#"https://taichung.housetube.tw/searchhouse?do=search&cid=162,165,163&unit=1&price_min=1&price_max=2000&houseKind=2&order=price&desc=asc&select1=2&room=3"
###　section　西屯區:104 北屯:102 南屯:105
### label 車位:7 陽台:9
###schedule 預售
###housekind 公寓大樓/電梯大樓


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
      
      soup_detail = BeautifulSoup(res.text  , "html.parser")
      
      
      platform_list =[]
      construction_list =[]
      price_list=[]
      community_list=[]
      #print("detail_url:",detail_url)
      #print("soup_detail:", soup_detail)
      
      for detail_items in soup_detail.find_all("div",{"class":"col-xl-5"} ,limit =1) :
         
             
              #print("detail_items:", detail_items)
              
              for sub_detail_items in   detail_items.find_all("div",{"class":"arch-info"},limit=1):
                for dev_item in sub_detail_items.find_all("div",{"class":""} ,limit =1 ) :
                     for h5_item in dev_item.find_all("h5",{"class":re.compile("mb-3")},limit=1) :
                          for h5_strong in h5_item.find_all("strong",{"class" :re.compile("^text-danger")} , limit =1 ) :
                          
                             #print(h5_item.get_text())
                             price_list.append(h5_strong.get_text())
                          
                     for p_item in dev_item.find_all("p") :
                       
                          if p_item.span.get_text() == "建設公司" :
                     
                             construction_list.append(p_item.get_text().replace("建設公司","").replace("\n",""))
                     
                     for p_item in dev_item.find_all("p",{"class" : "d-flex mb-0"} , limit =1)  :
                         for span_item in p_item.find_all("span",{"class" : "d-flex"} ,limit =1) :
                          
                             #platform.append(span_item.get_text().replace("\n",""))
                             platform_list.append(span_item.get_text().replace("\n",""))
                             #print(p_item.get_text())
                ### check community               
                for dev_item in sub_detail_items.find_all("div",{"class":"border-bottom"} ,limit =1 ) :
                    
                        community_list.append(dev_item.h1.string)  

                        for dev_item_category in dev_item.find_all("div",{"class":"category"},limit =1):
                            platform_category = dev_item_category.get_text() 

                            


      
      if len(price_list) > 0 :
         price = price_list[0]
      else :
         price = None
      
      
      if len(platform_list) > 0 :
         platform = platform_list[0]
      else :
         #platform = None
         platform = platform_category
      
      if len(construction_list) > 0 :
         construction = construction_list[0]
      else :
         construction = None     
      
      if len(community_list) > 0 :
         community = community_list[0]
      else :
         community = None
  

      return price ,construction ,platform ,community





def getList(dict):
    list = []
    for key in dict.keys():
        list.append(key)

    return list


def community_houseage(houseList_item_community_link):

    """
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\
    0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.5304.107 Safari/537.36'}
    """

    res = requests.get(houseList_item_community_link , headers={ 'user-agent': user_agent.random })

    age =None
    soup_detail = BeautifulSoup(res.text  , "html.parser")
    for items in soup_detail.find_all("div",{"class": "overview-container"} , limit =1) :
        for detail_age in items.find_all("ul",{"class": "detail-info"} , limit =1) :
            age = detail_age.find("p").get_text()

    return  age




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

    for house_list in  soup.find_all("div",{"id": "c_area"}  ) :

      for link in house_list.find_all("a" ,limit =1) :

         link_url = short_url + link.get('href')
         houseList_item_community_link.append(link_url)
         houseid.append(link.get('href').replace("/",""))
         
         detail_price ,detail_construction ,detail_platform ,detail_community = New_House_detail(link_url)

         try :
               ### 建案名稱
               houseList_item_community.append(link.img.get('alt'))
         except :
               houseList_item_community.append(detail_community)


         ### 取的售價/>建設公司/接待中心
         price.append(detail_price)
         construction.append(detail_construction)
         platform.append(detail_platform)


      for card_body in house_list.find_all("div",{"class" : "p-0 card-body"} , limit =1):
         tag_temp = []
         p_item_temp = []
         
         ###行政區
         for p_item in card_body.find_all("p",{"class" : re.compile("mb-1")}) :

               p_item_temp.append(p_item.get_text().replace("\n\n","").replace("\n                        ","/").replace("                    \n",""))
         houseList_item_section.append(p_item_temp[1])

         ### 建案格局
         for p_item in card_body.find_all("p",{"class" : re.compile("^mb-0 mb-lg-1")} ,limit =1) :
               info_floor_layout.append(p_item.get_text().replace("\n                                                            ","").replace("                    ","/").replace("                ","").replace("\n住", "住/").replace("\n//地坪","地坪"))


         ### 建案地址
         for p_item in card_body.find_all("p",{"class" : re.compile("^mb-0 card-text")} ,limit =1) :
               try : 
                   addr = p_item.get_text().replace("\n","").split("區",1)[1]

               except :   

                   addr=  p_item.get_text().replace("\n","") 

               houseList_item_address.append(addr)

         ### tags

         try :

             for p_item in card_body.find_all("p",{"class" : re.compile("^mt-3 card-text")} ,limit =1) :
                  for tag_item in  p_item.find_all("span") :
                     tag_temp.append(tag_item.get_text())

         except :
              tag_item = None


         tags.append(tag_temp)


    """
    row_data = { 'houseid' :houseid,
            'houseList_item_title': houseList_item_title,
            'detail_url':detail_url_list,
            'houseList_item_attrs_shape': houseList_item_attrs_shape,
            'info_floor_layout': info_floor_layout,
            'info_floor_addr_level': info_floor_addr_level,
            'info_floor_exp' : info_floor_exp ,
            'houseList_item_attrs_area': houseList_item_attrs_area,
            'houseList_item_attrs_mainarea' : houseList_item_attrs_mainarea,
            'houseList_item_attrs_houseage': houseList_item_attrs_houseage,
            'houseList_item_community' : houseList_item_community,
            'houseList_item_community_link' : houseList_item_community_link,
            'houseList_item_section' : houseList_item_section,
            'houseList_item_address' : houseList_item_address,
            'price' : price,
            'unitprice' : unitprice,
            'tags' : tags }
    """

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

soup_next_pages = BeautifulSoup(web.page_source  , "html.parser")

page_num =[]

for nav_item in soup_next_pages.find_all("nav",{"class":"pr-lg-3 py-3"} ,limit =1) :
      for page_items in nav_item.find_all("li",{"class":"page-item"} ) :
         
          for page_link in page_items.find_all("a" ,limit =1) : 
      
              page_num.append(page_link.get_text())
      
      for last_rows_item in nav_item.find_all("div",{"class":re.compile("^text-center")} ,limit =1 ) :
          
             total_rows = last_rows_item.get_text().split(" ",1)[1]
      

today = datetime.date.today().strftime('%Y%m%d')


first_Page = 1
last_Page= int(page_num[-2])
#print('last_Page:',last_Page)
#last_Page = 5

### truncate  data
delete_many_mongo_db('newbuild','housetube',{})

while first_Page <=  last_Page :

      req_url = url + '&page= '+ str(first_Page)
      
      #print('req_url:',req_url)
      match_row = New_House_Search_List(short_url,req_url)
      
      records = match_row.copy()
      
      if not records.empty :
        
        records["last_modify"]= datetime.datetime.now()
        #records["last_modify"]= today
        records = records.to_dict(orient='records')
      
      
        try :
          
             ##db.housetube.createIndex({houseid:1},{ name : "houseid_1" ,unique : true, background: true})
             insert_many_mongo_db('newbuild','housetube',records)
      
        except BulkWriteError as e:
      
               pass ## when duplicate date riseup error  by pass  
      
      
      time.sleep(random.randrange(1, 3, 1))
      print('pages:',first_Page, 'total_Page:',last_Page)
      
      
      first_Page += 1
      
      
      time.sleep(random.randrange(1, 5, 1))
      



web.quit()


end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))

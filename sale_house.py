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


short_url = 'https://sale.591.com.tw'
sale_url ='https://sale.591.com.tw/?shType=list&regionid=8&section=104,103,105&price=500$_1500$&pattern=3,4&label=7,9'
###　section　西屯區:104 北屯:102 南屯:105
### label 車位:7 陽台:9


options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--no-sandbox")
web = webdriver.Chrome(options=options)


"""
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)


def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list
"""


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





def house_detail(detail_url):

       user_agent = UserAgent()                                                                                
                                                                                                          
       """                                                                                                     
       headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\                                  
       0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.5304.107 Safari/537.36'}                       
                                                                                                          
       res = requests.get(detail_url, headers=headers )                                                        
       #res = requests.get(url,  headers={ 'user-agent': user_agent.random } )                                 
       """                                                                                                     
       res = requests.get(detail_url , headers={ 'user-agent': user_agent.random })                              
       res.encoding = 'utf8'                                                                                     
          
       time.sleep(random.randrange(1, 2, 1))
                                                                                                
       #soup_detail = BeautifulSoup(web.page_source  , "html.parser")                                          
       soup_detail = BeautifulSoup(res.text  , "html.parser")                                                    

       detail_layout = None
       detail_level  = None
       detail_exp  = None
       
       ###格局    
       
       for detail_items in soup_detail.find_all("div",{"class":"info-box-floor"}) :
       
          #items_name = detail_items['class'][0].replace("-","_")
          #print(items_name ,':')                                                                              
          for detail_floor in   detail_items.find_all("div",{"class":"info-floor-key"},limit=1):
          #for detail_floor in   detail_items.find_all("div",{'class':re.compile("^info-floor-key")},limit=1):
          #for detail_floor in   detail_items.find("div",{'class':re.compile("^info-floor-key")}):             
            # items_name = detail_floor['class'][0].replace("-","_")
             #print(items_name ,':',detail_floor.get_text())                                                   
             detail_layout = detail_floor.get_text()
       
       
       ### 樓層                                                                                                  
       
       
       for detail_items in soup_detail.find_all("div",{"class":"info-box-addr"}) :
           #items_name = detail_items['class'][0].replace("-","_")
           #print(items_name ,':')                                                                              
           for detail_addr in   detail_items.find_all("div",{"class":"info-addr-content"},limit=1):
           #for detail_addr in   detail_items.find_all("div",{'class':re.compile("^info-addr-content")},limit=1):
               #items_name = detail_addr['class'][0].replace("-","_")
               #print(items_name ,':',detail_addr.get_text())                                                    
               detail_level = detail_addr.get_text().replace('\n','')
       
       
       ### 有效期限                                                                                                
       
       for detail_items in soup_detail.find_all("div",{"class":"detail-title-info"}) :
           for detail_exp in detail_items.find_all("span",{"class":"detail-info-span"},limit=1):
           #for detail_exp in detail_items.find_all("span",{'class':re.compile("^detail-info-span")},limit=1):
               detail_exp = detail_exp.get_text().replace('有效期：','').replace('-','')






       return  detail_layout ,detail_level , detail_exp





def getList(dict):
    list = []
    for key in dict.keys():
        list.append(key)

    return list


def house_search(short_url,sale_url) :
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

    """
    user_agent = UserAgent()

    res = requests.get(sale_url, headers={ 'user-agent': user_agent.random })
    res.encoding = 'utf8'
    """
    web.get(sale_url)
    time.sleep(random.randrange(1, 3, 1))

    soup = BeautifulSoup(web.page_source  , "html.parser")
    #soup = BeautifulSoup(res.text  , "html.parser")

    for idx in  soup.find_all("div",{"class":"j-house houseList-item clearfix z-hastag"}) :
      #print(idx.prettify())

      #price = idx.find("div",{"class":"houseList-item-price"})
      #unitprice = idx.find("div",{"class":"houseList-item-unitprice"})
      id = idx['data-bind']
      for itmes_titile in  idx.find_all("div",{"class":"houseList-item-title"}) :
      #  items_name = itmes_titile['class'][0]
      #  print(items_name ,':',itmes_titile.get_text())

         for link in itmes_titile.find_all('a',limit=1) :
           #print('Title:',link.get_text())
           detail_url = short_url + link.get('href')
           #print('ID:',id , link.get_text() , itmes_titile['class'][0].replace("-","_") ,':',detail_url)
           houseid.append(id)
           houseList_item_title.append(link.get_text())
           detail_url_list.append(detail_url)





      ### show detail
      detail_layout ,detail_level , detail_exp = house_detail(detail_url)

      info_floor_layout.append(detail_layout)

      info_floor_addr_level.append(detail_level)

      info_floor_exp.append(detail_exp)

      #time.sleep(random.randrange(3, 5, 1))

      items_name_list=['houseList_item_attrs_shape','houseList_item_attrs_area','houseList_item_attrs_mainarea','houseList_item_attrs_houseage']
      items_name_all = []
      for item_attrs in idx.find_all('span',{'class':re.compile('^houseList-item-attrs')} ):
         #item_attrs_all.append(item_attrs['class'][0])
         items_name = item_attrs['class'][0].replace("-","_")
         #print(items_name ,':',item_attrs.get_text())
         ### 型態
         if items_name == 'houseList_item_attrs_shape' :
           houseList_item_attrs_shape.append(item_attrs.get_text())
           items_name_all.append(items_name)
         ### 權狀
         if items_name == 'houseList_item_attrs_area' :
           houseList_item_attrs_area.append(item_attrs.get_text())
           items_name_all.append(items_name)
         ### 主建物
         if items_name == 'houseList_item_attrs_mainarea' :
           houseList_item_attrs_mainarea.append(item_attrs.get_text())
           items_name_all.append(items_name)

         ### 屋齡
         if items_name == 'houseList_item_attrs_houseage' :
           houseList_item_attrs_houseage.append(item_attrs.get_text())
           items_name_all.append(items_name)



      chk = [x for x in items_name_list  if x not in items_name_all ]

      if chk and chk[0] == 'houseList_item_attrs_mainarea':
        houseList_item_attrs_mainarea.append(None)
      #print('houseList_item_attrs_mainarea:',houseList_item_attrs_mainarea)



      #### 住址/社區
      item_span_list=['houseList_item_community','houseList_item_community_link','houseList_item_section','houseList_item_address']

      item_span_all = []
      for item_div in idx.find_all('div',{'class':re.compile('^houseList-item-address-row')} ):
         #items_name = item_div['class'][0].replace("-","_")
         #link = item_div.find_all('a',limit=1)

         #for item_span in item_div.find_all('span',{'class':re.compile('^houseList-item*')} ):

         for item_span in item_div.find_all('span',{'class':re.compile('^houseList-item-community')}) :

           items_name = item_span['class'][0].replace("-","_")
           link = item_span.find_all('a',limit=1)


           if link and items_name == 'houseList_item_community':

             houseList_item_community.append(link[0].get_text().replace('\n                    ',''))

           else :

             houseList_item_community.append(None)
             houseList_item_community_link.append(None)

           item_span_all.append(items_name)


           if link :

             houseList_item_community_link.append(link[0].get('href'))


           item_span_all.append('houseList_item_community_link')

           #print('link:',link)

         for item_span in item_div.find_all('span',{'class':re.compile('^houseList-item-section')} ,limit=1) :
            items_name = item_span['class'][0].replace("-","_")
            houseList_item_section.append(item_span.get_text().replace("-",""))
            item_span_all.append(items_name)

         for item_span in item_div.find_all('span',{'class':re.compile('^houseList-item-address')} ,limit =1) :
            items_name = item_span['class'][0].replace("-","_")
            houseList_item_address.append(item_span.get_text())
            item_span_all.append(items_name)

         #print('item_span_all:',item_span_all)
         #print(item_span_all)
         chk = [x for x in item_span_list  if x not in item_span_all ]

         for chk_idx in  chk :

           if chk_idx == 'houseList_item_community':

             houseList_item_community.append(None)

           elif chk_idx == 'houseList_item_community_link' :

             houseList_item_community_link.append(None)


         #print('houseList_item_community:',houseList_item_community)



      ### 售價/單價

      for item_div in idx.find_all('div',{'class':re.compile('^houseList-item-right')} ):
         for item_price in item_div.find_all('div',{'class':re.compile('^houseList-item-price')} ,limit=1):
             price.append(item_price.get_text())

         for item_price in item_div.find_all('div',{'class':re.compile('^houseList-item-unitprice')} ,limit=1):
             unitprice.append(item_price.get_text())

      ### tags
      for item_div in idx.find_all('div',{'class':re.compile('^houseList-item-tag-row')} ):
         itme_tags= []
         for item_tag in item_div.find_all('div',{'class':re.compile('^houseList-item-tag-item')} ):
            itme_tags.append(item_tag.get_text())

         tags.append(itme_tags)


      #print('===================分隔線===========================')
      #print(' ')


    #web.quit()

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


    #keylist = getList(row_data)

    #for key in keylist :
    #  print(key , ':',len(row_data.get(key)) ,';', row_data.get(key))

    df = pd.DataFrame(row_data)

    return df



#house_search(short_url,sale_url)



web.get(sale_url)
time.sleep(random.randrange(3, 5, 1))

soup_next_pages = BeautifulSoup(web.page_source  , "html.parser")
first_Row = 0
for pages in soup_next_pages.find_all("div",{"class":"pages"}) :
  for last_page in pages.find_all("a",{"class":"pageNext"}) :
     #print(last_page['data-first'])
     total_Rows = int(last_page['data-total'])


today = datetime.date.today().strftime('%Y%m%d')



### delete Expired data or null 
dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
delete_many_mongo_db('591','sale_house',dicct)


while first_Row <  total_Rows:

  if  first_Row == 0 :

     match_row = house_search(short_url,sale_url)

  else :

     req_sale_url = sale_url + '&firstRow='+ str(first_Row) +'&totalRows=' + str(total_Rows)

     #print('sale_url:',req_sale_url)

     match_row = house_search(short_url,req_sale_url)

  records = match_row.copy()
  
  if not records.empty : 

    ### delete Expired data
    #dicct = {"info_floor_exp" : datetime.date.today().strftime('%Y%m%d') }
    #dicct = {"info_floor_exp" : {"$lte" : datetime.date.today().strftime('%Y%m%d') }
    #dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
    #delete_many_mongo_db('591','sale_house',dicct)       

    records["last_modify"]= datetime.datetime.now()
    records =records.to_dict(orient='records')

    try :

         """
         db.sale_house.createIndex({houseList_item_attrs_shape:1,info_floor_layout:1,info_floor_addr_level:1,houseList_item_attrs_area:1,houseList_item_attrs_mainarea:1,houseList_item_attrs_houseage:1,houseList_item_community:1 ,houseList_item_section:1,houseList_item_address:1,price:1,unitprice:1},{ name : "key_duplicate" ,unique : true, background: true})
         db.sale_house.createIndex({info_floor_exp:1}, {background: true})
         """

         insert_many_mongo_db('591','sale_house',records)

         #dicct = {"info_floor_exp" : {"$eq": None } }
         #delete_many_mongo_db('591','sale_house',dicct)
    

    except BulkWriteError as e:
           pass ## when duplicate date riseup error  by pass 


  #update_many_mongo_db('591','sale_house',records)
  
  time.sleep(random.randrange(1, 3, 1))
  print('pages:',first_Row, 'total:',total_Rows, 'done')


  first_Row += 30


  time.sleep(random.randrange(1, 5, 1))
  #idx_next = pages.find("a",{"class":"pageNext"})

web.quit()


### delete Expired data or null 
#dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
dicct = {"info_floor_exp" : {"$eq": None } }
delete_many_mongo_db('591','sale_house',dicct)    

end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))

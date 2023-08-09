from selenium import webdriver
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC
#from selenium.webdriver.common.by import By
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
from dateutil.relativedelta import relativedelta





start_time = datetime.datetime.now()


short_url = 'https://buy.yungching.com.tw'
#sale_url='https://buy.yungching.com.tw/region/住宅_p/台中市-西屯區_c/500-1500_price/3-4_rm/華廈,電梯大廈_type?pg=1'
sale_url='https://buy.yungching.com.tw/region/住宅_p/台中市-北屯區,台中市-西屯區,台中市-南屯區_c/500-1500_price/1-30_age/3-4_rm/華廈,電梯大廈_type/y_park?pg=1'
###　section　西屯區:104 北屯:102 南屯:105
### label 車位:7 陽台:9


options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--no-sandbox")
web = webdriver.Chrome(options=options)


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
     #res = requests.get(detail_url, headers=headers )
     res = requests.get(detail_url,  headers={ 'user-agent': user_agent.random } )
     res.encoding = 'utf8'     

     detail_level = None
     ##detail_exp = None
     detail_title = None
     detail_shape = None
     detail_mainarea = None
     detail_community = None
     detail_community_link = None
     detail_addr = None
     detail_layout = None
     detail_houseage = None
     detail_area = None
     detail_tags = None
     detail_price = None
     detail_unitprice = None
     detail_houseid = None     
     detail_section =None
   
     #soup_detail = BeautifulSoup(web.page_source  , "html.parser")
     soup_detail = BeautifulSoup(res.text  , "html.parser")
   
     ###格局
     for item_attrs in soup_detail.find_all("section",{"class":"m-house-infomation"},limit =1) :
   
        #items_name = detail_items['class'][0].replace("-","_")
        #print(items_name ,':')
   
           for div_attr in item_attrs.find_all("h1",{"class":"house-info-name"},limit =1):
               ## title
               detail_title = div_attr.get_text().replace('\r\n                    ','').replace('\r\n                    \n\n\n','').replace('\n\n\n','')
   
           for div_attr in item_attrs.find_all("div",{"class":"house-info-num"},limit =1):
               ## houseid
               detail_houseid = div_attr.get_text()
   
           for div_attr in item_attrs.find_all("div",{"class":"house-info-addr"},limit =1):
               ## 住址
               detail_addr = div_attr.get_text()
               ## 行政區
               detail_section = div_attr.get_text().split('台中市',1)[1].split('區')[0] + '區'

   
           for div_attr in item_attrs.find_all("div",{"class":re.compile("^house-info-prices")},limit =1):
               ## 總價
               for div_attr_span in div_attr.find_all("span",{"class":"price-num"},limit =1) :
                   detail_price = div_attr_span.get_text() + ' 萬'
               
   
               
           for div_attr in item_attrs.find_all("div",{"class":"m-house-info-wrap"},limit =1):  
              for div_attr_sub in  div_attr.find_all("div",{"class":"house-info-sub"}) :
                 attr_list = []
                 attr_name = div_attr_sub.i['class'][1]
                 
                 ### 權狀
                 if attr_name == 'i-ruler':
                   detail_area = div_attr_sub.get_text().replace('\n\n\n','').replace('\n\n','').replace('建物','權狀')
                   
                 ### 格局 
                 if attr_name == 'i-bed':
                   detail_layout = div_attr_sub.get_text().replace('\n\n ','').replace('                             \n','')
    
                 if attr_name == 'i-file':
                    ### 屋齡/型態/樓層
                   for attr in div_attr_sub.find_all('span') :  
                     attr_list.append(attr.get_text())
   
              ### 屋齡    
              detail_houseage = attr_list[0]
              ### 型態
              detail_shape = attr_list[1]
              ###樓層
              detail_level = attr_list[2]                             
                 
   
     for item_attrs in soup_detail.find_all("section",{"class":"m-house-detail-list"}) :
   
      attr_name =item_attrs['class'][1]
   
      ###車位
      if attr_name == 'bg-car' : 
        ul_attr = item_attrs.find("ul",{"class":"detail-list-lv1"})
        detail_tags=ul_attr.get_text().replace("\n","")
      
           
      ## 社區/社區link 
      if attr_name == 'bg-other' : 
        ul_attr = item_attrs.find("ul",{"class":"detail-list-lv1"})
        for link_attr in ul_attr.find_all("a",limit =1) : 
          ### community_link
          detail_community_link = "http:" + link_attr.get("href")
   
          ### community
          detail_community = link_attr.get_text()
   
      ### 單價
      if attr_name == 'bg-price' :   
        ul_attr = item_attrs.find("ul",{"class":"detail-list-lv1"})  
        for li_attr in ul_attr.find_all("li",{"class":"right"},limit =1) :
           detail_unitprice = li_attr.find('span').get_text().replace('每坪單價約','萬/坪')
   
      ### 主建
      if attr_name == 'bg-square' :   
        ul_attr = item_attrs.find("ul",{"class":"detail-list-lv1"})  
        for li_attr in ul_attr.find_all("li",{"class":"right"},limit =1) :
           for ul_attr in li_attr.find_all("ul",{"class":"detail-list-lv2"},limit =1) :
              detail_mainarea = ul_attr.find('li').get_text().replace('主建物小計','主建')     
        
   
   
     return detail_title ,detail_houseid ,detail_addr, detail_section, detail_community_link ,detail_community  , detail_price ,detail_layout ,detail_level ,detail_area, detail_mainarea ,detail_shape ,detail_houseage ,detail_unitprice ,detail_tags



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




    web.get(sale_url)
    time.sleep(random.randrange(1, 3, 1))

    soup = BeautifulSoup(web.page_source  , "html.parser")
    #soup = BeautifulSoup(hhtml  , "html.parser")

    for idx in  soup.find_all("div",{"class":"l-main-list"}) :

      for itmes_list in  idx.find_all("ul",{"class":"l-item-list"}) :
        ### 每個物件 
        for link_item in itmes_list.find_all("li",{"class":"m-list-item"}) :
           for link in link_item.find_all('a' ,limit =1) :
              detail_url = short_url + link.get('href')


           ### show detail

           detail_title ,detail_houseid ,detail_addr, detail_section, detail_community_link ,detail_community , detail_price ,detail_layout ,detail_level ,detail_area, detail_mainarea ,detail_shape ,detail_houseage ,detail_unitprice ,detail_tags = house_detail(detail_url)

           ### Ttile
           houseList_item_title.append(detail_title)

           ### houseid
           houseid.append(detail_houseid)

           ### detail_url
           detail_url_list.append(detail_url)

           ###社區link
           houseList_item_community_link.append(detail_community_link)

           ###格局
           info_floor_layout.append(detail_layout)
           ###屋齡
           houseList_item_attrs_houseage.append(detail_houseage)
           ###權狀
           houseList_item_attrs_area.append(detail_area)

           ###樓層
           info_floor_addr_level.append(detail_level)
           ###社區
           houseList_item_community.append(detail_community)
           ###地址
           houseList_item_address.append(detail_addr)
           ### 有效期限
           exp = (datetime.date.today() + relativedelta(months=3)).strftime('%Y%m%d')
           info_floor_exp.append(exp)
           ### 型態
           houseList_item_attrs_shape.append(detail_shape)

           ### 主建
           houseList_item_attrs_mainarea.append(detail_mainarea)

           ### 行政區
           houseList_item_section.append(detail_section)

           ### price
           price.append(detail_price)

           ### 單價
           unitprice.append(detail_unitprice)
          

           ### tags
           for itmes_list_tags in link_item.find_all("div",{"class":"item-tags"},limit =1):
              list_tags = []
              for itmes_tags in itmes_list_tags.find_all('span') :
                 if len(itmes_tags) > 0 :
                   list_tags.append(itmes_tags.get_text())

           if not detail_tags == None :
             list_tags.append(detail_tags)

           tags.append(list_tags)

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




### delete Expired data or null 
#dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
#delete_many_mongo_db('591','sale_house',dicct)


web.get(sale_url)
time.sleep(random.randrange(3, 5, 1))

soup_next_pages = BeautifulSoup(web.page_source  , "html.parser")

## get last page
for pages in soup_next_pages.find_all("div",{"class":"m-pagination"},limit=1) :
  for page_num in pages.find_all("a") :
     
     if page_num['ga_label'] == "buy_page_last" : 
       
       buy_page_last = int(page_num.get('href').split('pg=',1)[1])

first =1 
### page of buy_page_last
while first <= buy_page_last :

        if first ==1 :
   
           page_url = sale_url

        else :
           page_url = sale_url.replace('?pg=1','?pg='+str(first))

        #print('page_url:',page_url)
        match_row = house_search(short_url,page_url)
        #match_row = pd.DataFrame()

        records = match_row.copy()
   
        #print('records',records.info())  
        if not records.empty :
     
           ### delete Expired data
           dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
           delete_many_mongo_db('yungching','sale_house',dicct)       
      
           records["last_modify"]= datetime.datetime.now()
           records =records.to_dict(orient='records')
      
           try :
      
               # db.sale_house.createIndex({houseList_item_attrs_shape:1,info_floor_layout:1,info_floor_addr_level:1,houseList_item_attrs_area:1,houseList_item_attrs_mainarea:1,houseList_item_attrs_houseage:1,houseList_item_community:1 ,houseList_item_section:1,houseList_item_address:1,price:1,unitprice:1},{ name : "key_duplicate" ,unique : true, background: true})
                #db.sale_house.createIndex({info_floor_exp:1}, {background: true})
      
                insert_many_mongo_db('yungching','sale_house',records)
      
                #dicct = {"info_floor_exp" : {"$eq": None } }
                #delete_many_mongo_db('591','sale_house',dicct)
      
      
           except BulkWriteError as e:
                  pass ## when duplicate date riseup error  by pass 
      
        
        print('page:', first , 'last_page:',buy_page_last)

        first+=1

        time.sleep(random.randrange(1, 3, 1))

web.quit()


end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))

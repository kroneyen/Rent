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


short_url = 'https://www.sinyi.com.tw'
sale_url ='https://www.sinyi.com.tw/buy/list/500-1500-price/dalou-huaxia-type/plane-auto-mix-mechanical-firstfloor-tower-other-yesparking/1-30-year/3-3-roomtotal/Taichung-city/406-407-408-zip/Taipei-R-mrtline/03-mrt/yesparking/uniprice-asc/uniprice-asc/price-asc/index'
#sale_url ='https://www.sinyi.com.tw/buy/list/500-1500-price/dalou-huaxia-type/plane-auto-mix-mechanical-firstfloor-tower-other-yesparking/1-30-year/3-3-roomtotal/Taichung-city/407-zip/Taipei-R-mrtline/03-mrt/yesparking/uniprice-asc/price-asc/index'
###　section　西屯區:406 北屯:407 南屯:408
### label 車位:7 陽台:9
## https://www.sinyi.com.tw/buy/list/500-1500-price/dalou-huaxia-type/1-30-year/3-3-roomtotal/Taichung-city/406-407-408-zip/Taipei-R-mrtline/03-mrt/yesparking/uniprice-asc/uniprice-asc/price-asc/index
## https://www.sinyi.com.tw/buy/list/500-1500-price/dalou-huaxia-type/plane-auto-mix-mechanical-firstfloor-tower-other-yesparking/1-30-year/3-3-roomtotal/Taichung-city/406-407-408-zip/Taipei-R-mrtline/03-mrt/yesparking/uniprice-asc/uniprice-asc/price-asc/index

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
     detail_tags = []
     detail_price = None
     detail_uniprice = None
     detail_section = None

     user_agent = UserAgent()
     #res = requests.get(detail_url, headers=headers )
     res = requests.get(detail_url,  headers={ 'user-agent': user_agent.random } )
     res.encoding = 'utf8'

     time.sleep(random.randrange(1, 2, 1))
   
   
     #soup_detail = BeautifulSoup(web.page_source  , "html.parser")
     soup_detail = BeautifulSoup(res.text  , "html.parser")
   
     ###格局
     for detail_items in soup_detail.find_all("div",{"class":"buy-content-detail"},limit =1) :
   
        for item_attrs in   detail_items.find_all("div",{'class':re.compile("^buy-content-detail-sm")},limit=1):
   
           for div_attr in item_attrs.find_all("div",{"class":"buy-content-title-name"},limit =1):
               ## title
               detail_title = div_attr.get_text()
   
           for div_attr in item_attrs.find_all("div",{"class":"buy-content-title-id"},limit =1):
               ## houseid
               detail_houseid = div_attr.get_text().replace("(","").replace(")","")
   
           for div_attr in item_attrs.find_all("div",{"class":"buy-content-title-address"},limit =1):
               ## 住址
               detail_addr = div_attr.get_text()
               ##行政區
               detail_section = div_attr.get_text().split('台中市',1)[1].split('區')[0] + '區'

   
           for div_attr in item_attrs.find_all("a",limit =1):
               ## 社區連結
               detail_community_link = div_attr.get('href')
   
           for div_attr in item_attrs.find_all("div",{"class":"communityButton"},limit =1) :
               ## 社區
               detail_community = div_attr.get_text().replace('社區','').replace(' ','')
   
           for div_attr in item_attrs.find_all("div",{"class":"^buy-content-obj-tags-area"},limit =1) :
               ## tag
               for tag_idx in div_attr.find_all("div",{"class":"tags-cell"})   :
   
                 detail_tags.append(tag_idx.get_text())
   
           for div_attr_cell in item_attrs.find_all("div",{"class":"buy-content-cell"}) :
   
               div_attr = div_attr_cell.find("div",{"class":"buy-content-cell-title"})
   
               p = re.compile(fr'^總價')
   
               if  p.findall(div_attr.get_text()) :              
   
                  detail_price = div_attr_cell.find("div",{"class":"buy-content-cell-body"}).get_text().split('萬',1)[0]+'萬'
   
               if  div_attr.get_text() == '格局' :
   
                  detail_layout = div_attr_cell.find("div",{"class":"buy-content-cell-body"}).get_text()
   
               if  div_attr.get_text() == '樓層' :
   
                  detail_level = div_attr_cell.find("div",{"class":"buy-content-cell-body"}).get_text()
   
               if  div_attr.get_text() == '建坪' :
   
                  detail_area = div_attr_cell.find("div",{"class":"buy-content-cell-body"}).get_text()
   
                  ##'主建物'
                  span_list = []
                  for div_attr_cell_span in div_attr_cell.find_all("span") :
   
                     span_list.append(div_attr_cell_span.get_text())
   
                  detail_mainarea = ''.join(span_list).replace("(","").replace(")","")
   
               if  div_attr.get_text() == '類型' :
   
                  detail_shape = div_attr_cell.find("div",{"class":"buy-content-cell-body"}).get_text()
   
               if  div_attr.get_text() == '屋齡' :
   
                  detail_houseage = div_attr_cell.find("div",{"class":"buy-content-cell-body"}).get_text()
   
               if  div_attr.get_text() == '建坪單價' :
                  
                  detail_unitprice = div_attr_cell.find("div",{"class":"buy-content-cell-body"}).get_text()
   
   
     return detail_title ,detail_houseid ,detail_addr, detail_section, detail_community_link ,detail_community  , detail_price ,detail_layout ,detail_level ,detail_area, detail_mainarea ,detail_shape ,detail_houseage ,detail_unitprice




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
    web.get(sale_url)
    time.sleep(random.randrange(1, 3, 1))

    soup = BeautifulSoup(web.page_source  , "html.parser")
    #soup = BeautifulSoup(hhtml  , "html.parser")
    """
    user_agent = UserAgent()
    #res = requests.get(detail_url, headers=headers )
    res = requests.get(sale_url,  headers={ 'user-agent': user_agent.random } )
    res.encoding = 'utf8'

    time.sleep(random.randrange(1, 3, 1))


    #soup = BeautifulSoup(web.page_source  , "html.parser")
    soup = BeautifulSoup(res.text  , "html.parser")
    


    for idx in  soup.find_all("div",{"class":"buy-list-frame"}) :
      ### how much Rows of pages
      for itmes_list in  idx.find_all("div",{"class":"buy-list-item"}) :

        for link in itmes_list.find_all("a",limit=1) :

           detail_url = short_url + link.get('href')
           ### detail_url
           detail_url_list.append(detail_url)


           ### show detail

           detail_title ,detail_houseid ,detail_addr, detail_section, detail_community_link ,detail_community , detail_price ,detail_layout ,detail_level ,detail_area, detail_mainarea ,detail_shape ,detail_houseage ,detail_unitprice = house_detail(detail_url)

           ### Ttile
           houseList_item_title.append(detail_title)

           ### houseid
           houseid.append(detail_houseid)

           ### detail_url
           ###detail_url_list.append(detail_url)

           if not detail_community_link == None :
             items_detail_community_link = short_url + detail_community_link
           else  :
             items_detail_community_link = None

           ###社區link
           houseList_item_community_link.append(items_detail_community_link)

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
        for itmes_list_tags in itmes_list.find_all("div",{"class":"LongInfoCard_Type_SpecificTags"},limit =1):
           list_tags = []
           for itmes_tags in itmes_list_tags.find_all('span',{"class":"specificTag"}) :

              list_tags.append(itmes_tags.get_text())

        for itmes_tags in itmes_list.find_all('span',{"class":"LongInfoCard_Type_Parking"}) :
            if len(itmes_tags) > 0 :
              list_tags.append(itmes_tags.get_text())

        tags.append(list_tags)

      #print('===================分隔線===========================')
      #print(' ')

    #web.close()
     
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


for pages in soup_next_pages.find_all("ul",{"class":"pagination"},limit=1) :
    for page_num in pages.find_all("a",{"class":"pageLinkClassName"}) :

        page_url = sale_url.replace('index',page_num.get_text())


        match_row = house_search(short_url,page_url)

        time.sleep(random.randrange(1, 2, 1))

        records = match_row.copy()
     
        if not records.empty :
     
           ### delete Expired data
           dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
           delete_many_mongo_db('sinyi','sale_house',dicct)       
      
           records["last_modify"]= datetime.datetime.now()
           records =records.to_dict(orient='records')
      
           try :
      
                """
                db.sale_house.createIndex({houseList_item_attrs_shape:1,info_floor_layout:1,info_floor_addr_level:1,houseList_item_attrs_area:1,houseList_item_attrs_mainarea:1,houseList_item_attrs_houseage:1,houseList_item_community:1 ,houseList_item_section:1,houseList_item_address:1,price:1,unitprice:1},{ name : "key_duplicate" ,unique : true, background: true})
                db.sale_house.createIndex({info_floor_exp:1}, {background: true})
                """
      
                insert_many_mongo_db('sinyi','sale_house',records)
      
                #dicct = {"info_floor_exp" : {"$eq": None } }
                #delete_many_mongo_db('591','sale_house',dicct)
      
      
           except BulkWriteError as e:
                  pass ## when duplicate date riseup error  by pass 
      
      
           time.sleep(random.randrange(1, 3, 1))
           print('pages:',page_num.get_text())


      
 
web.quit()


end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))

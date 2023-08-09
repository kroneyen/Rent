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


short_url = 'https://www.century21.com.tw/'
sale_url ='https://www.century21.com.tw/buy/taichung-city/407_406_408/hugh-rise_mid-rise?priceFilter=500-1500&houseShape=equal-3_equal-4&houseAge=1-30&%3Fpage=1&page=1'
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
     #detail_community = None
     #detail_community_link = None
     detail_addr = None
     detail_layout = None
     detail_houseage = None
     detail_area = None
     detail_tags = []
     detail_price = None
     detail_unitprice = None

   
     #soup_detail = BeautifulSoup(web.page_source  , "html.parser")
     soup_detail = BeautifulSoup(res.text  , "html.parser")
   
     for border in soup_detail.find_all("div",{"class":re.compile("^o_border")}) :
    
        for forms in border.find_all("ul",{"class":re.compile("forms unstyled")},limit =1) :   
          for item_attrs in forms.find_all("div",{"class":"w-half"}) :
            for item_attrs_h6 in item_attrs.find_all('h6',{"class":"col-5"} ,limit =1) : 
             
              item_attrs_span = item_attrs_h6.get_text() 
             
    
              if item_attrs_span == '總價' : 
                detail_price = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text().replace('\n','')  
    
              if item_attrs_span == '參考單價' : 
                detail_unitprice = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text()  
    
              if item_attrs_span == '類型/用途' : 
                detail_shape = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text() 
    
              if item_attrs_span == '地址' : 
                detail_addr = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text()  
                ### 行政區
                
                detail_section = detail_addr.split('臺中市',1)[1].split('區')[0] + '區' 
                #detail_section =  info_list[0].split('台中市',1)[1].split('區')[0] + '區'
    
    
              if item_attrs_span == '坪數' : 
                detail_area = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text() 
    
              if item_attrs_span == '樓層' : 
                detail_level = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text()     
    
              if item_attrs_span == '格局' : 
                detail_layout = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text()     
    
              if item_attrs_span == '屋齡' : 
                detail_houseage = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text()         
    
              if item_attrs_span == '車位' : 
                #detail_tags.append(item_attrs.find("div",{"class":re.compile("^col-7")}).get_text())
                chk_text = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text()
                if not chk_text =='無' : 
                  detail_tags.append(chk_text.replace('有','車位').replace('\n',''))
                  #detail_tags.append(item_attrs.get_text().replace('車位有','').replace('\n',''))  
    
              if item_attrs_span == '警衛管理' : 
                chk_text = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text()
                if not chk_text =='無' : 
                  detail_tags.append(chk_text)
                #detail_tags.append(item_attrs.get_text().replace('\n',''))  
    
              if item_attrs_span == '管理費' : 
                #detail_tags.append(item_attrs.find("div",{"class":re.compile("^col-7")}).get_text())
                detail_tags.append(item_attrs.get_text().replace('\n',''))              
        
        for forms in border.find_all("ul",{"class":re.compile("forms unstyled area")},limit =1) : 
          for item_attrs in forms.find_all("div",{"class":"w-half"}) :
            for item_attrs_h6 in item_attrs.find_all('h6',{"class":"col-5"} ,limit =1) : 
             
              item_attrs_span = item_attrs_h6.get_text()
    
              if item_attrs_span == '主建物面積' : 
                detail_mainarea = item_attrs.find("div",{"class":re.compile("^col-7")}).get_text()    
    
    
     return detail_addr,detail_section, detail_price ,detail_layout ,detail_level ,detail_area, detail_mainarea ,detail_shape ,detail_houseage ,detail_unitprice ,detail_tags


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

    for idx in  soup.find_all("ul",{"id":"product_list"}) :
      ### 每個物件
      for itmes_list in  idx.find_all("div",{"class":"item"}) :

        for info_item in itmes_list.find_all("div",{"class":"list_info_item"},limit =1 ) :

          for link_item in info_item.find_all("div",{"class":"list_info_name"},limit =1 ) : 

                for link in link_item.find_all('a' ,limit =1) :
                   detail_url = link.get('href')
                   ### detail_url
                   detail_url_list.append(detail_url)
                   ### show detail
                
                   detail_addr,detail_section, detail_price ,detail_layout ,detail_level ,detail_area, detail_mainarea ,detail_shape ,detail_houseage ,detail_unitprice,detail_tags = house_detail(detail_url)
                   ### Ttile
                   detail_title= link.get_text()
                   houseList_item_title.append(detail_title)
                   
                
                   ### houseid
                   detail_houseid = detail_url.split('buypage/',1)[1]
                   houseid.append(detail_houseid)





        ### 總價
        price.append(detail_price)

        ###社區link
        detail_community_link =None
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
        detail_community = None
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
 
 
 
        ### 單價
        unitprice.append(detail_unitprice)
 
        
        ### tags
        tags.append(detail_tags)

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


    keylist = getList(row_data)

    for key in keylist :
      print(key , ':',len(row_data.get(key)) ,';', row_data.get(key))

    df = pd.DataFrame(row_data)

    return df




### delete Expired data or null 
#dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
#delete_many_mongo_db('591','sale_house',dicct)


web.get(sale_url)
time.sleep(random.randrange(3, 5, 1))

soup_next_pages = BeautifulSoup(web.page_source  , "html.parser")


## get last page
for pages in soup_next_pages.find_all("div",{"id":"pagination"},limit=1) :
  for page_num in pages.find_all("a",{"class":"page-link"}) :

     if not page_num.get("rel") :
       buy_page_last = int(page_num.get_text())


first =1


### page of buy_page_last
while first <= buy_page_last :

        if first ==1 :
   
           page_url = sale_url

        else :
           page_url = sale_url.replace('Fpage=1&page=1','Fpage=1&page='+str(first))


        #print('page_url:',page_url)
        match_row = house_search(short_url,page_url)
        #match_row = pd.DataFrame()
        records = match_row.copy()
   
        #print('records',records.info())  
        if not records.empty :
     
           ### delete Expired data
           dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
           delete_many_mongo_db('century21','sale_house',dicct)       
      
           records["last_modify"]= datetime.datetime.now()
           records =records.to_dict(orient='records')
      
           try :
      
               # db.sale_house.createIndex({houseList_item_attrs_shape:1,info_floor_layout:1,info_floor_addr_level:1,houseList_item_attrs_area:1,houseList_item_attrs_mainarea:1,houseList_item_attrs_houseage:1,houseList_item_community:1 ,houseList_item_section:1,houseList_item_address:1,price:1,unitprice:1},{ name : "key_duplicate" ,unique : true, background: true})
                #db.sale_house.createIndex({info_floor_exp:1}, {background: true})
      
                insert_many_mongo_db('century21','sale_house',records)
      
                #dicct = {"info_floor_exp" : {"$eq": None } }
                #delete_many_mongo_db('591','sale_house',dicct)
      
      
           except BulkWriteError as e:
                  pass ## when duplicate date riseup error  by pass 
        
        print('page:', first , 'last_page:',buy_page_last)
        #print_match = match_row.iloc[:[]]
        #print(print_match)

        first+=1

        time.sleep(random.randrange(1, 3, 1))

web.quit()


end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))

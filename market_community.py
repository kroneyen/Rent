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


#short_url = 'https://sale.591.com.tw'
#sale_url ='https://sale.591.com.tw/?shType=list&regionid=8&section=104,103,105&price=750_1000,1000_1250,1250_1500&pattern=3&label=7,9'
#market = 'https://market.591.com.tw/list?regionId=8&sectionId=104&price=1$_40$&age=_15&purpose=5,6&postType=2,8&isSale=1'
#market = 'https://market.591.com.tw/list?regionId=8&sectionId=104,103,105&price=1$_40$&age=_15&purpose=5,6&postType=2,8&isSale=1'

### regionId : 台中市 price: 單價 1~30萬 ,age : 屋齡 1~30 ,purpose:型態 大樓,華夏 ,posttype= 新建案 中古屋
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



def scroll_wrap( opt ,scroll_web , scroll_max) :

   y = 1000
   last_num =0
   scroll_times = 0
   scroll_tmp_num = 0
   while int(scroll_max) > int(last_num) :

       scroll_web.execute_script("window.scrollTo(0, "+str(y)+")")

       soup_mark = BeautifulSoup(scroll_web.page_source  , "html.parser")

       if  opt == 'community' :

            last_num = int(len(soup_mark.find_all("a",{"class":re.compile("^community-card")})))

       elif  opt == 'house' : 
             
             for last in soup_mark.find_all("div",{"class":"sale-list"}, limit =1) : 

                  last_num = int(len(last.find_all("a")))      

       else : 

              print('opt is not exiest')

       if last_num > 150 : 
                      
              scroll_max = last_num
                 
       elif scroll_tmp_num == last_num :
                     
              scroll_times +=1                           
              print('scroll_times:',scroll_times)

              if scroll_times == 10 : 
                break

       else : ### scroll_tmp_num < last_num
                        
              scroll_tmp_num = last_num 
              scroll_times = 0 
             


       y += 1000
       print(opt ,  last_num)

       time.sleep(random.randrange(1,2, 1))



def house_detail(detail_url):

  user_agent = UserAgent()
  
  """
  headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\
  0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.5304.107 Safari/537.36'}
  """

  res = requests.get(detail_url,  headers={ 'user-agent': user_agent.random } )
  res.encoding = 'utf8'
  time.sleep(random.randrange(1, 2, 1))
  
  detail_level = None
  detail_exp = None
  detail_title = None
  detail_shape = None
  detail_mainarea = None
  detail_community = None
  detail_addr = None
  detail_section = None
  detail_layout = None
  detail_houseage = None
  detail_area = None


  #soup_detail = BeautifulSoup(web.page_source  , "html.parser")
  soup_detail = BeautifulSoup(res.text  , "html.parser")

  ### title
  for detail_items in soup_detail.find_all("h1",{"class":"detail-title-content"},limit=1) :

     detail_title = detail_items.get_text().replace('\n','').replace(' ','')


  for detail_items in soup_detail.find_all("section",{"class":re.compile("^detail-house navScroll")},limit=1) :
    for detail_house_box_list in soup_detail.find_all("div",{"class":"detail-house-box"},limit =2) :
       for detail_house_content in detail_house_box_list.find_all("div",{"class":"detail-house-content"},limit=1) :
          for  detail_house_item in detail_house_content.find_all("div",{"class":"detail-house-item"}) :
           
                
              try :   
                  tag_text= detail_house_item.find("div",{"class":"detail-house-key"}).get_text()

                  if tag_text == '型態' :
                     ### 型態

                     detail_shape = detail_house_item.find("div",{"class":"detail-house-value"}).get_text()
                     #print('detail_shape:',detail_shape)

                  if tag_text == '主建物' :
                     ### 主建
                     detail_mainarea = detail_house_item.find("div",{"class":"detail-house-value"}).get_text()
                     #print('detail_mainarea:',detail_mainarea)

              except :
                  pass      


  ### 有效期限
  for detail_items in soup_detail.find_all("div",{"class":"detail-title-info"}) :
    for detail_exp in detail_items.find_all("span",{'class':re.compile("^detail-info-span")},limit=1):
       detail_exp = detail_exp.get_text().replace('有效期：','').replace('-','')


  ###格局/屋齡/權狀
  for detail_items in soup_detail.find_all("div",{"class":"info-box-floor"}) :

     items_name = detail_items['class'][0].replace("-","_")
     #print(items_name ,':')
     for detail_floor in   detail_items.find_all("div",{'class':re.compile("^info-floor-left")}):
     #for detail_floor in   detail_items.find("div",{'class':re.compile("^info-floor-key")}):
        #items_name = detail_floor['class'][0].replace("-","_")
        #print(items_name ,':',detail_floor.get_text())
        #detail_layout.append(detail_floor.get_text())
        try :

            floor_text = detail_floor.find("div",{"class":"info-floor-value"}).get_text()

            if  floor_text == '格局' :
               ### 格局
               detail_layout = detail_floor.find("div",{"class":"info-floor-key"}).get_text()
        
            if floor_text == '屋齡' :
               ### 屋齡
               detail_houseage = detail_floor.find("div",{"class":"info-floor-key"}).get_text()

            if floor_text == '權狀坪數' :
               ### 權狀坪數
               detail_area = detail_floor.find("div",{"class":"info-floor-key"}).get_text()

        except : 

             pass



  ### 樓層/社區/地址
  for detail_items in soup_detail.find_all("div",{"class":"info-box-addr"}) :
     #items_name = detail_items['class'][0].replace("-","_")
     #print(items_name ,':')
     for detail_addr in   detail_items.find_all("div",{'class':re.compile("^info-addr-content")}):
        items_name = detail_addr['class'][0].replace("-","_")
        #print(items_name ,':',detail_addr.get_text())

        try :

            add_text = detail_addr.find("span",{"class":"info-addr-key"}).get_text()

            if  add_text == '樓層' :
               ### 樓層
               detail_level = detail_addr.find("span",{"class":"info-addr-value"}).get_text()
        
            if add_text == '社區' :
               ### 社區
               detail_community = detail_addr.find("span",{"class":"info-addr-value"}).get_text()
               detail_community_link = detail_addr.get('href')

            if add_text == '地址' :
               ### 地址
               detail_addr = detail_addr.find("span",{"class":"info-addr-value"}).get_text()
               detail_section = detail_addr.split('區')[0].split('市')[1] + '區'

        except : 

             pass


  return detail_layout ,detail_houseage ,detail_area, detail_level ,detail_community ,detail_addr ,detail_section, detail_exp ,detail_title ,detail_shape ,detail_mainarea





def getList(dict):
    list = []
    for key in dict.keys():
        list.append(key)

    return list



def split_houseid(url) :

  houseid = url.split('.html')[0].split("/")[-1]
  return houseid



def community_search(link) :

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

    user_agent = UserAgent()
   
    comm_item_max = 0

    last_comm_item_max = 0
 
    """
    res = requests.get(market, headers={ 'user-agent': user_agent.random })
    res.encoding = 'utf8'

    time.sleep(random.randrange(1, 2, 1))

    soup = BeautifulSoup(res.text  , "html.parser")


    for idx in  soup.find_all("section",{"class":"list-container"}) :
    
      ### Row of limit
      for link in  idx.find_all("a",{"class":re.compile("^community-card")}) :
        ### 社區連結
    """
    ### filter 
    community_link =  str(link) + '/sale?total_price=500$_2000$&room=3,4&label=park'


    ####community_link.append(link.get('href'))
    
    ####to-do#####   
    try :
       
        try :
             web.execute_script("window.open()")  
             time.sleep(random.randrange(1,2, 1))
             
        except BulkWriteError as e:
              print('open:',e.details)

        try : 
              nnew = web.window_handles[1]
              time.sleep(random.randrange(1,2, 1))            
         
        except BulkWriteError as e:
              print('handel:',e.details)

        try :         
             web.switch_to.window(nnew)
             web.get(community_link)

        except BulkWriteError as e:
              print('switch:',e.details)
      
        community_soup = BeautifulSoup(web.page_source  , "html.parser")

        for section_item in community_soup.find_all("section",{"class":"with-broker t5-container"},limit =1) : 
                 for total_item in section_item.find_all("div",{"class":"total"},limit =1) : 
                     comm_item_max = int(total_item.strong.get_text())

        #print('item_max:', item_max)
        if comm_item_max >= 20 :

           scroll_wrap('house',web , comm_item_max)

        community_soup = BeautifulSoup(web.page_source  , "html.parser")

        

    except : 
           r = requests.get(community_link, headers={ 'user-agent': user_agent.random } )
           r.encoding = 'utf8'
           community_soup = BeautifulSoup(r.text  , "html.parser")


   
    ###社區銷售清單
    #for sale_list in community_soup.find_all("div",{"class":"sale-list"}) :
    for sale_list in community_soup.find_all("div",{"class":"sale-list"},limit =1):

          last_comm_item_max = int(len(sale_list.find_all("a")) )
          """
          if last_comm_item_max > 30 :

                 last_comm_item_max = 30 
          else : 

                 pass
          """
          #for detail_idx in  sale_list.find_all("a" , limit =5) :
          for detail_idx in  sale_list.find_all("a" ,limit = last_comm_item_max) :

            comm_item_url = detail_idx.get('href')
            #https://sale.591.com.tw/home/house/detail/2/13733829.html

            ### id
            houseid.append(split_houseid(comm_item_url))

            ### show detail

            detail_layout ,detail_houseage ,detail_area, detail_level ,detail_community ,detail_addr ,detail_section, detail_exp ,detail_title ,detail_shape ,detail_mainarea = house_detail(comm_item_url)
            
            ### Ttile
            houseList_item_title.append(detail_title)

            ### detail_url
            detail_url_list.append(comm_item_url)

            #houseList_item_community_link.append(link.get('href'))
            houseList_item_community_link.append(link)
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
            info_floor_exp.append(detail_exp)
            ### 型態
            houseList_item_attrs_shape.append(detail_shape)

            ### 主建
            houseList_item_attrs_mainarea.append(detail_mainarea)

            ### 行政區
            houseList_item_section.append(detail_section)


            for item_info in sale_list.find_all("div",{"class":"info"},limit =1) :

               """
               detail_info_span = []
               
               for detail_span in item_info.find_all("div",{"class":"detail"},limit =1):
                 for span_tag in detail_span.find_all("span") :
                    detail_info_span.append(span_tag.get_text())


               info_floor_layout.append(detail_info_span[0])
               houseList_item_attrs_area.append(detail_info_span[1])
               info_floor_addr_level.append(detail_info_span[2])
               """

               for price_info in item_info.find_all("div",{"class":"price-info"},limit =1):
                  ### 售價
                  for detail_price in price_info.find_all("span",{"class":"total"},limit =1) :
                    price.append(detail_price.get_text())
                  ### 單價
                  for detail_price in price_info.find_all("span",{"class":"price"},limit =1) :
                    unitprice.append(detail_price.get_text())
            ### tag 
            for item_tag_list in detail_idx.find_all("div",{"class": "tags"}, limit =1 ):
                     tags.append(item_tag_list.get_text())

    time.sleep(random.randrange(1,2, 1))
    #print('community: ',)
    #community_num +=1


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
    return df ,comm_item_max ,last_comm_item_max
    #return df 


#house_search(short_url,sale_url)


"""
web.get(sale_url)
time.sleep(random.randrange(3, 5, 1))

soup_next_pages = BeautifulSoup(web.page_source  , "html.parser")
first_Row = 0
for pages in soup_next_pages.find_all("div",{"class":"pages"}) :
  for last_page in pages.find_all("a",{"class":"pageNext"}) :
     #print(last_page['data-first'])
     total_Rows = int(last_page['data-total'])


today = datetime.date.today().strftime('%Y%m%d')
"""


### delete Expired data or null 
dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
delete_many_mongo_db('591','market_house',dicct)
#market_list = ['104','103','105']
market_list = ['104']

for market_idx in market_list :

   market = 'https://market.591.com.tw/list?regionId=8&sectionId='+ market_idx + '&price=1$_40$&age=_15&purpose=5,6&postType=2,8&isSale=1'
   

   web.get(market)

   
   time.sleep(random.randrange(1,3, 1))
   
   ### cal max items
   soup_mark = BeautifulSoup(web.page_source  , "html.parser")
   
   for h2_class in soup_mark.find_all("h2",{"class":"total"} ,limit =1 ) :
   
       community_max = int(h2_class.find('span').get_text())
   print('section:',market_idx)   
   print('community_max:' ,community_max)
   ### scroll all items
   scroll_wrap('community',web,community_max)
   
   #time.sleep(random.randrange(1,3, 1))
   
   ###reload data
   soup_mark = BeautifulSoup(web.page_source  , "html.parser")
   
   community_num = 1
   item_max = 0
   last_item_max = 0
   
   ### community link
   for idx in  soup_mark.find_all("section",{"class":"list-container"}) :
   
       ##計算本次筆數
       last_num = len(idx.find_all("a",{"class":re.compile("^community-card")}))
   
       print( 'last_num' ,  last_num )
   
       for link in  idx.find_all("a",{"class":re.compile("^community-card")}) :
            ###community_link
            for community_info in link.find_all("div",{"class":"community-info"} ,limit =1) :
                for em in community_info.find_all("em",{"class":""} , limit =1) :
                    community_name = em.get_text()
                    #print( 'community_{}:{}'.format(community_num,community_name))
   
            #community_num +=1         
            try:
                 match_row , item_max,last_item_max = community_search(link.get('href'))
                 print( 'community_{} : {} , itmes: {} , last_item_max:{} '.format(community_num,community_name,item_max,last_item_max))
   
            except BulkWriteError as e:
                 print(e.details)
                 break
   
            #match_row  = community_search(link.get('href'))         
            #match_row ,sale_times = community_search(link.get('href'))
            #print( 'community_{} : {}  itmes: {}'.format(community_num,community_name,sale_times))
            records = match_row.copy()
   
   
            if not records.empty :
   
                ### delete Expired data
                #dicct = {"info_floor_exp" : datetime.date.today().strftime('%Y%m%d') }
                #dicct = {"info_floor_exp" : {"$lte" : datetime.date.today().strftime('%Y%m%d') }
                #dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
                #delete_many_mongo_db('591','sale_house',dicct)       
   
                records["last_modify"]= datetime.datetime.now()
                ###delete null data
                records.dropna()
                records =records.to_dict(orient='records')
   
                try :
   
                     """
                     db.market_house.createIndex({houseList_item_attrs_shape:1,info_floor_layout:1,info_floor_addr_level:1,houseList_item_attrs_area:1,houseList_item_community:1 ,houseList_item_section:1,houseList_item_address:1,price:1,unitprice:1},{ name : "key_duplicate" ,unique : true, background: true})
                     db.sale_house.createIndex({info_floor_exp:1}, {background: true})
                     db.market_house.createIndex({info_floor_addr_level:1,houseList_item_community:1 ,houseList_item_section:1,price:1},{ name : "key_duplicate" ,unique : true, background: true})
                     """
   
                     insert_many_mongo_db('591','market_house',records)
   
                     #dicct = {"info_floor_exp" : {"$eq": None } }
                     #delete_many_mongo_db('591','sale_house',dicct)
   
   
                except BulkWriteError as e:
                       pass ## when duplicate date riseup error  by pass 
   
   
            time.sleep(random.randrange(1, 2, 1))
            community_num +=1
   ### for loop section
   time.sleep(random.randrange(30, 60, 10))

       
web.quit()





web.quit()




### delete Expired data or null 
#dicct = {"$or" : [  {"info_floor_exp":  {"$lte" : datetime.date.today().strftime('%Y%m%d') } }, {"info_floor_exp" : {"$eq": None } } ] }
dicct = {"info_floor_exp" : {"$eq": None } }
delete_many_mongo_db('591','market_house',dicct)    

end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))

import random
import requests
import time
import re
import pandas as pd
from pymongo import MongoClient
import datetime
from fake_useragent import UserAgent
import redis
from collections import Counter


today = datetime.date.today().strftime("%Y-%m-%d")

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)

#notify_time = "23:00:00"


def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list




###local mongodb
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )




### atlas mongodb conn info
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)






def read_mongo_db(_db,_collection,dicct,_columns):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)



def delete_redis_data(today):

     dd = 10 ##days :10
     while  len(r.keys('*_reurl_lists')) >2 and dd > 1 :

       yy = today - datetime.timedelta(days=dd)
       yy_key = yy.strftime("%Y%m%d")+'_reurl_lists_sale'

       if r.exists(yy_key) : ### del archive  key
          r.delete(yy_key)

       dd -=1


def insert_redis_data(_key,_values):
    line_display = []
    if r.exists(_key) :
           _list = r.lrange(_key,'0','-1')
           for i_value in _values :
               if i_value.replace("https://reurl.cc/","") not in _list :
                  line_display.append(i_value)
                  r.rpush(_key,i_value.replace("https://reurl.cc/",""))

    else :
       for i_value in _values :
           line_display.append(i_value)
           r.rpush(_key,i_value.replace("https://reurl.cc/",""))
           
    return line_display 





def reurl_API(link_list,_key):

     reurl_link_list = []
     reurl_api_key = "".join(get_redis_data('short_url_key','hget',_key,'NULL'))  ## list_to_str


     if _key  ==  "reurl_key" :

        
        s_reurl = 'https://api.reurl.cc/shorten'
        s_header = {"Content-Type": "application/json" , "reurl-api-key": reurl_api_key}

        for link in link_list :
            s_data = {"url": link}


            r = requests.post(s_reurl, json= s_data , headers=s_header ).json()

            try :
                rerul_link = r["short_url"]
            except :
                rerul_link = link

            reurl_link_list.append(rerul_link)


            time.sleep(round(random.uniform(0.5, 1.0), 10))


     elif   _key  ==  "ssur_key" :

         

        for link in link_list :
            s_reurl = 'https://ssur.cc/api.php?'
            s_data = {"format": "json" , "appkey": reurl_api_key ,"longurl" : link}


            r = requests.post(s_reurl, data= s_data  ).json()

            try :
                  rerul_link = r["ae_url"]
            except :
                  rerul_link = link
                  
            reurl_link_list.append(rerul_link)


            time.sleep(random.randrange(2, 5, 1))

     elif _key  ==  "reurl_hotmail_key" :


        s_reurl = 'https://api.reurl.cc/shorten'
        s_header = {"Content-Type": "application/json" , "reurl-api-key": reurl_api_key}

        for link in link_list :
            s_data = {"url": link}


            r = requests.post(s_reurl, json= s_data , headers=s_header ).json()

            try :
                rerul_link = r["short_url"]
            except :
                rerul_link = link

            reurl_link_list.append(rerul_link)


            time.sleep(round(random.uniform(0.5, 1.0), 10))

              

     return reurl_link_list




def send_line_notify(token,msg):

    requests.post(
    url='https://notify-api.line.me/api/notify',
    headers={"Authorization": "Bearer " + token},
    data={'message': msg}
    )




def sale_house_line(_today,_db,_section):

    """
    cal_date_str = _today + "T00:00:00+00:00"
    cal_date = datetime.datetime.strptime(cal_date_str, '%Y-%m-%dT%H:%M:%S%z')
    """
    #cal_date_str = '2023-07-20' + "T00:00:00"
    #lt_cal_date_str = '2023-07-21' + "T00:00:00"
    cal_date_str = _today + "T00:00:00"
    cal_date = datetime.datetime.strptime(cal_date_str, '%Y-%m-%dT%H:%M:%S')
    #cal_date_lt = datetime.datetime.strptime(lt_cal_date_str, '%Y-%m-%dT%H:%M:%S')
    exp_date =  datetime.date.today().strftime("%Y%m%d")
    #exp_date = '20230720'
     

    #cal_date = cal_date - datetime.timedelta(days =1)
    #print('cal_date:',cal_date)
    dicct= {"last_modify":  {"$gte" : cal_date},"houseList_item_section" : _section ,"info_floor_exp":{"$gt":exp_date}}
    #dicct= {"last_modify":  {"$gte" : cal_date , "$lt" : cal_date_lt} ,"houseList_item_section" : _section ,"info_floor_exp":{"$gt":exp_date}}
    column= {"_id":0}
    
    
    mydoc = read_mongo_db(_db,'sale_house',dicct,column) 
    match_row = pd.DataFrame(list(mydoc))
    #rediskeys = str(_today) + '_reurl_lists_sale'

    #line_display_list = insert_redis_data(rediskeys,match_row) ##  5rows
    #print(match_row.info())   

    try :

         match_row['URL'] = reurl_API(match_row['detail_url'].values, 'reurl_hotmail_key') ## short url(轉址)

    except :

         pass


    match_row_line = match_row.copy()
    
    #print(match_row_line.info())
    return match_row_line.head(2)


### main call 

db_list = ['591','sinyi','yungching','century21']
section_list = [ '西屯區','南屯區', '北屯區']


for section_idx in section_list :

   sale_house_df = pd.DataFrame()
   match_row_line = pd.DataFrame()
   
   for db_idx in  db_list :

       sale_house_df = sale_house_line(today,db_idx,section_idx)
       match_row_line = pd.concat([match_row_line,sale_house_df],axis=0)

       time.sleep(round(random.uniform(0.3, 1.0), 10))

   
   try :  
      #print(match_row)                                                                                                                                      
      if not match_row_line.empty  :

                match_row = match_row_line.iloc[:,[1,14,18]]
                match_row.columns=['Title','price','link']

                line_key_list =[]
           
                line_key_list.append(get_redis_data('line_key_hset','hget','591_sale','NULL')) ## for rss_google of signle                                   
                
                #print('section_idx:',section_idx) 
                for match_row_index in range(0,len(match_row),5) :
                    #msg = "\n " + match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False)  ## for line notify msg 1000  character limit 
                    msg = " " + section_idx + "\n " + match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False)  ## for line notify msg 1000  character limit 
   
                    ### for multiple line group                                                                                                              
                    for line_key in  line_key_list : ##                                                                                                      
                        send_line_notify(line_key,msg)
                    #print(match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False))
                    time.sleep(random.randrange(1, 3, 1))                   
   
      time.sleep(random.randrange(5, 10, 1))
   
   except : 
   	       pass                                                             


   time.sleep(random.randrange(120, 240, 10)) 

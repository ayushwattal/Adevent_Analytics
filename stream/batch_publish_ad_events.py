from google.cloud import pubsub
#from google.cloud  import storage
from time import sleep
import random
import datetime,time
import json,re,os
import string

publisher = pubsub.PublisherClient()


project = "advertising-analytics-grp5"
batch_topic = "adevents_batch_data_informer"
batch_topic_path = publisher.topic_path(project, batch_topic)


#Creating JSON ad event with custom data
publisher_ids = [123,125,168,189,195]
page_ids = ['SPORTS_1','FINANCE_3','FASHION_4','TECH_5','GAMES_3','FRONTPAGE_1']
ad_poisions = ['LTOP1','LTOP2','BANNER1','RBANNER1','FOOTER1','SCROLL1']


user_agents = ["Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0",
"Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0",
"Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1",
"Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/69.0.3497.105 Mobile/15E148 Safari/605.1",
"Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1",
"Mozilla/5.0 (iPhone9,3; U; CPU iPhone OS 10_0_1 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14A403 Safari/602.1",
"Mozilla/5.0 (Linux; Android 6.0.1; SM-G920V Build/MMB29K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.98 Mobile Safari/537.36",
"Mozilla/5.0 (Linux; Android 6.0.1; SM-G935S Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36",
"Mozilla/5.0 (Linux; Android 8.0.0; SM-G960F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36",
"Mozilla/5.0 (Linux; Android 7.0; SM-G892A Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/60.0.3112.107 Mobile Safari/537.36",
"Mozilla/5.0 (Linux; Android 8.0.0; SM-G960F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36",
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
"Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1",
"Mozilla/5.0 (CrKey armv7l 1.5.16041) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.0 Safari/537.36",
"Mozilla/5.0 (Linux; U; Android 4.2.2; he-il; NEO-X5-116A Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30",
"Mozilla/5.0 (Linux; Android 5.1; AFTS Build/LMY47O) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/41.99900.2250.0242 Safari/537.36",
"Mozilla/5.0 (Linux; Android 7.0; Pixel C Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.98 Safari/537.36",
"Mozilla/5.0 (Linux; Android 6.0.1; SGP771 Build/32.2.A.0.253; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.98 Safari/537.36",
"Mozilla/5.0 (Linux; Android 6.0.1; SHIELD Tablet K1 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Safari/537.36"]

clicked = ['yes','no','unknown']
latency = [3523,4356,6789,1234,4567,120,1500,2300,]

ad_id_list = []
for ad_id in range (400,500):
    ad_id_list.append(ad_id)
server_ids = ['eastcoast1','central1','westcoast1','asia1','australia1','eu1','africa1']


def userid_generator(size=10, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


server_fp = open("server_events.json","w+")

client_fp = open("client_events.json","w+")

#Create batch file for specific Data and Hour
n = 11
my_oldtime = int(time.time()) -(n*24*3600)
formatted=(time.strftime("%Y%m%d%H", time.localtime(int(my_oldtime))))

day = (formatted[:-2])
hr = (formatted[-2:])

folder_path = (day+"/"+hr)
os.makedirs(folder_path)

#Batch folder contains both server and client
server_fp = open(folder_path+"/server_events.json","w+")
client_fp = open(folder_path+"/client_events.json","w+")


#Generate 100 beacons (Ad events [Client and Server])
for beacons in range(100):
    client_ad_event ={}
    server_ad_event = {}
    server_ad_event['ad_id'] = client_ad_event['ad_id'] = random.choice(ad_id_list)
    server_ad_event['user_id'] = client_ad_event['user_id'] = userid_generator(10)
    server_ad_event['publisher_id'] = client_ad_event['publisher_id'] = random.choice(publisher_ids)
    server_ad_event['page_id'] = client_ad_event['page_id'] = random.choice(page_ids)
    server_ad_event['ad_position'] = client_ad_event['ad_position'] = random.choice(ad_poisions)
    server_ad_event['user_agent'] = client_ad_event['user_agent'] = random.choice(user_agents)
    server_ad_event['server_id'] = random.choice(server_ids)
    client_ad_event['latency'] = random.randrange(1000,4500)
    server_ad_event['stimestamp'] = my_oldtime
    client_ad_event['ctimestamp'] = server_ad_event['stimestamp']+12
    client_ad_event['clicked'] = random.choice(clicked)
#    print(server_ad_event)
#    print(client_ad_event)
    server_fp.write(json.dumps(server_ad_event)+"\n")
    client_fp.write(json.dumps(client_ad_event)+"\n")


#upload to GCS
os.system('gsutil cp -r '+day+' gs://adevents_hourly_batch_data/')

#Inform pub/sub about new batch file in GC
batch_event = {"bucket": "gs://adevents_hourly_batch_data", "data_frequency": "hourly", "folder_path":folder_path,"server_events_file":"server_events.json","client_events_file":"client_events.json"}
publisher.publish(batch_topic_path, bytes(json.dumps(batch_event),"utf-8"))


   
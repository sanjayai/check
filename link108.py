import requests,datetime,json
import pandas as pd
import re,os
import pymssql
from random import randrange
import requests
from unidecode import unidecode
import Eventapi_connection,sys


Status=0
inputtable = 'navigateinput'
outputtable='input_chk'
domainid = 108

startid,endid=1,1000000000
#script, Status ,startid, endid, inputtable, outputtable, Offline, proxyId, DB =sys.argv
ms_host=Eventapi_connection.ms_host
ms_pass=Eventapi_connection.ms_pass
ms_db=Eventapi_connection.ms_db
ms_user=Eventapi_connection.ms_user

db       = pymssql.connect(host =ms_host, user =ms_user, password =ms_pass, database = ms_db)
cursor   = db.cursor()
print("DB_Connected")

cursor.execute("Select id,url from" +' '+inputtable+' '+ "with (nolock) where domainid= '%s' and id  between '%s' and '%s' and Status = '%s' order by id" % (domainid,startid, endid, Status)) 
resultset=cursor.fetchall() 

proxyq = f"select proxy from proxy_list with (nolock) where status = %s"%(proxyId)
cursor.execute(proxyq)
proxyset = cursor.fetchall()

try:
    for rset in resultset:
        id=rset[0]
        print(id)
        URL=rset[1]
#         print(URL)
        irand = randrange(0, len(proxyset))
        proxy_id = proxyset[irand]
        proxy1 = proxy_id[0]
        
        if 'https' in URL:
            proxies = {"https": "http://%s"% (proxy1)}
        else:
            proxies = {"http": "http://%s"% (proxy1)}
        
#         print(proxies)
        headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36','accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9','accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',}
        response = requests.get(URL, headers=headers,proxies=proxies)
        html=response.text
        if re.search(r"(?s)formDigestElement.value = '(.*?),",html):
            code=re.search(r"(?s)formDigestElement.value = '(.*?),",html).group(1)
    #             print(code)
        headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36','content-type': 'application/json;odata=verbose','accept': 'application/json;odata=verbose','x-requested-with': 'XMLHttpRequest','x-requestdigest': str(code),'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',}
        params = (
            ('$select', 'ID,Title,StartDate,OData__EndDate,Location,DWTCIsLocalEvent,DWTCFriendlyURL,FieldValuesAsText/DWTCEventSectors,FieldValuesAsText/DWTCEventTypes,FieldValuesAsText/DWTCEventVenues,FieldValuesAsText/DWTCEventAudiences,FieldValuesAsHtml/PublishingPageImage'),
            ('$expand', 'FieldValuesAsText,FieldValuesAsHtml'),
            ('$top', '121'),
        )
        sdate=datetime.datetime.now().strftime('%Y-%m-%d')
#         print(sdate)
        edate = pd.to_datetime(sdate) + pd.DateOffset(days=365)
        edate = re.sub(r'\s.*',r'',str(edate))
#         print(edate)
        data={"query":{"__metadata":{"type":"SP.CamlQuery"},"ViewXml":"<View Scope='RecursiveAll'><Query><OrderBy><FieldRef Name='StartDate' /></OrderBy><Where><And><Eq><FieldRef Name='ContentType' /><Value Type='Text'>DWTC Event Page</Value></Eq><And><And><IsNotNull><FieldRef Name='StartDate' /></IsNotNull><IsNotNull><FieldRef Name='_EndDate' /></IsNotNull></And><Or><And><Geq><FieldRef Name='StartDate' /><Value Type='DateTime'>"+str(sdate)+"T00:00:00Z</Value></Geq><Lt><FieldRef Name='StartDate' /><Value Type='DateTime'>"+str(edate)+"T00:00:00Z</Value></Lt></And><Or><And><Geq><FieldRef Name='_EndDate' /><Value Type='DateTime'>2022-02-01T00:00:00Z</Value></Geq><Lt><FieldRef Name='_EndDate' /><Value Type='DateTime'>2022-03-01T00:00:00Z</Value></Lt></And><And><Lt><FieldRef Name='StartDate' /><Value Type='DateTime'>2022-02-01T00:00:00Z</Value></Lt><Gt><FieldRef Name='_EndDate' /><Value Type='DateTime'>2022-03-01T00:00:00Z</Value></Gt></And></Or></Or></And></And></Where></Query></View>"}}
        response = requests.post('https://www.dwtc.com/en/events-site/_api/web/lists/GetByTitle(%27Pages%27)/getitems', headers=headers, params=params, json=data,proxies=proxies)
        print(response)
    #     open("lkj.html","w").write(str(response.text))
    #     print("done")
        js=json.loads(str(response.text))
        if response.status_code == 200:
            if 'd' in js:
                block=js['d']
                if 'results' in block:
                    data=block['results']
                    for data1 in data:
                        if 'DWTCFriendlyURL' in data1:
                            url='https://www.dwtc.com'+data1['DWTCFriendlyURL']
                            cursor.execute("insert into "+outputtable+" (url,domainid,status) Values ('%s','%s','%s')"%(url,domainid,Status))
                            db.commit()
                    cursor.execute("UPDATE" +' '+inputtable+' '+ "SET Status='1' where id='%s'" % (id))
                    db.commit()
        else:
            cursor.execute("UPDATE" +' '+inputtable+' '+ "SET Status='2' where id='%s'" % (id))
            db.commit()
                
except Exception as e:
    print(e)

            

from datetime import date
import datetime
import pymssql,sys
from datetime import date, timedelta
import Eventapi_connection
 
ms_host = Eventapi_connection.ms_host
ms_user = Eventapi_connection.ms_user
ms_pass = Eventapi_connection.ms_pass
ms_db   = Eventapi_connection.ms_db

domainid=962
# startid=1
# endid=1000000
Status=0
inputtable='ticket_master_calendar'
outputtable = 'input'

script, Status ,startid, endid, inputtable, outputtable, Offline, proxyId, DB =sys.argv

db =pymssql.connect(host=ms_host, user=ms_user, password=ms_pass, database=ms_db)
cursor = db.cursor()
print ("DB_Connected")

# selectq="Select domainid,url from" +' '+inputtable+' '+ "with (nolock) where domainid= '%s' and id  between '%s' and '%s' and Status = '%s' order by id" % (domainid,startid, endid, Status)
# cursor.execute(selectq)
# resultset =cursor.fetchall()
# print(resultset)
#Date format is (YYYY, MM, DD)
# start_date = date(2022,2, 18)
selectq="Select startdate,enddate from ticket_master_calendar with (nolock)"
cursor.execute(selectq)
result =cursor.fetchall()
for dates in result:
    start_date=dates[0]
    end_date=dates[1]

def insert(url,Domainid,status):
    SQLUpdate = "INSERT Into "+(outputtable)+"(url,domainid,Status) values(N'%s',N'%s',N'%s')" %(url,domainid,Status)
#     print("INSERT Into "+(outputtable)+"(url,domainid,Status) values(N'%s',N'%s',N'%s')" %(url,domainid,Status))
    cursor.execute(SQLUpdate)
    db.commit()
    print("Inserted")
##today = date.today()
##current_date = today.strftime("%Y-%m-%d")
##print(current_date)
date_list=[]
delta = end_date - start_date
print("Start Date :",start_date)
print("End Date :",end_date)
for i in range(delta.days + 1):
    day = start_date + timedelta(days=i)
    date_list.append(str(day))
    #print(day)

for day in date_list:
    for x in range(1,50):
        part1_url="https://www.ticketmaster.com/api/next/graphql?operationName=CategorySearch&variables={%22localStartEndDateTime%22:%22"+str(day)+"T00:00:00,"+str(day)+"T15:59:59"
        part2_url=f"%22,%22sort%22:%22date,asc%22,%22page%22:{x}"
        part3_url=",%22size%22:100,%22lineupImages%22:true,%22withSeoEvents%22:true,%22radius%22:%2210000%22,%22type%22:%22event%22,%22includeTBA%22:%22yes%22,%22includeTBD%22:%22yes%22}&extensions={%22persistedQuery%22:{%22version%22:1,%22sha256Hash%22:%225664b981ff921ec078e3df377fd4623faaa6cd0aa2178e8bdfcba9b41303848b%22}}"
        url=str(part1_url+part2_url+part3_url)
        insert(url,domainid,Status)
for day in date_list:
    for x in range(1,50):
        part1_url="https://www.ticketmaster.com/api/next/graphql?operationName=CategorySearch&variables={%22localStartEndDateTime%22:%22"+str(day)+"T16:00:00,"+str(day)+"T23:59:59"
        part2_url=f"%22,%22sort%22:%22date,asc%22,%22page%22:{x}"
        part3_url=",%22size%22:100,%22lineupImages%22:true,%22withSeoEvents%22:true,%22radius%22:%2210000%22,%22type%22:%22event%22,%22includeTBA%22:%22yes%22,%22includeTBD%22:%22yes%22}&extensions={%22persistedQuery%22:{%22version%22:1,%22sha256Hash%22:%225664b981ff921ec078e3df377fd4623faaa6cd0aa2178e8bdfcba9b41303848b%22}}"
        url=str(part1_url+part2_url+part3_url)
        insert(url,domainid,Status)
print("Inserted Completed")

cursor.execute("update pyscripts  set runstatus = 1 where domainid = 962 and processid = 1")
db.commit()

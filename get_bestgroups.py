#!/usr/bin/python

from datetime import datetime, date, timedelta
from pymongo.connection import Connection
from pymongo import DESCENDING

db = Connection().ga_groups

datefrom = date.today() - timedelta(days=2)

end_date = datefrom
end_date_mongo = datetime(end_date.year, end_date.month, end_date.day, 23,59,59)



total = db.charts.find_one({'groupslug': 'total', 'date': end_date_mongo})
totalratio = total['views_lastweek'] / total['views_prevweek']
print "totalratio: " + str(totalratio)
if totalratio <= 1:
    etalon = float(5)
else:
    etalon = (totalratio-1)*100 + 5
    
featured_after = end_date_mongo - timedelta(days=28)
    
groups = db.charts.find({'date': end_date_mongo}).sort('views_lastweek', DESCENDING).limit(250)
counter = 0
featured=list()
for group in groups:
    if counter > 25:
        break
    if group['ratio'] > etalon:
        if db.charts.find({'featured': True, 'date': {'$gt': featured_after}, 'groupslug': group['groupslug']}).count() == 0:
            db.charts.update({'groupslug': group['groupslug'], 'date': end_date_mongo}, {'$set': {'featured': True}})
            featured.append(group)
            counter += 1
            print "http://subscribe.ru/group/"+ str(group['groupslug'])+"/"
        else:
            continue
    else:
        continue

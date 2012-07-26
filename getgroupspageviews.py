#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2011 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
 Это приложение запрашивает статистику групп Сабскрайб в Гугланалитикс
 за прошедшую неделю. Формат запроса - дай первые 20000 страниц 
 отсортированных по количеству просмотров.
 Затем эти 20000 страниц группируются по группам, которым они 
 принадлежат (путем разбора URL). Затем та же процедура проводится с 
 предыдущей неделей (start_date_prev-end_date_prev).
 Результаты этих двух запросов запоминаюися и сравниваются погруппно.
 В результате получаем и параметр роста(падения) по сравнению с предыду-
 щей неделей. По этому параметру потом отбираем группы в чарт.
"""

__author__ = 'api.nickm@google.com (Nick Mihailovski)'


import sys
from datetime import datetime, date, timedelta
from urlparse import urlparse
from operator import itemgetter
from pymongo.connection import Connection
from pymongo import DESCENDING
import re
import gdata.analytics.client
import gdata.sample_util
import gdata.client
import auth

db = Connection().ga_groups

APP_NAME = 'Subscribe.Ru analytics bot'
TABLE_ID = 'ga:22656631'  # Insert your Table Id here.

datefrom = date.today() - timedelta(days=2)

end_date = datefrom
end_date_mongo = datetime(end_date.year, end_date.month, end_date.day, 23,59,59)

start_date = end_date - timedelta(days=7)

end_date_prev = start_date - timedelta(days=1)
start_date_prev = end_date_prev - timedelta(days=7)

def main():
  """Main method of this application."""
  my_client = gdata.analytics.client.AnalyticsClient(source=APP_NAME)
  my_auth_helper = auth.AuthRoutineUtil()

  # my_auth = auth.OAuthRoutine(my_client, my_auth_helper)

  # It's better to use OAuthHelper.
  my_auth = auth.ClientLoginRoutine(my_client, my_auth_helper)

  try:
    my_client.auth_token = my_auth.GetAuthToken()

  except auth.AuthError, error:
    print error.msg
    sys.exit(1)

  data_query = GetDataFeedQuery(TABLE_ID, start_date.isoformat(), end_date.isoformat())

  # If the token is invalid, a 401 status code is returned from the server and
  # a gdata.client.Unauthorized exception is raised by the client object.
  # For ClientLogin this happens after 14 days. For OAuth this happens if
  # the token is revoked through the Google Accounts admin web interface.
  # Either way, the token is invalid so we delete the token file on the
  # client. This allows the next iteration of the program to prompt the user
  # to acquire a new auth token.
  try:
    feed = my_client.GetDataFeed(data_query)

  except gdata.client.Unauthorized, error:
    print '%s\nDeleting token file.' % error
    my_auth_helper.DeleteAuthToken()
    sys.exit(1)

  dict_groups = FeedtoGroups(feed)

  data_query_prev = GetDataFeedQuery(TABLE_ID, start_date_prev.isoformat(), end_date_prev.isoformat())
  feed_prev = my_client.GetDataFeed(data_query_prev)
  dict_groups_prev = FeedtoGroups(feed_prev)
  
  for key in dict_groups.keys():
    if key in dict_groups_prev:
      if dict_groups_prev[key] != 0:
        r = dict_groups[key] / dict_groups_prev[key]
        if r >= 1:
          r = (dict_groups[key] / dict_groups_prev[key] - 1)*100
          record = {  'date': end_date_mongo, 
                'groupslug': key, 
                'views_lastweek': dict_groups[key], 
                'views_prevweek': dict_groups_prev[key],
                'ratio': r
              }
        elif r == 0:
          record = {  'date': end_date_mongo, 
                'groupslug': key, 
                'views_lastweek': dict_groups[key], 
                'views_prevweek': dict_groups_prev[key],
                'ratio': r
              } 
        else:
          r = -((dict_groups_prev[key] / dict_groups[key] - 1)*100)
          record = {  'date': end_date_mongo, 
                'groupslug': key, 
                'views_lastweek': dict_groups[key], 
                'views_prevweek': dict_groups_prev[key],
                'ratio': r
              }
      else:
        r=1000
        record = {  'date': end_date_mongo, 
                'groupslug': key, 
                'views_lastweek': dict_groups[key], 
                'views_prevweek': 0,
                'ratio': r
              }
    else:
      r = 1000
      record = {  'date': end_date_mongo, 
                'groupslug': key, 
                'views_lastweek': dict_groups[key], 
                'views_prevweek': 0,
                'ratio': r
              }
              
    db.charts.save(record)
    
  print str(len(dict_groups)) + " groups processed!\n"
  
  SelectBestGroups()
  
def GetDataFeedQuery(table_id, start_date, end_date):
  """Returns a Data Export API query object.

  The query specifies the top traffic sources by visits to the site.

  Args:
    table_id: string The table id from which to retrieve data.
        Format is ga:xxxxxx, where xxxxxx is the profile ID.
    start_date: string The beginning of the date range. Format YYYY-MM-DD.
    end_date: string The end og the date range. Format YYYY-MM-DD.

  Returns:
    A new gdata.analytics.client.DataFeedQuery object.
  """
  return gdata.analytics.client.DataFeedQuery({
      'ids': table_id,
      'start-date': start_date,
      'end-date': end_date,
      'dimensions': 'ga:pagePath',
      'metrics': 'ga:pageviews',
      'sort': '-ga:pageviews',
      'max-results': '50000'})


def FeedtoGroups(feed):
  """Outputs dictionary with { groupname: views) pairs
  """
  pattern = re.compile(r'^(?P<groupname>[a-z\-]+)/.*$')

  groups = dict()
  groupname = 'other'
  groups['other'] = float(0)
  groups['total'] = float(0)
  for entry in feed.entry:
    for dim in entry.dimension:
      dim.value = dim.value[7:]
      m = pattern.search(dim.value)
      if m:
        groupname = str(m.group('groupname'))
        if not groupname in groups.keys():
          groups[groupname] = float(0)
        else:
          break
      else:
        continue
      
    for met in entry.metric:
      value = float(met.value)
      groups[groupname] = groups[groupname] + value
      groups['total'] += value
  
  items = groups.items()
  items.sort(key = itemgetter(1), reverse=True)
  for i in items:
    print str(i) + "\n"
  print "Total: " + str(groups['total']) + " pageviews in " + str(len(groups)) + " groups."
  
  return groups

def SelectBestGroups():
    """ This will select best growing groups for the last week comparing to previous week
    """
    total = db.charts.find_one({'groupslug': 'total', 'date': end_date_mongo})
    totalratio = total['views_lastweek'] / total['views_prevweek']
    print "totalratio: " + str(totalratio)
    if totalratio <= 1:
        etalon = float(5)
    else:
        etalon = (totalratio-1)*100 + 15
    
    featured_after = end_date_mongo - timedelta(days=35)
    
    groups = db.charts.find({'date': end_date_mongo}).sort('views_lastweek', DESCENDING).limit(250)
    counter = 0
    featured=list()
    for group in groups:
        if counter > 25:
            break
        if group['ratio'] > etalon:
            if db.charts.find({'featured': True, 'date': {'$gt': featured_after}, 'groupslug': group['groupslug']}).count() == 0:
                db.charts.update(group, {'$set': {'featured': True}})
                featured.append(group)
                counter += 1
                print "http://subscribe.ru/group/"+ str(group['groupslug'])+"/"
            else:
                continue
        else:
            continue
            
    return featured

if __name__ == '__main__':
  main()

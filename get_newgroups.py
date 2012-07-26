#!/usr/bin/python

import urllib
import re

def main():
  """Get HTML by URL and extract pathnames"""
  
  f = urllib.urlopen("http://subscribe.ru/group/by-date/")
  f2 = urllib.urlopen("http://subscribe.ru/group/by-date/?page=2")
  f3 = urllib.urlopen("http://subscribe.ru/group/by-date/?page=3")
#  f4 = urllib.urlopen("http://subscribe.ru/group/by-date/?page=4")
  
  page=f.read() + f2.read() + f3.read() #+ f4.read()
  f.close()
  f2.close()
  f3.close()
#  f4.close()
  
  groupseek_pattern = re.compile(r'<h2><a href="/group(?P<group_name>.+)">.*</a></h2>')
#  key_value_pattern = re.compile(r'(?P<key>\w+)=\"(?P<value>[\S^=]+)\"', re.U)
  
  m = groupseek_pattern.search(page)
  while m:
    groupname = unicode(m.group('group_name'))
    print "http://subscribe.ru/group"+groupname  
    page = page[:m.start()]+page[m.end():]
    m = groupseek_pattern.search(page)

if __name__ == '__main__':
  main()

#!/usr/bin/python

import urllib
import re

def main():
  """Get HTML by URL and extract pathnames"""
  
  f = urllib.urlopen("http://subscribe.ru/group/")
  page=f.read()
  f.close()
  page_stripped = page[page.find('div class="konkurs"'):]
  groupseek_pattern = re.compile(r'<i>(?P<group_number>\d)</i>\n.*<a href="/group(?P<group_name>.+)">.*</a>')
  
  m = groupseek_pattern.search(page_stripped)
  while m:
    groupnumber = int(m.group('group_number'))
    if groupnumber <= 3:
      groupname = unicode(m.group('group_name'))
      print "http://subscribe.ru/group"+groupname  
    page_stripped = page_stripped[:m.start()]+page_stripped[m.end():]
    m = groupseek_pattern.search(page_stripped)

if __name__ == '__main__':
  main()

#!/usr/bin/env python2
#    Copyright (C) 2011 Pau Escrich <pau@dabax.net>
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License along
#    with this program; if not, write to the Free Software Foundation, Inc.,
#    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
#    The full GNU General Public License is included in this distribution in
#    the file called "COPYING".

import sqlite3
import sys
import socket
from BeautifulSoup import BeautifulSoup
import re
from urllib import FancyURLopener
import string
import threading
from Queue import Queue
import time

class SimpleBrowser(FancyURLopener):
    version = "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.6) Gecko/20070725 Firefox/2.0.0.6"

class crawler():
	def __init__(self,url,db):	
		self.url = url
		self.db = db
		print "|Crawling: " + url + '|'

	def _find_domain(self,domain):
		found = False
		sql = sqlite3.connect(self.db)
		c = sql.cursor()
		s = (domain,)
		c.execute('select * from domain where domain=?', s)
		if c.fetchone() != None: 
			found = True
		sql.close()
		return found

	def _insert_domain(self,domain):
		ip = self._get_ip(domain)
		if not ip: return False
	#	print "Inserting: %s -> %s" %(domain,ip)
		sql = sqlite3.connect(self.db)
		c = sql.cursor()
		i = (ip,)
		c.execute('insert into ip values (?)', i)
		i = (domain,ip)
		c.execute('insert into domain values (?,?)', i)
		sql.commit()
		sql.close()
		return True

#	def _find_url(self,url):

	def _get_ip(self,host):
		try:
			ip = socket.gethostbyname(host)
		except:
			print "Cannot resolve " + host
			ip = False

		return ip

	def _getUrl(self):
		web = SimpleBrowser()
		try:
			bs = BeautifulSoup(web.open(self.url).read())
		except:
			bs = False
		return bs

	def getLinks(self,force=False):
		other_domains = []
		doc = self._getUrl()
		if not doc: return []

		for link in doc.findAll('a',href=re.compile('^http://.*')):
			domain = link.get('href').split('/')[2]
			if not self._find_domain(domain):
				insd = self._insert_domain(domain)
				if insd: other_domains.append(domain)
		return other_domains

class crawthread(threading.Thread):
	def __init__(self,db,q,url=False):
		threading.Thread.__init__(self)
		self.db = db
		self.q = q
		self.url = url

	def run(self):
		if self.url:
			crw = crawler(self.url,self.db)
			other = crw.getLinks(True)
			for o in other: 
				self.q.put(o)

		while not self.q.empty():
			crw = crawler("http://"+self.q.get(),self.db)
			other = crw.getLinks()
			for o in other:
				self.q.put(o)
					

def new_database(file):
	sql = sqlite3.connect(file)
	c = sql.cursor()
	c.execute('''create table ip (ip text)''')
	c.execute('''create table domain (domain text, ip text, foreign key(ip) references ip(ip))''')
	c.execute('''create table visited (url text)''')
	sql.commit()
	sql.close()
	print "New database created at file " + file

def print_help():
	print "Domain Crawler"
	print "Usage: domaincrawler <option> [params]"
	print "Options:"
	print "		-n <filename> : Creates new database named filename"
	print "		-c <database> <URL> <#threads> : Starts crawling process from given URL using database"
	print "		-a <database> : List all domains stored in database"
	print "		-h : Shows this help"
	print ""
	print "|| by hakais@gmail.com, Happy crawling ||"
	sys.exit(1)

def crawl_url(db,url,t):
	q = Queue()
	print "## STARTING MAIN THREAD ##"
	crawthread(db,q,url).start()

	time.sleep(15)

	print "## STARTING THREADS ##"
	for i in range(0,t):
		crawthread(db,q).start()	

	while True:
		print "|----------------------------"
		print "| Active threads: " + str(threading.active_count())
		print "| Queue size: " + str(q.qsize())
		print "|----------------------------"
		
		for i in range(threading.active_count(),t):
			crawthread(db,q).start()
		
		time.sleep(5)
		
		

def list_all(db):
	sql = sqlite3.connect(db)
	c = sql.cursor()
	c.execute("select domain,ip from domain")
	for r in c:
		print "%s -> %s" %(r[0],r[1])
	sql.close()

def main():
	#No parameters
	if len(sys.argv) < 2:
		print_help()
	#New database
	if sys.argv[1] == "-n": 
		if len(sys.argv) < 3: print_help()
		new_database(sys.argv[2])
	#Start crawling
	elif sys.argv[1] == "-c":
		if len(sys.argv) < 5: print_help()
		crawl_url(sys.argv[2],sys.argv[3],int(sys.argv[4]))
	#List all domains stored in db
	elif sys.argv[1] == "-a":
		if len(sys.argv) < 3: print_help()
		list_all(sys.argv[2])
	#Print help
	else: print_help()

main()
	

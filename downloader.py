import sys, getopt
import sched
import time
import csv
from pprint import pprint
import urllib, urllib2
from random import randint
import threading
 
proxies_file = ""
targets_file = ""
 
proxies = []
targets = []
 
scheduler = sched.scheduler(time.time, time.sleep)
 
def pick_random_proxy():
	proxy_count = len(proxies) - 1;
	
	if proxy_count == 0:
		return None
		
	proxy_index = randint(0, proxy_count )
	proxy = proxies[ proxy_index ]
        
	return proxy[ "host" ] + ":" + proxy[ "port" ]
	
def download_file(interval, url):
	threading.Thread( target = lambda: download_file_impl(interval, url) )
	
	#randomize the interval
	new_interval = interval + randint( -1 * interval, interval )
	if new_interval < interval:
		new_interval = interval
		
	#repeat itself forever
	scheduler.enter(new_interval, 1 , download_file, (new_interval, url)  )
	print "==> Next download of " + url + " in " + str( new_interval ) + " seconds"
	
def download_file_impl(interval, url):
	selected_proxy = pick_random_proxy();
 
	download_was_ok = True
	try:
		request = urllib2.Request(url)
 
		if selected_proxy is None:
			print "NO PROXY!"
		else:
			request.set_proxy(selected_proxy, 'http')
		
		response = urllib2.urlopen(request)
		
		print "Response code: " + str( response.code )
		
		download_was_ok = response.code == 200
 
	except urllib2.URLError, e:
		download_was_ok = False
		pprint( e ) 
 
	
	if( download_was_ok ):
		print " OK! "
	else:
		print " NOK! "
			
 
 
def main(argv):
	global scheduler
	
	parse_command_line_parameters(argv)
 
	proxiesReader =  csv.DictReader(open(proxies_file), dialect='excel', delimiter=',')
	for row in proxiesReader:
		proxies.append( row )
			
	targetsReader =  csv.DictReader(open(targets_file), dialect='excel', delimiter=',')
	for row in targetsReader:
		targets.append( row )
			
	print "==============================================================================="
	print "Proxies file: " + proxies_file
	print "Targets file: " + targets_file
	print "-------------------------------------------------------------------------------"
	print "Proxies (total:" + str( len(proxies) ) + ")"
	pprint( proxies )
	print "Targets (total:" + str( len(targets) ) + ")"
	pprint( targets )
	print "==============================================================================="
	
	for target in targets:
		interval = int( target[ "interval" ] )
		url = target[ "url" ]
		scheduler.enter(interval, 1 , download_file, (interval, url )  )
	
	scheduler.run()
	
def parse_command_line_parameters(argv):
	global proxies_file
	global targets_file 
	try:
		opts, args = getopt.getopt(argv,"hp:t:",["proxies=","targets="])
	except getopt.GetoptError:
		print 'downloader.py -p <proxiesfile> -t <targetsfile>'
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print 'downloader.py -p <proxiesfile> -t <targetsfile>'
			sys.exit()
		elif opt in ("-p", "--proxiesfile"):
			proxies_file = arg
		elif opt in ("-t", "--targetsfile"):
			targets_file = arg
	
if __name__ == "__main__":
   main(sys.argv[1:])
import time
import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
from upstox_api.api import *
import multiprocessing
from threading import Thread

#upstox session create
s = Session ('unh8eRWaT69sxUnmHQJfZ9ZvgisQ2VVk6UARO3Ax')
s.set_redirect_uri ('http://127.0.0.1:3000')
s.set_api_secret ('qktu3vd7yd')
print (s.get_login_url())
try:
	u = Upstox ('unh8eRWaT69sxUnmHQJfZ9ZvgisQ2VVk6UARO3Ax', '19b5f7a17fbddea2d9722c46c41518e05fffe276')
except Exception as e:
	print e, type(e)

#mysql connection start
try:	
	config = {
	  'user': 'root',
	  'password': '',
	  'host': '127.0.0.1',
	  'database': 'tick_db',
	  'raise_on_warnings': True,
	}
	cnx = mysql.connector.connect(**config)
	cursor = cnx.cursor()
	stmt = "SHOW TABLES LIKE 'upstox_broker_info'"
	cursor.execute(stmt)
	result = cursor.fetchone()
	if result:
		print "upstox_broker_info table is exist"
	else:
	    cursor.execute(
             """DROP TABLE IF EXISTS `upstox_broker_info`;
				CREATE TABLE `upstox_broker_info` (
				  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
				  `exchange` varchar(255) NOT NULL,
				  `log_time` datetime(6) NOT NULL,
				  `symbol` varchar(255) NOT NULL,
				  `ltp` decimal(10,2) NOT NULL,
				  `vtt` decimal(15,2) NOT NULL,
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8;""")
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)

#mysql connection end
#event handler to log live feed

global q 
q = multiprocessing.Queue()
def event_handler_quote_update(message):
	today = datetime.now()
	#log view
	print "Symbol : ", message['symbol'] , " log_time:" , today, " exchange:" , message['exchange'], " ltp:" , message['ltp'], " vtt:" , message['vtt']
	add_information = "INSERT INTO upstox_broker_info SET exchange = '"+message['exchange']+"', log_time = '"+str(today)+"', symbol = '"+str(message['symbol'])+"', ltp = "+str(message['ltp'])+", vtt = "+str(message['vtt'])
	# Insert new log
	q.put((message['exchange'], today, message['symbol'], message['ltp'], message['vtt']))
	cursor.execute(add_information)
	cnx.commit()
	
u.set_on_quote_update(event_handler_quote_update)
u.get_master_contract('NSE_EQ') # get contracts for NSE EQ
u.get_master_contract('NSE_FO') # get contracts for NSE FO
u.get_master_contract('NSE_INDEX') # get contracts for NSE INDEX
u.get_master_contract('BSE_EQ') # get contracts for BSE EQ
u.get_master_contract('BCD_FO') # get contracts for BCD FO
u.get_master_contract('BSE_INDEX') # get contracts for BSE INDEX
u.get_master_contract('MCX_INDEX') # get contracts for MCX INDEX
u.get_master_contract('MCX_FO') # get contracts for MCX FO
while True:
	try:
		u.subscribe(u.get_instrument_by_token('BSE_EQ', 950237), LiveFeedType.Full)
		u.start_websocket(True)
		time.sleep(1)
	except:
		print "Websocket error"
	finally:
		time.sleep(1)
#mysql disconnect
cursor.close()
cnx.close()

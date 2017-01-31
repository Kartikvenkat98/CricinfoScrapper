ffrom mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config
import requests
from bs4 import BeautifulSoup

def isValid(val):

	#print "entering valid, val",val
	#print "string val",val.get_text()
  
	if val is None:
		# print "val is none"
		return 0
  
	if val.string is None:
		# print "val string is none"
		return 0

	if val.get_text() == '-':
		return 0
	# print "finalstring: ", val.get_text().encode('utf-8');
	return val.get_text().encode('utf-8');

def scrape_table(soup):
	
	table = soup.select("table.engineTable td.left > b")
	
	for i in range(len(table)):
		if "Twenty20" in repr(table[i]):
			#print i + 1
			break;
	rownum = i + 1
	return scrape_player(soup, rownum)
	
def scrape_player(soup, rownum):
	
	player={}
	playerBatting={}
	playerBowling={}

	playerData=soup.find_all('td',attrs={"title":"record rank: "+ str(rownum)})
	matchDetails=soup.find('td',attrs={"title":"record rank: "+ str(rownum)})

	if matchDetails is not None:
		playerShortName = soup.find('div',class_="ciPlayernametxt")
			# player['name'] = isValid(playerInfo.span)
			#print playerShortName.h1.string;
		player['name'] = playerShortName.h1.get_text().encode('ascii', 'ignore')
		#print type(player['name'])
		player['country'] = soup.find("h3",class_="PlayersSearchLink").b.string.extract().replace(u'\u2020',' ').encode('utf-8')
		playerInfo=soup.find('p',class_="ciPlayerinformationtxt")
		playerInfo=playerInfo.find_next_siblings("p")
		t3 = playerInfo[3].b.string.extract().replace(u'\u2020',' ').encode('utf-8')
		t4 = playerInfo[4].b.string.extract().replace(u'\u2020',' ').encode('utf-8')

		if t3 == 'Playing role':
			player['style'] = isValid(playerInfo[3].span)
		elif t4 == 'Playing role':
			player['style'] = isValid(playerInfo[4].span)
		elif t3 == 'Batting style':
			player['style'] = isValid(playerInfo[3].span)
		elif t4 == 'Batting style':	
			player['style'] = isValid(playerInfo[4].span)
		
		if player['style'].find('rounder')!=-1:
			player['playerType']='Allrounder'
		elif player['style'].find('keeper')!=-1:
			player['playerType']='Wicketkeeper'
		elif player['style'].find('bat')!=-1:
			player['playerType']='Batsman'
		else:
			player['playerType']='Bowler'
				
		#print player['playerType']

		insertPlayer(player)

		
		matchDetails=matchDetails.find_next_siblings("td")

		playerBatting['id']=401
		playerBatting['matches']=int(isValid(matchDetails[0]))
		playerBatting['inns']=int(isValid(matchDetails[1]))
		playerBatting['not_outs']=int(isValid(matchDetails[2]))
		playerBatting['runs']=int(isValid(matchDetails[3]))
		high=str(isValid(matchDetails[4]))
		playerBatting['high_score']=high
		playerBatting['avg']=float(isValid(matchDetails[5]))
		playerBatting['balls']=int(isValid(matchDetails[6]))
		playerBatting['strike_rate']=float(isValid(matchDetails[7]))
		playerBatting['100s']=int(isValid(matchDetails[8]))
		playerBatting['50s']=int(isValid(matchDetails[9]))
		playerBatting['4s']=int(isValid(matchDetails[10]))
		playerBatting['6s']=int(isValid(matchDetails[11]))
		playerBatting['catches']=int(isValid(matchDetails[12]))
		playerBatting['stumpings']=int(isValid(matchDetails[13]))

		insertBatting(playerBatting)


		playerData=playerData[1].find_next_siblings("td")
		if  playerData is not None :
			playerBowling['id']=playerBatting['id']
			playerBowling['matches']=playerBatting['matches']
			playerBowling['inns']=int(isValid(playerData[1]))
			playerBowling['balls']=int(isValid(playerData[2]))
			playerBowling['runs']=int(isValid(playerData[3]))
			playerBowling['wickets']=int(isValid(playerData[4]))
			playerBowling['bbi']=isValid(playerData[5])
			playerBowling['bbm']=isValid(playerData[6])
			playerBowling['avg']=float(isValid(playerData[7]))
			playerBowling['economy']=float(isValid(playerData[8]))
			playerBowling['strike_rate']=float(isValid(playerData[9]))
			playerBowling['four_wickets']=int(isValid(playerData[10]))
			playerBowling['five_wickets']=int(isValid(playerData[11]))
			playerBowling['ten_wickets']=int(isValid(playerData[12]))

		insertBowling(playerBowling)
		
		imagediv = soup.find('div', class_="pnl490M")

		img = imagediv.find_all('img')
		
		try:
			imgPath="http://www.espncricinfo.com"+img[0].get('src').split('html')[0]+'jpg'
		except:
			imgPath="default"
		
		print imgPath
		img_dload = requests.get(imgPath, stream=True)
		player['imgpath']=str(playerBatting['id'])+".jpg"
		#player['imgpath']="4451.png"
		
		with open("player_images/" + player['imgpath'], 'wb') as fp:
			for chunk in img_dload.iter_content(chunk_size=34):
				fp.write(chunk)
		

		print player['imgpath']


def insertPlayer(data):
	#query = 'INSERT INTO player_details (player_name, country, type) VALUES ({:s}, {:s}, {:s}, {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:d});' 

	query = "INSERT INTO player_details (player_name, country, type) VALUES ('{}', '{}', '{}')".format(data['name'], data['country'], data['playerType']) 
	#qf = query.format(*t)
	#print qf
	db_config = read_db_config()
	conn = MySQLConnection(**db_config)
	cursor = conn.cursor()
	cursor.execute(query)
	conn.commit()
	#pid = cursor.lastrowid
	#print pid
	#return pid
	cursor.close()
	

def insertBatting(data):
	query = "INSERT INTO player_batting_details (player_id, matches, inns, not_outs,runs, high_score, avg, balls, strike_rate, hundreds, fifties, fours, sixes,catches, stumpings) VALUES ({},{},{},{},{},'{}',{},{},{},{},{},{},{},{},{})".format(data['id'],data['matches'],data['inns'], data['not_outs'], data['runs'], data['high_score'], data['avg'], data['balls'],data['strike_rate'],data['100s'],data['50s'],data['4s'],data['6s'],data['catches'],data['stumpings'])

	db_config = read_db_config()
	conn = MySQLConnection(**db_config)
	cursor = conn.cursor()
	cursor.execute(query)
	conn.commit()
	cursor.close()

	
def insertBowling(data):
	query = 'INSERT INTO player_bowling_details (player_id, matches, inns, balls,runs, wickets_taken, bbi, bbm, avg, economy, strike_rate, four_wickets, five_wickets, ten_wickets) VALUES ({}, {}, {}, {}, {}, {}, "{}", "{}", {}, {}, {}, {}, {}, {})'.format(data['id'], data['matches'], data['inns'], data['balls'], data['runs'], data['wickets'], data['bbi'], data['bbm'], data['avg'], data['economy'], data['strike_rate'], data['four_wickets'], data['five_wickets'], data['ten_wickets'])

	db_config = read_db_config()
	conn = MySQLConnection(**db_config)
	cursor = conn.cursor()
	cursor.execute(query)
	conn.commit()
	cursor.close()


def main():
	r = requests.get('http://www.espncricinfo.com/ci/content/player/625371.html')
	soup = BeautifulSoup(r.text, 'html.parser')
	t = scrape_table(soup)
	#print t
	
	

if __name__ == "__main__":
	main()

import urllib.request, json
import numpy as np
import pandas as pd
from datetime import date

personsTable = pd.read_csv('persons.csv')
matchesTable = pd.read_csv('matches.csv')
resultsTable = pd.read_csv('results.csv')

def steamID3toSteamID(steamID3):
	parts = steamID3.split(':')
	accountID = int(parts[-1].rstrip(']'))
	if (accountID % 2 == 0):
		x = 0
		y = accountID / 2
	else:
		x = 1
		y = ((accountID - 1) / 2)
	steamID = (y * 2) + x + 7960265728
	return int(steamID) # returns part of the steamID that follows '7656119'

# read in list of logs.tf IDs
listFile = open('logs.txt','r')
numlogs = 0

# for each ID, get json file
for line in listFile:
	logtfIdInt = int(line)

	if logtfIdInt not in matchesTable['logsID'].values: # check if record already exists in matchesTable
		print('Getting data from logs.tf/' + str(logtfIdInt) + '...')

		# load json file from URL
		logUrl = 'http://logs.tf/json/' + line
		url = urllib.request.urlopen(logUrl)
		obj = json.load(url)

		numlogs = numlogs + 1

		# extract match info
		timestamp = date.fromtimestamp(obj['info']['date'])
		readableDate = timestamp.strftime('%Y-%m-%d')
		mapName = obj['info']['map']
		title = obj['info']['title']
		totalLength = obj['info']['total_length']
		bluScore = obj['teams']['Blue']['score']
		redScore = obj['teams']['Red']['score']

		# get region info from title
		if ('na.serveme.tf' in title):
			region = 'NAm'
		elif ('sea.serveme.tf' in title):
			region = 'SEA'
		elif ('serveme.tf' in title):
			region = 'EU'
		else:
			region = 'unknown'

		
		# extract player info (steamid3, steamid, steam name, total players)
		playerDict = obj['names']
		numPlayers = len(playerDict)

		# write data to matchesTable
		newRow = {'logsID' : logtfIdInt, 'region' : region, 'date' : readableDate, 'map' : mapName, 'length' : totalLength, 'numplayers' : numPlayers, 'blu_score' : bluScore, 'red_score' : redScore}
		matchesTable = matchesTable.append(newRow, ignore_index=True)


		for steamID3, steamName in playerDict.items():
			steamID = steamID3toSteamID(steamID3)
			# if player already exists in personsTable, update steam name, else write new record to personsTable
			if steamID in personsTable['steamID'].values:
				personsTable.loc[(personsTable['steamID'] == steamID),'steamName'] = steamName
			else:
				newRow = {'steamID' : steamID, 'steamID3' : steamID3, 'steamName' : steamName}
				personsTable = personsTable.append(newRow, ignore_index=True)

		# extract stats for each player (team, class, time, damage, k/a/d, med_healing, med_charges, med_drops BS, HS)
		playerStats = obj['players']
		for steamID3, stats in playerStats.items():
			steamID = steamID3toSteamID(steamID3)
			steamName = personsTable.loc[personsTable['steamID'] == steamID,'steamName'].item()
			team = stats['team']		

			for classStats in stats['class_stats']:
				className = classStats['type']
				classTime = classStats['total_time']
				kills = classStats['kills']
				assists = classStats['assists']
				deaths = classStats['deaths']
				damage = classStats['dmg']
				if className == 'sniper':
					hs = stats['headshots']
				else:
					hs = 0
				if className == 'spy':
					bs = stats['backstabs']
				else:
					bs = 0
				if className == 'medic':
					healing = stats['heal']
					charges = stats['ubers']
					drops = stats['drops']
				else:
					healing = 0
					charges = 0
					drops = 0
				newRow = {'logsID' : logtfIdInt, 'steamID' : steamID, 'steamName' : steamName, 'date' : readableDate, 'team' : team, 'class' : className, 'total_time' : classTime, 'kills' : kills, 'assists' : assists, 'deaths' : deaths, 'damage' : damage, 'BS' : bs, 'HS' : hs, 'med_healing' : healing, 'med_charges' : charges, 'med_drops' : drops}
				resultsTable = resultsTable.append(newRow, ignore_index=True)
				
	else: print('logs.tf/' + str(logtfIdInt) + ' already exists in database, skipping...')

matchesTable = matchesTable.sort_values(['date', 'logsID'], ascending=[True, True])
resultsTable = resultsTable.sort_values(['date', 'logsID'], ascending=[True, True])

print('Added ' + str(numlogs) + ' match log(s) to database. Writing to CSV...')

matchesTable.to_csv('matches.csv', index=False)
personsTable.to_csv('persons.csv', index=False)
resultsTable.to_csv('results.csv', index=False)
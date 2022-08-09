#TO DO 
# Look into using AWS EC2

from bs4 import BeautifulSoup
import sys
import requests
import csv
import pandas as pd
import shutil
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import logging
from io import StringIO
import smtplib


args = [0,0,0,0]

#Recieving input from user about what league/team/year to start the scraping on
for arg in sys.argv:
    args.append(arg)
league_offset = int(args[1])
year_offset = int(args[2])
team_offset = int(args[3])

first_time = True
#Read in base league URLS
urlDf = pd.read_excel("TM_Urls.xlsx",engine='openpyxl')

valid_years =  ['2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']
# player_dict := Key: player_name + dob    Value: {pob, agent, stats}
# stats = {year: [matchday, date, home, away, result, pos, goals, assists, own goals, yellow, yellow2, red, sub in, sub off, mins]}
player_dict = {}
urlList = []

#League URLS that are weird and don't work for scraping
bad_urls = ['https://www.transfermarkt.co.uk/portugal-championship-final-phase/startseite/wettbewerb/P2RL', 'https://www.transfermarkt.co.uk/liguilla-apertura/startseite/wettbewerb/POMX', 'https://www.transfermarkt.co.uk/liga-nacional-apertura-play-off/startseite/wettbewerb/GUPA', 'https://www.transfermarkt.co.uk/liga-1-championship-group/startseite/wettbewerb/RO1C']

#Basic user agent headers so they don't get suspicious why we are making a bunch of requests
headers1 = {'User-Agent':
                   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
default_url = "https://img.a.transfermarkt.technology/portrait/small/default.jpg?lm=1"
tiers = {'First Tier': 1, 'Second Tier': 2, 'Third Tier': 3, 'Fourth Tier': 4, 'Fifth Tier': 5, 'Sixth Tier': 6, 'Seventh Tier':7}

#Function that sends an email to the specified email stored in send_to_email from the email pythonnotifmail@gmail.com
#idx1, idx2, idx3 are just ids that tell me at what league/team/year the program crashed
'''def send_email_crash_notification(crash_message, idx1, idx2, idx3):
    email = 'pythonnotifmail@gmail.com'
    send_to_email = 'mjwellma@umich.edu'
    subject = 'Python application CRASHED!'
    msg = MIMEMultipart()
    msg['From'] = email
    msg['To'] = send_to_email
    msg['Subject'] = subject
    message = crash_message + "\n\n At idx combo " + str(idx1) + " " + str(idx2) + " " + str(idx3)
    msg.attach(MIMEText(message, 'plain'))
    # Send the message via SMTP server.
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login('jr.wellman@comcast.net', 'Patriots.0211')
    text = msg.as_string()
    server.sendmail(email, send_to_email, text)
    server.quit()
    print('email sent to ' + str(send_to_email))
    return True'''

#Function that prints out the progress of the scraper when it finishes scraping a year for a team
def print_status(last_team, last_year):
    if last_team and last_year:
        print("Completed a team with offsets:", league_offset, "Last Year", "Last Team")
    elif last_team:
        print("Completed a team with offsets:", league_offset, year_offset, "Last Team")
    elif last_year:
        print("Completed a team with offsets:", league_offset, "Last Year", team_offset)
    else:
        print("Completed a team with offsets:", league_offset, year_offset, team_offset)

#Formats money values in a way that is good
#Ex. 3Th. get changed to 3000
def format_value(val):
    out = ""
    if "Th." in val:
        out = str(float(val[1:-3]) *1000)
    else:
        out = str(float(val[1:-1]) * 1000000)
    return out
   

#Parses a html table object to get the revelant stats for a player
def parse_player_table(tds, player_key, all_box, year, league_abr, game):
    print("Parsing player table of " + player_name)
    record_year = str(year)
    ## remove space in front of the year
    ## if a name of the header is the same as the url name (ie GB1) then use this table)
    correct_box = ""
    i = 0
    for box in all_box.find_all("div", {"class": "box"})[1:]:
        if box.find("a") and box.find("a")["name"] == league_abr:
            correct_box = box
    if correct_box == "":
        print("Bad happening 1 with " + player_name)
        return game
    if record_year in valid_years:
        if record_year not in player_dict[player_key]["stats"]:
            game = ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '']


    player_dict[player_key]["stats"] = {}
    p_table = correct_box.find("div", {"class": "responsive-table"})
    if p_table and p_table.find("tbody"):
        p_terms = p_table.find("tbody").find_all("tr")
        p_ct = len(p_terms)
        for i in range(p_ct):
            if i < p_ct:
                tds = p_terms[i].find_all("td")
                if len(tds) == 17:
                    if tds[0].text != "-":
                        game[0] = str(tds[0].text[:-1].replace("\\", "").replace("n", ""))
                        game[0] = game[0][1:]
                    if tds[1].text != "":
                        game[1] = str(tds[1].text)
                    if tds[3].text != "-":
                        game[2] = str(tds[3].find("a")['title'])
                    if tds[5].text != "-":
                        game[3] = str(tds[5].find("a")['title'])
                    if tds[6].text != "-":
                        game[4] = str(tds[6].text)
                    if tds[7].text != "-":
                        game[5] = str(tds[7].text)
                    if tds[8].text != "":
                        game[6] = str(tds[8].text)
                    else:
                        game[6] = str("0")
                    if tds[9].text != "":
                        game[7] = str(tds[9].text)
                    else:
                        game[7] = str("0")
                    if tds[10].text != "":
                        game[8] = str(tds[10].text)
                    else:
                        game[8] = str("0")
                    if tds[11].text != "":
                        game[9] = str(tds[11].text)
                    else:
                        game[9] = str("X")
                    if tds[12].text != "":
                        game[10] = str(tds[12].text)
                    else:
                        game[10] = str("X")
                    if tds[13].text != "":
                        game[11] = str(tds[13].text)
                    else:
                        game[11] = str("X")
                    if tds[14].text != "":
                        game[12] = str(tds[14].text)
                    if tds[15].text != "":
                        game[13] = str(tds[15].text)
                    if tds[16].text != "":
                        game[14] = str(tds[16].text)
                    game.append(game[:])
                if len(tds) == 8: 
                    if tds[0].text != "-":
                        game[0] = str(tds[0].text[:-1].replace("\\", "").replace("n", ""))
                        game[0] = game[0][1:]
                    if tds[1].text != "-":
                        game[1] = str(tds[1].text)
                    if tds[3].text != "-":
                        game[2] = str(tds[3].find("a")['title'])
                    if tds[5].text != "-":
                        game[3] = str(tds[5].find("a")['title'])
                    if tds[6].text != "-":
                        game[4] = str(tds[6].text)
                    game[5] = ''
                    game[6] = str("Did not play")
                    game[7] = ''
                    game[8] = ''
                    game[9] = ''
                    game[10] = ''
                    game[11] = ''
                    game[12] = ''
                    game[13] = ''
                    game[14] = ''
                    game.append(game[:])
                else:
                    continue

    return game 
                  


def add_to_player_dict(url, player_name, pid, year, league_abr, game):
    player_key = pid
    player_dict[player_key] = {}
    player_dict[player_key]["stats"] = {}
    split = url.split("/")
    url = "https://www.transferMarkt.co.uk/" + split[1] + "/leistungsdatendetails/spieler/" + split[4] + "/saison/" + str(year) + "/verein/0/liga/0/wettbewerb//pos/0/trainer_id/0/plus/1"
    playerTree = requests.get(url, headers=headers1)
    playerSoup = BeautifulSoup(playerTree.content, 'html.parser')
    playerSoup = playerSoup.find("div", {"id": "main"})
    if playerSoup == None:
        print("Bad player page", player_name, pid)
        return
    twelves = playerSoup.find_all("div", {"class": "large-12 columns"})
    if len(twelves) < 3:
        return
    all_box = twelves[1]
    year = str(year)
    game = parse_player_table(tds, player_key, all_box, year, league_abr, game)
    return game

try:
    for i in range(0,346):
        if urlDf['URL'][i] not in bad_urls:
            urlList.append(urlDf['URL'][i])
    print("Starting Up")
    for leagueidx, leagueurl in enumerate(urlList):

        print("Starting league", leagueurl)
        #if first_time and leagueidx != league_offset:
            #continue
        league_offset = leagueidx
        league_abr = leagueurl.split('/')[6]
        print(league_abr)
        for year in range(2004, 2021):
            if first_time and year - 2004 != year_offset:
                continue
            year_offset = year - 2004
            page1 = leagueurl + "/plus/?saison_id=" + str(year)
            pageTree1 = requests.get(page1, headers=headers1)
            pageSoup1 = BeautifulSoup(pageTree1.content, 'html.parser')
            table = pageSoup1.find_all("div",{"class": "box"})[2]
            if table is None:
                print("Somethin bad 1", leagueurl, year)
                continue
            year_check = table.find("h2")
            if (year_check is None or year_check.text.split('/')[0].split(" ")[-1] != str(year)[2:]) and not (year_check.text.strip().split(" ")[-1] == str(year+1)):
                print("Somethin bad 2", leagueurl, year)
                continue
            team_table = table.find("table",{"class":"items"})
            header = pageSoup1.find("div",{"class":"box-content"})
            if header is None:
                print("Somethin bad 3", leagueurl, year)
                continue
            header_table = header.find("table", {"class": "profilheader"})
            if header_table is None:
                print("Somethin bad 4", leagueurl, year)
                continue

            leagueName = pageSoup1.find("div", {"class": "box-header"}).find("h1").text
            if team_table is None or team_table.find("tbody") is None:
                print("No team table", leagueurl, year)
                continue
            teamUrls = []
            teamNames = []
            skip_ctr = 0
            teams = table.find("tbody").find_all("tr")
            for i in teams:
                link = i.find_all("td")[1]
                text = link.a.text
                teamNames.append(text)
                link = link.a.get("href")
                link = link.replace("startseite", "kader")
                teamUrls.append("https://www.transferMarkt.co.uk" + link + "/plus/1")
            for teamidx, teamUrl in enumerate(teamUrls):
                if first_time and team_offset != teamidx:
                    continue
                first_time = False
                team_offset = teamidx
                # From club page
                pids = []
                pnames = []
                positions = []

                # From player table
                matchday = []
                date = []
                home = []
                away = []
                goals = []
                assists = []
                ogs = []
                yellow = []
                yellow2 = []
                red = []
                subon = []
                suboff = []
                mins = []

                teamName = teamNames[teamidx]
                teamTree1 = requests.get(teamUrl, headers=headers1)
                teamSoup1 = BeautifulSoup(teamTree1.content, 'html.parser')
                tableMini = teamSoup1.find("table",{"class":"items"})
                if tableMini is not None and tableMini.find("tbody") is not None:
                    odd = tableMini.find("tbody").find_all("tr",{"class":"odd"})
                    even = tableMini.find("tbody").find_all("tr",{"class":"even"})
                    ct = max(len(odd), len(even))

                    for i in range(ct):
                        if i < len(odd):
                            data = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
                            game = []
                            player_name = ""
                            pos = ""
                            tds = odd[i].find_all("td")

                            player_name = odd[i].find_all("tr")[0].find_all("td")[1].text.strip()
                            player_url = odd[i].find("td", {"class": "hauptlink"}).find("a")['href']
                            player_id = player_url.split("/")[-1]
                            pos = tds[4].text
                            
                            if player_id not in player_dict:
                                game = add_to_player_dict(player_url, player_name, player_id, year, league_abr, game)

                            if str(year) in player_dict[player_id]['stats']:
                                data = player_dict[player_id]['stats'][str(year)]
                            for data in game:
                                pids.append(player_id)
                                pnames.append(player_name)
                
                        
                        '''val_td = even[i].find("td", {"class": "rechts hauptlink"})
                        if val_td.text.strip() != "" and val_td.text.strip() != "-":
                            val = format_value(val_td.text.strip())'''
                        
                    
                        field_names = ["PID", "Player Name", "Team Name", "League Name", "Year", "Position", 
                        "Matchday", "Date", "Home Team", "Away Team", "Result", "Goals", "Assists", "Own Goals", "Yellow Cards", "Second Yellow", "Red Cards", "Subbed on", "Subbed off", "Minutes Played"]
                        with open("game_data_fall_2021_brit.csv", 'a') as out_file:
                            writer = csv.DictWriter(out_file, fieldnames=field_names)
                            i = 15
                            for pidx in range(len(game)):
                                if i < len(game):
                                    out_dict = {"PID": player_id, "Player Name": player_name, "Team Name": teamName, "League Name": leagueName, "Year": year, "Position": game[i][5],
                                    "Matchday": game[i][0], "Date": game[i][1], "Home Team": game[i][2], "Away Team": game[i][3], "Result": game[i][4], "Goals": game[i][6], "Assists": game[i][7], "Own Goals": game[i][8], "Yellow Cards": game[i][9], "Second Yellow": game[i][10], "Red Cards": game[i][11], "Subbed on": game[i][12], "Subbed off": game[i][13], "Minutes Played": game[i][14]}
                                    i += 1
                                    
                                    writer.writerow(out_dict)
                        print("Done with " + player_name)
                    last_team = (teamidx == len(teamUrls) - 1)
                    last_year = (year == 2020)
                    print_status(last_team, last_year)

except Exception as e:
    print(e)
    log_stream = StringIO()
    logging.basicConfig(stream=log_stream, level=logging.INFO)
    logging.error("Exception occurred", exc_info=True)
    ## send_email_crash_notification(log_stream.getvalue(), league_offset, year_offset , team_offset)
    print(log_stream.getvalue())



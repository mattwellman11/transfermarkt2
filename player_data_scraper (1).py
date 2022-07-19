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


args = []

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
# stats = {year: [sappear, mappear, ppg, goals, assists, ogs, subon, suboff, yellow, yellow2, red, pens, mins]}
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
def send_email_crash_notification(crash_message, idx1, idx2, idx3):
    email = 'pythonnotifmail@gmail.com'
    send_to_email = 'elliotmo@umich.edu'
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
    server.login('pythonnotifmail@gmail.com', 'pythonnotifpassword123')
    text = msg.as_string()
    server.sendmail(email, send_to_email, text)
    server.quit()
    print('email sent to ' + str(send_to_email))
    return True

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

#Generates filename for player picture to be downloaded
def generate_file_name(player_id):
    return player_id + ".jpg"
   

#Parses a html table object to get the revelant stats for a player
def parse_player_table(tds, player_key):
    record_year = "20" + tds[0].text.split("/")[0]
    if record_year in valid_years:
        if record_year not in player_dict[player_key]["stats"]:
            player_dict[player_key]["stats"][record_year] = [0,0,0,0,0,0,0,0,0,0,0,0,0]

        if len(tds) == 17:
            #Goalkeeper
            if tds[4].text != "-":
                player_dict[player_key]["stats"][record_year][0] += int(tds[4].text)
            if tds[5].text != "-" and tds[6].text != "-":
                player_dict[player_key]["stats"][record_year][2] = round((player_dict[player_key]["stats"][record_year][2] * player_dict[player_key]["stats"][record_year][1] + int(tds[5].text) * float(tds[6].text.replace(",", "."))) / (player_dict[player_key]["stats"][record_year][1] + int(tds[5].text)), 2)
            if tds[5].text != "-":
                player_dict[player_key]["stats"][record_year][1] += int(tds[5].text)
            if tds[7].text != "-":
                player_dict[player_key]["stats"][record_year][3] += int(tds[7].text)
            if tds[8].text != "-":
                player_dict[player_key]["stats"][record_year][5] += int(tds[8].text)
            if tds[9].text != "-":
                player_dict[player_key]["stats"][record_year][6] += int(tds[9].text)
            if tds[10].text != "-":
                player_dict[player_key]["stats"][record_year][7] += int(tds[10].text)
            if tds[11].text != "-":
                player_dict[player_key]["stats"][record_year][8] += int(tds[11].text)
            if tds[12].text != "-":
                player_dict[player_key]["stats"][record_year][9] += int(tds[12].text)
            if tds[13].text != "-":
                player_dict[player_key]["stats"][record_year][10] += int(tds[13].text)
            if tds[16].text != "-":
                player_dict[player_key]["stats"][record_year][12] += int(tds[16].text[:-1].replace(".", ""))
        else:
            if tds[4].text != "-":
                player_dict[player_key]["stats"][record_year][0] += int(tds[4].text)
            if tds[5].text != "-" and tds[6].text != "-":
                player_dict[player_key]["stats"][record_year][2] = round((player_dict[player_key]["stats"][record_year][2] * player_dict[player_key]["stats"][record_year][1] + int(tds[5].text) * float(tds[6].text.replace(",", "."))) / (player_dict[player_key]["stats"][record_year][1] + int(tds[5].text)), 2)
            if tds[5].text != "-":
                player_dict[player_key]["stats"][record_year][1] += int(tds[5].text)
            if tds[7].text != "-":
                player_dict[player_key]["stats"][record_year][3] += int(tds[7].text)
            if tds[8].text != "-":
                player_dict[player_key]["stats"][record_year][4] += int(tds[8].text)
            if tds[9].text != "-":
                player_dict[player_key]["stats"][record_year][5] += int(tds[9].text)
            if tds[10].text != "-":
                player_dict[player_key]["stats"][record_year][6] += int(tds[10].text)
            if tds[11].text != "-":
                player_dict[player_key]["stats"][record_year][7] += int(tds[11].text)
            if tds[12].text != "-":
                player_dict[player_key]["stats"][record_year][8] += int(tds[12].text)
            if tds[13].text != "-":
                player_dict[player_key]["stats"][record_year][9] += int(tds[13].text)
            if tds[14].text != "-":
                player_dict[player_key]["stats"][record_year][10] += int(tds[14].text)
            if tds[15].text != "-":
                player_dict[player_key]["stats"][record_year][11] += int(tds[15].text)
            if tds[17].text != "-":
                player_dict[player_key]["stats"][record_year][12] += int(tds[17].text[:-1].replace(".", ""))

def add_to_player_dict(url, player_name, birth, pid):
    player_key = pid
    player_dict[player_key] = {}
    player_dict[player_key]["pob"] = ""
    player_dict[player_key]["agent"] = ""
    player_dict[player_key]["stats"] = {}
    split = url.split("/")
    url = "https://www.transferMarkt.co.uk/" + split[1] + "/leistungsdatendetails/spieler/" + split[4] + "/plus/1"
    playerTree = requests.get(url, headers=headers1)
    playerSoup = BeautifulSoup(playerTree.content, 'html.parser')
    playerSoup = playerSoup.find("div", {"id": "main"})
    if playerSoup == None:
        print("Bad player page", player_name, pid)
        return
    agent = ''
    pob = ''
    twelves = playerSoup.find_all("div", {"class": "large-12 columns"})
    if len(twelves) < 3:
        return
    first_box = twelves[2]

    img = first_box.find("img", {"title": player_name})
    if img is not None:
        img = img.get("src")
    else:
        checker = first_box.find("div", {"class": "dataBild"})
        if checker is not None:
            img = checker.find("img")
            if img:
                img = img.get("src")
    

    if img is not None and img != "https://img.a.transfermarkt.technology/portrait/header/default.jpg?lm=1":
        r = requests.get(img, stream=True)
        save_name = generate_file_name(pid)
        if r.status_code == 200:
            with open("brit_images/"+save_name, 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)


    data_bottom = first_box.find("div", {"class": "dataBottom"})
    ps = data_bottom.find_all("p")

    for p in ps:
        if agent != "" and pob != "":
            break
        span_item = p.find("span", {"itemprop": "birthPlace"})
        if span_item:
            town = span_item.text
            countryimg = p.find("img")
            if countryimg is not None:
                town += " " + countryimg.get("alt")
            pob = town
            continue
        if p.find("span", {"class": "dataItem"}) and p.find("span", {"class": "dataItem"}).text == "Agent:":
            agent = p.find("span", {"class": "dataValue"}).text
            continue
    

    player_dict[player_key]["pob"] = pob.strip().replace(",", "")
    player_dict[player_key]["agent"] = agent.strip().replace(",", "")
    player_dict[player_key]["stats"] = {}
    p_table = playerSoup.find("div", {"class": "responsive-table"})
    if p_table and p_table.find("tbody"):
        p_odd = p_table.find("tbody").find_all("tr",{"class":"odd"})
        p_even = p_table.find("tbody").find_all("tr",{"class":"even"})
        p_ct = max(len(p_odd), len(p_even))
        for i in range(p_ct):
            if i < len(p_odd):
                tds = p_odd[i].find_all("td")
                parse_player_table(tds, player_key)

            if i < len(p_even):
                tds = p_even[i].find_all("td")
                parse_player_table(tds, player_key)

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
                dobs = []
                nations = []
                nations2 = []
                heights = []
                foots = []
                positions = []
                values = []

                # From player page
                agents = []
                pobs = []

                # From player table
                sappear = []
                mappear = []
                ppg = []
                goals = []
                assists = []
                ogs = []
                subon = []
                suboff = []
                yellow = []
                yellow2 = []
                red = []
                pens = []
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
                            data = [0,0,0,0,0,0,0,0,0,0,0,0,0]
                            player_name = ""
                            birth = ""
                            nat = ""
                            nat2 = ""
                            height = ""
                            foot = ""
                            pos = ""
                            val = ""
                            tds = odd[i].find_all("td")

                            player_name = odd[i].find_all("tr")[0].find_all("td")[1].find_all("span")[0].text
                            player_url = odd[i].find("a", {"class": "spielprofil_tooltip"})['href']
                            player_id = player_url.split("/")[-1]
                            pos = tds[4].text
                            birth = tds[5].text[:-5]
                            if len(birth.split(",")) > 1:
                                birth = birth.split(",")[0] + birth.split(",")[1]
                            else:
                                birth = ''

                            if player_id not in player_dict:
                                add_to_player_dict(player_url, player_name, birth, player_id)

                            if player_id in player_dict:
                                pobs.append(player_dict[player_id]['pob'])
                                agents.append(player_dict[player_id]['agent'])
                            
                            if str(year) in player_dict[player_id]['stats']:
                                data = player_dict[player_id]['stats'][str(year)]
                            
                            sappear.append(data[0])
                            mappear.append(data[1])
                            ppg.append(data[2])
                            goals.append(data[3])
                            assists.append(data[4])
                            ogs.append(data[5])
                            subon.append(data[6])
                            suboff.append(data[7])
                            yellow.append(data[8])
                            yellow2.append(data[9])
                            red.append(data[10])
                            pens.append(data[11])
                            mins.append(data[12])

                            flags = odd[i].find_all("img", {"class": "flaggenrahmen"})
                            if len(flags) == 1:
                                nat = flags[0].get("title")
                            elif len(flags) == 2:
                                nat = flags[0].get("title")
                                nat2 = flags[1].get("title")
                                
                            if "Korea" in nat and "South" in nat:
                                nat = "South Korea"
                            if "Korea" in nat2 and "South" in nat:
                                nat = "South Korea"
                            if "Korea" in nat and "North" in nat:
                                nat = "North Korea"
                            if "Korea" in nat2 and "North" in nat:
                                nat = "North Korea"
                            if len(tds) == 13:
                                height = tds[7].text
                                if height != "" and height != "N/A":
                                    height = height[:-2].replace(",", ".")
                                
                                foot = tds[8].text
                            if len(tds) == 14:
                                height = tds[8].text
                                if height != "" and height != "N/A":
                                    height = height[:-2].replace(",", ".")
                                
                                foot = tds[9].text
                            
                            val_td = odd[i].find("td", {"class": "rechts hauptlink"})
                            if val_td.text.strip() != "" and val_td.text.strip() != "-":
                                val = format_value(val_td.text.strip())

                            pids.append(player_id)
                            pnames.append(player_name)
                            dobs.append(birth)
                            nations.append(nat.replace("\"", "").replace(",", ""))
                            nations2.append(nat2.replace("\"", "").replace(",", ""))
                            heights.append(height)
                            foots.append(foot)
                            positions.append(pos)
                            values.append(val)

                        if i < len(even):
                            data = [0,0,0,0,0,0,0,0,0,0,0,0,0]
                            player_name = ""
                            birth = ""
                            nat = ""
                            nat2 = ""
                            height = ""
                            foot = ""
                            pos = ""
                            val = ""
                            tds = even[i].find_all("td")

                            player_name = even[i].find_all("tr")[0].find_all("td")[1].find_all("span")[0].text
                            player_url = even[i].find("a", {"class": "spielprofil_tooltip"})['href']
                            player_id = player_url.split("/")[-1]
                            pos = tds[4].text
                            birth = tds[5].text[:-5]
                            if len(birth.split(",")) > 1:
                                birth = birth.split(",")[0] + birth.split(",")[1]
                            else:
                                birth = ''

                            if player_id not in player_dict:
                                add_to_player_dict(player_url, player_name, birth, player_id)
                            if player_id in player_dict:
                                pobs.append(player_dict[player_id]['pob'])
                                agents.append(player_dict[player_id]['agent'])
                            if str(year) in player_dict[player_id]['stats']:
                                data = player_dict[player_id]['stats'][str(year)]
                            
                            sappear.append(data[0])
                            mappear.append(data[1])
                            ppg.append(data[2])
                            goals.append(data[3])
                            assists.append(data[4])
                            ogs.append(data[5])
                            subon.append(data[6])
                            suboff.append(data[7])
                            yellow.append(data[8])
                            yellow2.append(data[9])
                            red.append(data[10])
                            pens.append(data[11])
                            mins.append(data[12])

                            flags = even[i].find_all("img", {"class": "flaggenrahmen"})
                            if len(flags) == 1:
                                nat = flags[0].get("title")
                            elif len(flags) > 1:
                                nat = flags[0].get("title")
                                nat2 = flags[1].get("title")
                            if "Korea" in nat and "South" in nat:
                                nat = "South Korea"
                            if "Korea" in nat2 and "South" in nat:
                                nat = "South Korea"
                            if "Korea" in nat and "North" in nat:
                                nat = "North Korea"
                            if "Korea" in nat2 and "North" in nat:
                                nat = "North Korea"
                            if len(tds) == 13:
                                height = tds[4].text
                                if height != "" and height != "N/A":
                                    height = height[:-2].replace(",", ".")
                                
                                foot = tds[5].text
                            if len(tds) == 14:
                                height = tds[8].text
                                if height != "" and height != "N/A":
                                    height = height[:-2].replace(",", ".")
                                
                                foot = tds[9].text
                            
                            val_td = even[i].find("td", {"class": "rechts hauptlink"})
                            if val_td.text.strip() != "" and val_td.text.strip() != "-":
                                val = format_value(val_td.text.strip())
                            pids.append(player_id)
                            pnames.append(player_name)
                            dobs.append(birth)
                            nations.append(nat.replace("\"", "").replace(",", ""))
                            nations2.append(nat2.replace("\"", "").replace(",", ""))
                            heights.append(height)
                            foots.append(foot)
                            positions.append(pos)
                            values.append(val)
                    
                    field_names = ["PID", "Player Name", "Team Name", "League Name", "Year", "Nationality 1", "Nationality 2", "Position", "Date of Birth", "Market Value", "Birth Place", 
                    "Foot", "Height", "Agent", "Squad App", "Match App", "PPG", "Goals", "Assists", "Own Goals", "Subs On", "Subs Off", "Yellow Cards", "Second Yellow", "Red Cards", "Penalty Goals", "Minutes Played"]
                    with open("player_data_fall_2021_brit.csv", 'a') as out_file:
                        writer = csv.DictWriter(out_file, fieldnames=field_names)
                        for pidx in range(len(pids)):
                            out_dict = {"PID": pids[pidx], "Player Name": pnames[pidx], "Team Name": teamName, "League Name": leagueName, "Year": year, "Nationality 1": nations[pidx], "Nationality 2": nations2[pidx], "Position": positions[pidx], "Date of Birth": dobs[pidx], "Market Value": values[pidx], "Birth Place": pobs[pidx] , 
                    "Foot": foots[pidx], "Height": heights[pidx], "Agent": agents[pidx], "Squad App": sappear[pidx], "Match App": mappear[pidx], "PPG": ppg[pidx], "Goals":goals[pidx], "Assists": assists[pidx], "Own Goals": ogs[pidx], "Subs On": subon[pidx], "Subs Off": suboff[pidx], "Yellow Cards": yellow[pidx], "Second Yellow": yellow2[pidx], "Red Cards": red[pidx], "Penalty Goals": pens[pidx], "Minutes Played": mins[pidx]}
                            
                            writer.writerow(out_dict)
                    last_team = (teamidx == len(teamUrls) - 1)
                    last_year = (year == 2020)
                    print_status(last_team, last_year)

except Exception as e:
    print(e)
    log_stream = StringIO()
    logging.basicConfig(stream=log_stream, level=logging.INFO)
    logging.error("Exception occurred", exc_info=True)
    send_email_crash_notification(log_stream.getvalue(), league_offset, year_offset , team_offset)




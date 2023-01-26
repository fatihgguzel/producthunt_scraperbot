import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import threading



xml = []
# function to extract user data from a user profile
def extract_user_data(user_url):
    try:
        # get the HTML content of the user profile
        page = requests.get(user_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        user_data = soup.find('script', id='__NEXT_DATA__')
        data = json.loads(user_data.text)

        name_surname = ''
        bio = ''
        follower_number = 0
        point = 0
        streak_number = 0
        about_headline = ''
        social_links = ''
        last_activity_date = ''

        # name surname, bio, followers count, point, streaknumber, abouttext
        for key in data['props']['apolloState']:
            if key.startswith('User') and not key.startswith('UserLink') and not key.startswith('UserBadge') :
                name_surname = data['props']['apolloState'][key]['name']
                bio = data['props']['apolloState'][key]['about']
                follower_number = data['props']['apolloState'][key]['followersCount']
                point = data['props']['apolloState'][key]['karmaBadge']['score']
                streak_number = data['props']['apolloState'][key]['visitStreak']['duration']
                headline_text = data['props']['apolloState'][key]['meta']['description']
                sub_headline = headline_text[headline_text.index('(') + 1:headline_text.index(')')]
                if len(sub_headline) == 0:
                    sub_headline = 'null'
                about_headline = sub_headline

        # social links
        social_links_for_user = ''
        for key in data['props']['apolloState']:
            if key.startswith('UserLink'):
                social_links_for_user += (data['props']['apolloState'][key]['url']) + ' '
        social_links = social_links_for_user

        # last activity date
        for key in data['props']['apolloState']:
            if key.startswith('Post'):
                iso_date = data['props']['apolloState'][key]['createdAt']
                date_object = datetime.fromisoformat(iso_date)
                last_activity_date = date_object.strftime("%A, %B %d, %Y %I:%M:%S %p")
                break

        # append the user data as a list
        global xml
        xml.append([user_url, name_surname, bio, follower_number, point, streak_number, about_headline, social_links,
                    last_activity_date])
    except (AttributeError, KeyError):
        print(user_url)


def data_to_spreadsheet(pageName, list):
    #TODO .JSON FILE
    SERVICE_ACCOUNT_FILE = 'keys.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = None
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID of spreadsheet.
    SAMPLE_SPREADSHEET_ID = 'TODO'

    try:
        service = build('sheets', 'v4', credentials=credentials)

        # Call the Sheets API
        sheet = service.spreadsheets()
        request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=pageName + '!A2',
                                        valueInputOption='USER_ENTERED', body={'values': list}).execute()

    except HttpError as err:
        print(err)


# read the XML files
#TODO
tree1 = ET.parse('users_sitemap1.xml')
root1 = tree1.getroot()
tree2 = ET.parse('users_sitemap2.xml')
root2 = tree2.getroot()

url_dict1 = root1.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url')
url_dict2 = root2.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url')

url_dict = url_dict1 + url_dict2

# loop through the user profile links in the XML files
threads = []
for i in range(len(url_dict)):
    user_url = url_dict[i][0].text
    print('i : ',i)
    t = threading.Thread(target=extract_user_data, args=(user_url,))
    threads.append(t)
    t.start()

for t in threads:
    t.join(5)

data_to_spreadsheet('Sayfa1', xml)

print('scraping completed.')






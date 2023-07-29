import sys
import re
from bs4 import BeautifulSoup
import pandas as pd
import json



data =  '''"user1": {
    "name": 'adsad',
    "email": 'rew@gmail.com'
    
} '''

soup = BeautifulSoup(data, 'html.parser')
res = soup.find('script')
json_object = json.loads(res.contents[0])

print(json_object)
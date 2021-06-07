# -*- coding: utf-8 -*-

import re
import pandas as pd
from datetime import datetime, time
from dateutil.parser import parse
import string

import requests


def dateTorfc2822(date, maxDate=False):
    '''Convert date in format YYYY/MM/DD to RFC2822 format.
        If value is None, return current utc time.
    '''
    if maxDate:
        date = datetime.combine(date, time.max)
    else:
        date = datetime.combine(date, time.min)
    return date.strftime('%a, %d %b %Y %H:%M:%S -0000')

def extractcolumns(json_response,column_names):
    '''Extract only columns related to the client report
       Return a pandas Dataframe object
    '''
    result = []
    for item in json_response.get('items', {}):
        email = dict(item)
        tags = email.get('tags',' ')
        #if (email) and (tags) and ('test' not in str(tags)) and (tags !=' '):
        if (email) and ('test' not in str(tags)):
            
            date_time = datetime.fromtimestamp(email.get('timestamp'))
            
            subject = email.get('message', {}).get('headers', {}).get('subject', '')
            match = re.search(r'XXX:\s\d*',subject) if subject else ''
            
            useragent = email.get('client-info', {}).get('user-agent', '')
            
            isRouted = email.get('flags', {}).get('is-routed', '')
            isAuthenticated = email.get('flags', {}).get('is-authenticated', '')
            isSystemTest = email.get('flags', {}).get('is-system-test', '')
            isTestMode = email.get('flags', {}).get('is-test-mode', '')            
            
            routes = list(email.get('routes', ''))
            routeExpression =''
            routeID = ''
            routeRecipient = ''
            for i in routes:
                route = dict(i)
                routeExpression = route.get('expression', ''),
                routeID = route.get('id', ''),
                routeRecipient = route.get('match', {}).get('recipient', '')
            
            result.append([
                email.get('event',''),
                str(tags[0]) if tags else '',
                date_time.strftime("%Y-%m-%d %H:%M:%S"),
                email.get('id'),
                email.get('recipient'),
                email.get('envelope', {}).get('sender', ''),
                email.get('recipient-domain',''),
                subject.translate(str.maketrans('', '', string.punctuation)),
                email.get('user-variables', {}).get('messageID', ''),
                email.get('geolocation', {}).get('country', ''),
                email.get('geolocation', {}).get('region', ''),
                email.get('geolocation', {}).get('city', ''),
                email.get('client-info', {}).get('client-type', ''),
                email.get('client-info', {}).get('device-type', ''),
                email.get('client-info', {}).get('client-name', ''),
                useragent.translate(str.maketrans('', '', string.punctuation)),
                email.get('client-info', {}).get('client-os', ''),
                1 if isRouted  else 0,
                1 if isAuthenticated  else 0,
                1 if isSystemTest else 0,
                1 if isTestMode else 0,
                routeExpression ,
                routeID,
                routeRecipient,
                email.get('url', ''),                
                email.get('message', {}).get('headers', {}).get('message-id', '')
            ]
            )
    r
def requestlog(url,key, params):
    '''MailGun API call
       Return a JSON like object
    '''
    r = requests.get(url, auth=("api", key), params=params)
    return r.json()

def main():
    # url for retrieval
    domain = YOUR_DOMAMIN
 
    #API key
    api_key = YOUR_API_KEY
  
    # Events URL
    url = 'https://api.mailgun.net/v3/%s/events' % domain
  
    column_names = ['event','tags','messageDatetime','id'
                    ,'recipient','sender','recipientDomain','subject'
                    ,'messageId','geolocCountry', 'geolocRegion'
                    , 'geolocCity','deviceClientType', 'deviceType'
                    , 'deviceClientName', 'deviceAgent', 'deviceOS'
                    , 'isRouted', 'isAuthenticated', 'isSystemTest', 'isTestMode'
                    , 'routesExpression', 'routesId', 'routesRecipient', 'URLclicked'
                    , 'mailgunID']
    
    # Final result storage
    dfmain = pd.DataFrame(columns = column_names)
    
    begin_day = parse("2021/02/16")
    end_day = parse("2021/02/17")
    
    # format to mailGun accepted format rfc2822
    begin = dateTorfc2822(begin_day, maxDate=False)
    end = dateTorfc2822(end_day,maxDate=True)
    
    print(begin)
    print(end)
    # set paramns to query
    params = {
            'begin': begin,
            'end': end,
            'limit': 300,
            'ascending': True
        }
    
    # Flog to stop the loop
    keepGoing = True
    
    i=0
    while keepGoing:
        
        # Calls mailGun API
        json_response = requestlog(url,api_key, params)
        
        # Check if there is items to read
        if len(json_response['items'])>0:        
            print(url)
            
            # Group previews result with the new one
            dfmain = pd.concat([dfmain, extractcolumns(json_response,column_names)])
            
            # Change the URRL to the next page
            url = json_response['paging']['next']
            
            # Loop Control
            i +=1
            print('loop n: '+ str(i))
        else:
            # Dump the result as a CSV file
            dfmain.to_csv('MailGunlogs.csv',index=False)
            keepGoing = False
            break
    
if __name__ == '__main__':
    main()
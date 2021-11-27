import pandas as pd
import requests
import pandas as pd
# For parsing the dates received from twitter in readable formats
from datetime import date, datetime, timedelta
import pytz
#To add wait time between requests
import time
import logging
from config_logger import APP_NAME

logger = logging.getLogger(APP_NAME)
class TwitterExtractor:
    
    def __init__(self, database_con, companies_table, tweets_table, api_token, append):
        self.__database_con = database_con
        self.__companies_table = companies_table
        self.__tweets_table = tweets_table
        self.__api_token = api_token
        self.__append = append
        self.get_companies()
    
    def _add_companies(self, companies):
        '''
        Add companies to database
        '''
        self.df_companies = pd.DataFrame(data=companies)
        self.df_companies.to_sql(name=self.__companies_table, 
                        con=self.__database_con,
                        if_exists='append' if self.__append else 'replace')

    def get_companies(self):
        '''
        Load companies from database
        '''
        self.df_companies = pd.read_sql_table(self.__companies_table, self.__database_con)
        self.df_companies = self.df_companies.set_index('id')
        logger.info('Successfully loaded company table!')
        return self.df_companies

    def add_company(self, id, name, query):
        if id not in self.df_companies.index:

            df_company = pd.DataFrame(data={'id':[id], 'name':[name], 'query':[query]})
            df_company = df_company.set_index('id')
            df_company.to_sql(name=self.__companies_table, 
                        con=self.__database_con,
                        if_exists='append')
            logger.info(f'The company {name} has been added to the database.')                        
        else:
            logger.warning('Already a company with the same id, carrying the company not added.')
        self.get_companies()

        

    def _create_headers(self):
        headers = {"Authorization": "Bearer {}".format(self.__api_token)}
        return headers
    
    def _create_url(self, keyword, start_date, max_results = 10):
    
        search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

        #change params based on the endpoint you are using
        query_params = {'query': keyword,
                        'start_time': start_date,
                        'max_results': max_results,
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {}}
        return (search_url, query_params)
    
    def _connect_to_endpoint(self, url, headers, params, next_token = None):
        params['next_token'] = next_token   #params object received from create_url function
        response = requests.request("GET", url, headers = headers, params = params)
        #print("Endpoint Response Code: " + str(response.status_code))
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()
    
    def _extract_tweets(self, company, max_tweets=10000, results_by_page=50):
        today = date.today()
        today = datetime.combine(date.today(), datetime.min.time())
        delta = timedelta(hours=24)
        start_time = (datetime.now() - timedelta(hours=24)).replace(tzinfo=pytz.timezone('America/Sao_Paulo'))
        start_time =start_time.isoformat()
        headers = self._create_headers()
        keyword = f"{company['query']} lang:pt"

        search = True
        next_token = None
        df_tweets = pd.DataFrame()
        page = 1
        while search and df_tweets.shape[0] < max_tweets:

            url = self._create_url(keyword, start_time, results_by_page)
            json_response = self._connect_to_endpoint(url[0], headers, url[1], next_token=next_token)


            for tweet in json_response['data']:
                tweet_info = {'company':company['name'], 'replied_to': False, 'quoted': False}
                retweet = False
                if 'referenced_tweets' in tweet: 
                    for ref in tweet['referenced_tweets']:
                        if ref['type'] == 'retweeted':
                            retweet = True
                        else:
                            tweet_info[ref['type']] = True
                if retweet:
                    continue
                for metric in tweet['public_metrics']:
                    tweet_info[metric] = tweet['public_metrics'][metric]
                tweet_info['text'] = tweet['text']
                tweet_info['source'] = tweet['source']
                tweet_info['lang'] = tweet['lang']
                tweet_info['reply_settings'] = tweet['reply_settings']
                tweet_info['created_at'] = tweet['created_at']
                df_tweets = df_tweets.append(tweet_info, ignore_index=True)
            if 'next_token' in json_response['meta']:
                next_token = json_response['meta']['next_token']
            else:
                next_token = None
            print(f"\rpage: {page}, tweets: {df_tweets.shape[0]}", end='')
            page += 1
            if next_token is None:
                search = False
        print()
        
        return df_tweets
    
    def extract_last_tweets(self,  max_tweets_search=10000, results_by_page=100):
        new_tweets = pd.DataFrame()

        for i, (c_id, company) in enumerate(self.df_companies.iterrows()):
            logger.info(f"[{i+1}/{self.df_companies.shape[0]}] Extracting tweets about company {company['name']}...")
            new_tweets = new_tweets.append(self._extract_tweets(company, max_tweets=max_tweets_search, results_by_page=results_by_page), ignore_index=True)
            time.sleep(10)

        new_tweets = new_tweets[new_tweets.lang == 'pt']
        new_tweets['created_at'] = pd.to_datetime(new_tweets['created_at'])
        
        
        logger.info(f'Extraction of {new_tweets.shape[0]} tweets completed successfully!')

        logger.info('Upload tweets to database...')
        
        new_tweets.to_sql(name=self.__tweets_table, 
                        con=self.__database_con,
                        if_exists='append' if self.__append else 'replace')
        logger.info('Upload completed successfully!')
    def get_tweets(self):
        logger.info('Load tweets from database...')
        self.df_tweets = pd.read_sql_table(self.__tweets_table, self.__database_con)
        logger.info(f'Load of {self.df_tweets.shape[0]} completed successfully!')
        return self.df_tweets
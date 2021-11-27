from twitter_extractor import TwitterExtractor
import argparse
import yaml
import logging
from config_logger import APP_NAME
# This sets the root logger to write to stdout (your console).
# Your script/app needs to call this somewhere at least once.
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(APP_NAME)
def get_parser():
    parser = argparse.ArgumentParser(description='Process args.')
    parser.add_argument('--extract', 
                        action='store_true',
                        default=False)

    parser.add_argument('--save', 
                        action='store_true',
                        default=False)

    parser.add_argument('--append', 
                        action='store_true',
                        default=False)

    parser.add_argument('--credentials', 
                        action='store',
                        default=None,
                        help='Path to credentials file')
    parser.add_argument('--companies_tb', 
                        action='store',
                        default='companies',
                        help='Companies Table Name')
    parser.add_argument('--tweets_tb', 
                        action='store',
                        default='tweets',
                        help='Tweets table name')
    
    parser.add_argument('--save_path', 
                        action='store',
                        default='tweets.csv',
                        help='Path to tweets save file')
    
                            

    return parser

def load_credentials(credentials_path):
     with open(credentials_path, "r") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)
            
if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    credentials = load_credentials(args.credentials)
    extractor = TwitterExtractor(database_con=credentials['database_con'], 
                                 companies_table=args.companies_tb, 
                                 tweets_table=args.tweets_tb,
                                 api_token=credentials['twitter_api'], 
                                 append=args.append)

    if args.extract:
        extractor.extract_last_tweets()

    if args.save:
        df_tweets = extractor.get_tweets()
        df_tweets.to_csv(args.save_path, index=None)
        logger.info(f"Tweets save in {args.save_path}")
    
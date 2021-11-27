from twitter_extractor import TwitterExtractor
import argparse
import yaml

def get_parser():
    parser = argparse.ArgumentParser(description='Process args.')
    parser.add_argument('--credentials', 
                        action='store',
                        default=None,
                        help='Path to credentials file')
    parser.add_argument('--companies_tb', 
                        action='store',
                        default='companies',
                        help='Path to credentials file')
    parser.add_argument('--tweets_tb', 
                        action='store',
                        default='tweets',
                        help='Path to credentials file')
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
                                 api_token=credentials['twitter_api'])
    extractor.extract_last_tweets()
    
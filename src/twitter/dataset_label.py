import pandas as pd
import tkinter as tk
import argparse
import logging
import yaml
from twitter_extractor import TwitterExtractor
from config_logger import APP_NAME

logger = logging.getLogger(APP_NAME)

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(APP_NAME)



def get_parser():
    parser = argparse.ArgumentParser(description='Process args.')

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
    parser.add_argument('--n_samples', 
                        action='store',
                        type=int,
                        help='Number of samples')

    return parser


def load_credentials(credentials_path):
     with open(credentials_path, "r") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)

LABELS = ['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']

class LabelText():
    def __init__(self):
        self.df_sample = pd.read_csv('sample.csv')

    def __call__(self, sample):
        if sample['text'] not in self.df_sample['text']:
            logger.info(f"text size: {len(sample['text'])}")
            self.sample = sample
            self.root = tk.Tk()
            self.label = tk.Label(self.root, 
                    text=f"Company:{sample['company']}\nText:{sample['text']}",
                    wraplength=250,
                    padx = 20).pack()
            self.v = tk.StringVar()
            for label in LABELS:
                r = tk.Radiobutton(self.root, 
                            text=label,
                            padx = 20, 
                            variable=self.v,
                            command=self.quit, 
                            value=label).pack(pady=5)
            self.root.mainloop()
        

    def quit(self):
        logger.info(f"Choice: {self.v.get()}")
        new_sample = {'text':self.sample['text'], 'company':self.sample['company'], 'label':self.v.get()}
        self.df_sample = self.df_sample.append(new_sample, ignore_index=True)
        self.df_sample.to_csv('sample.csv', index=None)
        self.root.destroy()


def close_window(tk_root):
    tk_root.close()

def label_sample(sample):
    tk_root = tk.Tk()
    tk_root.geometry('300x220')
    tk_root.resizable(False, False)
    tk_root.title('Label Text')
    tk.Label(tk_root, 
         text=f"Company:{sample['company']}\nText:{sample['text']}",
         justify = tk.LEFT,
         padx = 20).pack(fill='x', padx=5, pady=5)

    v = tk.StringVar()

    for label in LABELS:
        r = tk.Radiobutton(tk_root, 
                    text=label,
                    padx = 20, 
                    variable=v, 
                    value=label).pack(pady=5)
    button = tk.Button(
                tk_root,
                text="Get Selected Size",
                command=close_window)
    button.pack(fill='x', padx=5, pady=5)
    tk_root.mainloop()
    logger.info(f"Choice: {v.get()}")

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    credentials = load_credentials(args.credentials)
    extractor = TwitterExtractor(database_con=credentials['database_con'], 
                                 companies_table=args.companies_tb, 
                                 tweets_table=args.tweets_tb,
                                 api_token=credentials['twitter_api'], 
                                 append=True)

    df_tweets = extractor.get_tweets()

    app = LabelText()
    for i, (tweet_id, tweet) in enumerate(df_tweets.sample(args.n_samples).iterrows()):
        app(tweet)
        app.df_sample.to_csv('sample.csv', index=None)
        
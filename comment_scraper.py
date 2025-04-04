import praw
import pandas as pd
import os
from datetime import datetime

# Remove the single output file constant
# FILENAME_COMMENTS = 'output_comments_after2022.csv'


def create_reddit_instance():
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )
    return reddit


def get_subreddit(reddit, subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)
    return subreddit


def load_existing_data(year):
    # Create the data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    file_name = f'data/comments_{year}.csv'
    if os.path.exists(file_name):
        df = pd.read_csv(file_name)
        existing_ids = df['id'].tolist()
    else:
        df = pd.DataFrame(columns=['id', 'url', 'score', 'body', 'date'])
        existing_ids = []
    return df, existing_ids, file_name


def get_new_comment_row(comment):
    date = datetime.fromtimestamp(comment.created)
    new_row = {
        "id": comment.id,
        "url": "https://www.reddit.com" + comment.permalink,
        "score": comment.score,
        "body": comment.body,
        "date": date
    }
    return new_row


def save_data(df, file_name):
    df.to_csv(file_name, index=False)


def main():
    reddit = create_reddit_instance()
    subreddit = get_subreddit(reddit, 'tunisia')
    
    # Dict to store dataframes by year
    year_dfs = {}
    year_existing_ids = {}
    year_filenames = {}

    print('Starting to scrape comments')

    # Fetch 1000 newest comments
    new_comments = list(subreddit.comments(limit=1000))

    for comment in new_comments:
        date = datetime.fromtimestamp(comment.created)
        year = date.year
        
        # If we haven't loaded this year's data yet, load it
        if year not in year_dfs:
            year_dfs[year], year_existing_ids[year], year_filenames[year] = load_existing_data(year)
        
        # Skip if already processed
        if comment.id in year_existing_ids[year]:
            print(f'Skipped comment {comment.id} (already in {year} data)')
            continue
            
        new_row = get_new_comment_row(comment)
        year_dfs[year] = year_dfs[year]._append(new_row, ignore_index=True)
        print(f'Added comment {comment.id} to {year} data')

    # Save each year's data to its respective file
    for year, df in year_dfs.items():
        save_data(df, year_filenames[year])
        print(f"Data for {year} saved to {year_filenames[year]}")

    print('Finished scraping')


if __name__ == "__main__":
    main()

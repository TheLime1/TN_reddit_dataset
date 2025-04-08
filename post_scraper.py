import time
import praw
import prawcore
import pandas as pd
import os
from datetime import datetime


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
    
    # Define the expected columns
    columns = [
        'id', 'url', 'score', 'title', 'body', 
        'top_comment1', 'top_comment2', 'top_comment3', 
        'top_comment4', 'top_comment5', 'date'
    ]
    
    file_name = f'data/posts_{year}.csv'
    if os.path.exists(file_name):
        try:
            df = pd.read_csv(file_name)
            # Check if 'id' column exists
            if 'id' in df.columns:
                existing_ids = df['id'].tolist()
            else:
                print(f"Warning: 'id' column not found in {file_name}. Creating a new DataFrame.")
                df = pd.DataFrame(columns=columns)
                existing_ids = []
        except Exception as e:
            print(f"Error loading {file_name}: {str(e)}. Creating a new DataFrame.")
            df = pd.DataFrame(columns=columns)
            existing_ids = []
    else:
        df = pd.DataFrame(columns=columns)
        existing_ids = []
    
    return df, existing_ids, file_name


def get_top_comments(submission):
    top_comments = []
    submission.comments.sort_by = 'top'
    for comment in submission.comments[:5]:
        top_comments.append(comment.body)
    return top_comments


def get_new_post_row(submission, top_comments):
    date = datetime.fromtimestamp(submission.created)
    # Use the correct Reddit URL format
    reddit_url = f"https://www.reddit.com/r/Tunisia/comments/{submission.id}/"
    new_row = {
        "id": submission.id,
        "url": reddit_url,  # Use our formatted URL instead of submission.url
        "score": submission.score,
        "title": submission.title,
        "body": submission.selftext,
        "top_comment1": top_comments[0] if len(top_comments) > 0 else None,
        "top_comment2": top_comments[1] if len(top_comments) > 1 else None,
        "top_comment3": top_comments[2] if len(top_comments) > 2 else None,
        "top_comment4": top_comments[3] if len(top_comments) > 3 else None,
        "top_comment5": top_comments[4] if len(top_comments) > 4 else None,
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

    print('Starting to scrape posts')

    # Fetch 1000 newest posts
    new_posts = list(subreddit.new(limit=1000))

    for submission in new_posts:
        date = datetime.fromtimestamp(submission.created)
        year = date.year
        
        # If we haven't loaded this year's data yet, load it
        if year not in year_dfs:
            year_dfs[year], year_existing_ids[year], year_filenames[year] = load_existing_data(year)
        
        # Skip if already processed
        if submission.id in year_existing_ids[year]:
            print(f'Skipped post {submission.id} (already in {year} data)')
            continue
            
        try:
            top_comments = get_top_comments(submission)
            new_row = get_new_post_row(submission, top_comments)
            year_dfs[year] = year_dfs[year]._append(new_row, ignore_index=True)
            print(f'Added post {submission.id} to {year} data')
        except prawcore.exceptions.TooManyRequests:
            print("Hit rate limit, sleeping .....")
            time.sleep(60)

    # Save each year's data to its respective file
    for year, df in year_dfs.items():
        save_data(df, year_filenames[year])
        print(f"Data for {year} saved to {year_filenames[year]}")

    print('Finished scraping')


if __name__ == "__main__":
    main()

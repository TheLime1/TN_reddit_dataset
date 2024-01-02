import praw
import pandas as pd
import os
from datetime import datetime

FILENAME_COMMENTS = 'output_comments_after2022.csv'


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


def load_existing_data(file_name):
    if os.path.exists(file_name):
        df = pd.read_csv(file_name)
        existing_ids = df['id'].tolist()
    else:
        df = pd.DataFrame()
        existing_ids = []
    return df, existing_ids


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
    df_comments, existing_comment_ids = load_existing_data(FILENAME_COMMENTS)

    print('Starting to scrape comments')

    # Fetch 1000 newest comments
    new_comments = list(subreddit.comments(limit=1000))

    for comment in new_comments:
        if comment.id in existing_comment_ids or comment.created_utc < 1672444800:  # Skip comments before 2022
            print(f'Skipped comment {comment.id}')
            continue
        new_row = get_new_comment_row(comment)
        df_comments = df_comments._append(new_row, ignore_index=True)
        save_data(df_comments, FILENAME_COMMENTS)
        print(f'Loaded comment {comment.id}')

    print('Finished scraping')
    print("Data saved to ", FILENAME_COMMENTS)


if __name__ == "__main__":
    main()

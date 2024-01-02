import time
import praw
import prawcore
import pandas as pd
import os
from datetime import datetime

FILENAME_POSTS = 'output_posts_after2022.csv'


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


def get_top_comments(submission):
    top_comments = []
    submission.comments.sort_by = 'top'
    for comment in submission.comments[:5]:
        top_comments.append(comment.body)
    return top_comments


def get_new_post_row(submission, top_comments):
    date = datetime.fromtimestamp(submission.created)
    new_row = {
        "id": submission.id,
        "url": submission.url,
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
    df_posts, existing_post_ids = load_existing_data(FILENAME_POSTS)

    print('Starting to scrape posts')

    # Fetch 1000 newest posts
    new_posts = list(subreddit.new(limit=1000))

    for submission in new_posts:
        if submission.id in existing_post_ids:
            print(f'Skipped post {submission.id}')
            continue
        try:
            top_comments = get_top_comments(submission)
            new_row = get_new_post_row(submission, top_comments)
            df_posts = df_posts._append(new_row, ignore_index=True)
        except prawcore.exceptions.TooManyRequests:
            print("Hit rate limit, sleeping .....")
            time.sleep(60)

    save_data(df_posts, FILENAME_POSTS)
    print('Finished scraping')
    print("Data saved to ", FILENAME_POSTS)


if __name__ == "__main__":
    main()

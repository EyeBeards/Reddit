import datetime
import sqlite3
import praw
import time

def update_db(feed, records):
    conn = sqlite3.connect('reddit.db')
    c = conn.cursor()
    subreddit = feed[:feed.find('/')]

    columns = ''' (
                feed text,
                position integer,
                id text,
                time real,
                title text, 
                author text,
                created_utc real, 
                num_comments integer,
                score integer, 
                upvote_ratio real,
                over_18 integer,
                is_self integer,
                url text
                )'''
    table_sql = 'CREATE TABLE IF NOT EXISTS ' + subreddit + columns
    c.execute(table_sql)

    insert_sql = 'INSERT INTO ' + subreddit + ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
    c.executemany(insert_sql, records)

    conn.commit()
    conn.close()


# get title, creation time, upvote count, tags, and comments for each submission
def record_submissions(feeds):
    t = time.time()
    for feed, submissions in feeds.items():
        records = []
        count = 0
        for post in submissions:
            if not records:
                records = [(feed, count, post.id, t, post.title, post.author.name, post.created_utc, post.num_comments, post.score, post.upvote_ratio, post.over_18, post.is_self, post.url)]
            else:
                records.append((feed, count, post.id, t, post.title, post.author.name, post.created_utc, post.num_comments, post.score, post.upvote_ratio, post.over_18, post.is_self, post.url))
            count += 1
        update_db(feed, records)

# get the submissions from each subreddit in a list
def get_feeds(reddit, subreddits):
    if len(subreddits) > 5:
        print('MAX subreddits: 5')
        subreddits = subreddits[:4]
    feed_return = {}
    for sub in subreddits:
        try:
            feed_return['{}/hot'.format(sub)] = reddit.subreddit(sub).hot()
            feed_return['{}/rising'.format(sub)] = reddit.subreddit(sub).rising()
            feed_return['{}/new'.format(sub)] = reddit.subreddit(sub).new()
        except:
            print('{} failed'.format(sub))

    return feed_return


def timer(minutes, reddit, subs_list):
    minute = int(datetime.datetime.now().strftime('%M'))
    delay = 16
    if minute < minutes:
        delay = minutes - minute
    else:
        factor = (minute // minutes) * minutes
        delay = minutes - (minute - factor)

    print('delay: {} at {}'.format(delay, datetime.datetime.now().strftime('%H:%M')))
    time.sleep(delay * 60)
    feeds = get_feeds(reddit, subs_list)
    record_submissions(feeds)


def main():
    subs_list = ['Art', 'dataisbeautiful', 'politics']
    reddit = praw.Reddit('bot_1', user_agent='python/praw:post_analysis:v0.1')

    run = True
    while run:
        try:
            print('timer: {}'.format(datetime.datetime.now().strftime('%H:%M')))
            timer(15, reddit, subs_list)
        except:
            main()

main()

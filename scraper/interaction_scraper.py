import csv
import praw
import re
import os
from time import time, sleep

reddit_client = os.environ.get('REDDIT_CLIENT')
reddit_secret = os.environ.get('REDDIT_SECRET')

reddit = praw.Reddit(client_id=reddit_client, client_secret=reddit_secret, user_agent='Interaction Scraper')
subreddit_pattern = r'(\s|^|reddit\.com)\[?\/?r\/([a-zA-Z0-9-_]+)'

start_time = time()
last_update = 0
seen_posts = 0
valid_posts = 0

while True:
  try:
    for post in reddit.subreddit('all').stream.submissions():
      seen_posts += 1

      post_vars = vars(post)
      post_subreddit = post_vars['subreddit'].display_name
      if post_vars['over_18'] or post_subreddit.startswith('u_'):
        continue

      crossposts = set()
      if 'crosspost_parent_list' in post_vars:
        crosspost_parents = [parent['subreddit'] for parent in post_vars['crosspost_parent_list']]
        crosspost_parents = [subreddit for subreddit in crosspost_parents if not subreddit.startswith('u_')]
        if crosspost_parents:
          crossposts.add(crosspost_parents[-1])

      if not crossposts:
        # Don't include posts mentioned in crossposts as mentions for this post's subreddit
        mentions = set()
        if post_vars['selftext']:
          for match in re.finditer(subreddit_pattern, post_vars['selftext']):
            mention = match.group(2).strip()
            if mention:
              mentions.add(mention)

      if mentions or crossposts:
        valid_posts += 1

        with open('subreddit_interactions.csv', 'a') as file:
          writer = csv.writer(file)

          for crossposter in crossposts:
            writer.writerow([int(post_vars['created_utc']), post_vars['author'].name, post_vars['title'], crossposter, post_subreddit, 'CROSSPOSTED_TO'])

          for mentioned in mentions:
            writer.writerow([int(post_vars['created_utc']), post_vars['author'].name, post_vars['title'], post_subreddit, mentioned, 'MENTIONS'])

      if time() - last_update > 60:
        print(f'Seen {seen_posts} ({valid_posts} valid) in {time() - start_time} seconds')

      last_update = time()
  except Exception as e:
    print('Retrying', e)

    sleep(5)

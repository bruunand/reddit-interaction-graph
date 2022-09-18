import pandas as pd

df = pd.read_csv('dataset/subreddit_interactions.csv')

# 1. Normalize subreddits
df.source = df.source.str.lower()
df.destination = df.destination.str.lower()

# 2. Filter away self-references
df = df[~(df.source == df.destination)]

# 3. Filter away top 2% users by amount of posts
user_post_count = df.user.value_counts()
top_users = set(user_post_count.keys()[:int(0.02 * len(user_post_count))])
df = df[~df.user.isin(top_users)]

# Count interactions between subreddits
df['pair'] = list(zip(df.source, df.destination))
df.pair = df.pair.apply(lambda pair: tuple(sorted(pair)))

# Count interactions between subreddits, including the type of interaction
df['triple'] = list(zip(df.pair, df.relationship))
triple_counts = dict(df.triple.value_counts())
df['interactions'] = df.triple.apply(lambda triple: triple_counts.get(triple))

# 5. Remove single-interaction subreddits
single_interaction_pairs = {pair for pair, counts in dict(df.pair.value_counts()).items() if counts == 1}
df = df[~df.pair.isin(single_interaction_pairs)]

# 6. Merge interactions by removing duplicates
df = df.drop_duplicates(subset=['source', 'destination', 'relationship'])

# Write subreddits/nodes
all_subreddits = list(set(df.source).union(df.destination))
subreddit_frame = pd.DataFrame({'subreddit': all_subreddits})
subreddit_frame.to_csv('dataset/subreddits.csv', index=False)

# Write interactions/relationships
df.to_csv('dataset/relationships.csv', index=False)

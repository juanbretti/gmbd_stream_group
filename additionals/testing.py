any(word in 'some one long two phrase three' for word in ['twso', 'longs'])
'some one long two phrase three'.split(' ') in ['two', 'long']

# %%

def check_positive_negative(tweet):
    positive = sum([words_positive.count(x) for x in tweet])
    negative = sum([words_positive.count(x) for x in tweet])
    # Check what is trending
    if (positive and not negative):
        res = 'Positive'
    elif (not positive and not negative):
        res = 'Neutral'
    else:
        res = 'Negative'
    return res

# %%
words_positive = ['a', 'b', 'c']
words_negative = ['d', 'e', 'f']

# %%

txt = 'f j k'
print(check_positive_negative(txt))

# %%

sum([words_positive.count(x) for x in txt])


# %%

def check_positive_negative(tweet):
    tweet = tweet.split(' ')
    positive = any(word in words_positive for word in tweet)
    negative = any(word in words_negative for word in tweet)
    # Check what is trending
    if (positive and not negative):
        res = 'Positive'
    elif (not positive and negative):
        res = 'Negative'
    elif (positive and negative):
        res = 'Dubious'
    else:
        res = 'Not enough information'
    return res


# %%
txt = 'f j k f f'.split(' ')
words_positive = ['a', 'b', 'c']
len([w for w in txt if w in words_positive])
# %%

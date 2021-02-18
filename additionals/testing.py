any(word in 'some one long two phrase three' for word in ['twso', 'longs'])
'some one long two phrase three'.split(' ') in ['two', 'long']

# %%

def check_positive_negative(tweet):
    positive = any(word in words_positive for word in tweet)
    negative = any(word in words_negative for word in tweet)
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
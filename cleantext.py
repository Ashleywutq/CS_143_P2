#!/usr/bin/env python

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse

import json

__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.
def fun_bigrams(texts):
    list_word = texts.split(' ')
    out = ''
    for i in range(len(list_word)-1):
        if i!=0:
            out+= ' '
        out += list_word[i]+'_'+list_word[i+1]
    return out

def fun_trigrams(texts):
    list_word = texts.split(' ')
    out = ''
    for i in range(len(list_word)-2):
        if i!=0:
            out+= ' '
        out += list_word[i]+'_'+list_word[i+1]+'_'+list_word[i+2]
    return out

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    # YOUR CODE GOES BELOW:

    #8.convert all the text into lowercase
    lower_case = text.lower()
    #1.replace newline and tab into single space
    replace_lower = lower_case.replace('\n', ' ').replace('\t', ' ')
    #2.remove url
    remove_url0 = re.sub(r'\[(.*)\]\(https?:\/\/.*\)', r'\1', replace_lower, flags=re.MULTILINE)
    remove_url1 = re.sub(r'https?:\/\/\S+', '', remove_url0, flags=re.MULTILINE)
    remove_url = re.sub(r'www\.\S+', '', remove_url1, flags=re.MULTILINE)
    removeline = re.sub(r'\\', '', remove_url)
    #6. seperate external punctuation
    remove_punc0 = re.sub(r'([a-zA-Z0-9]+[^\s]*[a-zA-Z0-9]+)', r' \1 ', removeline, flags=re.MULTILINE)
    word_list = remove_punc0.split(' ')
    remove_punc = []
    for word in word_list:
        if re.match(r'([a-zA-Z0-9]+[^\s]*[a-zA-Z0-9]+)',word) == None:
            word = re.sub(r'([^a-zA-Z0-9 ])', r' \1 ',word)
        remove_punc.append(word)
    remove_out = ' '.join(remove_punc)
    #7 remove punctuations keep , . ! : ; ?
    remove_puncs = re.sub(r' [^,.!?:;a-zA-Z0-9 ] ', '', remove_out, flags=re.MULTILINE)
    #5.split text into single space TODO@@@remove head and tail space 
    split_text0 = re.sub(r' +',' ',remove_puncs,flags=re.MULTILINE)
    split_text1 = re.sub(r'^ *| *$', '', split_text0)
    #10.unigrams
    parsed_text = split_text1
    remove_all_puncs = re.sub(r' [,.!?:;]', '', split_text0, flags=re.MULTILINE)
    unigrams= re.sub(r'^ *| *$', '', remove_all_puncs)
    
    partition = re.split(r' [,.!?:;]', split_text0)
    bigrams = ''
    trigrams = ''
    for line in partition:
        clean_line = re.sub(r'^ *| *$', '', line)
        bigrams+=' '+fun_bigrams(clean_line)
        trigrams+=' '+fun_trigrams(clean_line)
    bigrams = re.sub(r' +',' ',bigrams,flags=re.MULTILINE)
    bigrams= re.sub(r'^ *| *$', '', bigrams)
    trigrams= re.sub(r' +',' ',trigrams,flags=re.MULTILINE)
    trigrams= re.sub(r'^ *| *$', '', trigrams)

    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.

    # rawdata = []
    # with open('sample.json') as f:
    #     for line in f:
    #         rawdata.append(json.loads(line)['body'])
        
    # for text in rawdata:
    #     print(sanitize(text))

    text ="hello, it's me "
    ans = sanitize(text)
    print(ans[0])
    print(ans[1])
    print(ans[2])
    print(ans[3])
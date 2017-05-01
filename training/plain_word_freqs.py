"""
Create the word frequencies file from a corpus composed of several files

Example usage: python plain_word_freqs ./corpus/*.txt freqs.txt
"""
from __future__ import unicode_literals

import sys
import codecs
import glob
from multiprocessing import Pool
from collections import Counter

import plac
from tqdm import tqdm

def count_words(fpath):
    """
    Count the words of each file
    """
    with codecs.open(fpath, encoding="utf8") as file:
        words = file.read().split()
        counter = Counter(words)
    return counter


def main(input_glob, out_loc, workers=4):
    """
    Main function
    """
    pool = Pool(processes=workers)
    counts = pool.map(count_words, tqdm(list(glob.glob(input_glob))))
    df_counts = Counter()
    word_counts = Counter()
    for wcount in tqdm(counts):
        df_counts.update(wcount.keys())
        word_counts.update(wcount)
    with codecs.open(out_loc, "w", encoding="utf8") as file:
        if sys.version_info[0] == 2:
            items = df_counts.iteritems()
        else:
            items = df_counts.items()

        for word, docf in items:
            file.write(u"{freq}\t{df}\t{word}\n".format(word=repr(word),
                                                        df=docf, freq=word_counts[word]))

if __name__ == "__main__":
    plac.call(main)

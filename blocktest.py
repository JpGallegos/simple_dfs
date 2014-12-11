#! /usr/bin/env python2
import os.path

fname = "README.html"
fp = open(fname, 'rb')
fsize = os.path.getsize(fname)
n = 5
blocksize = int(fsize/n)

blocks = []

for block in iter(lambda: fp.read(blocksize), ''):
	blocks.append(block)
# print fp.read(blocksize)
print len(blocks)

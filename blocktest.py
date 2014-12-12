#! /usr/bin/env python2
import os
import os.path
import uuid

fname = "README.html"
fp = open(fname, 'rb')
fsize = os.path.getsize(fname)
n = 5
blocksize = int(fsize/n)

datapath = "test/"
if not os.path.exists(datapath):
	os.mkdir(datapath)

blocks = []

for block in iter(lambda: fp.read(blocksize), ''):
	blocks.append(block)
# print fp.read(blocksize)

for block in blocks:
	blockid = str(uuid.uuid1())

	blockpath = datapath + fname + "/"

	if not os.path.exists(blockpath):
		os.mkdir(blockpath)

	fp = open(blockpath + blockid + ".part", 'wb')
	fp.write(block)

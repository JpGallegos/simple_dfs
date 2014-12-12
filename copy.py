###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path

from Packet import *

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

def fileblocks(n, fsize, fp):
	"""A Generator that divides the file into blocks of equal size."""
	blocksize = int(fsize/n)
	
	for block in iter(lambda: fp.read(blocksize), ''):
		yield block

def copyToDFS(address, fname, path):
	""" Contact the metadata server to ask to copy file fname,
	    get a list of data nodes. Open the file in path to read,
	    divide in blocks and send to the data nodes. 
	"""

	# Create a connection to the data server
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# Read file
	fp = open(fname, 'rb')
	fsize = os.path.getsize(fname)

	# Create a Put packet with the fname and the length of the data,
	# and sends it to the metadata server 
	p = Packet()

	packet = p.BuildPutPacket(path+fname, fsize)

	sock.connect(address)
	sock.sendall(p.getEncodedPacket())

	msg = sock.recv(1024)

	# If no error or file exists
	# Get the list of data nodes.
	# Divide the file in blocks
	# Send the blocks to the data servers
	if msg == "DUP":
		print "The file '%s' is already in the file system." % fname
		return
	elif msg == "OK":
		p.RefreshPacket()
		p.BuildGetPacket(fname)
		sock.sendall(p.getEncodedPacket())
		p.RefreshPacket()

		msg = sock.recv(1024)
		sock.close()

		if msg == "NFOUND":
			print "The file '%s' is not in the file system." % fname
			return
		else:
			p.DecodePacket(msg)
			metalist = p.getDataNodes()
			blocks = []

			n = len(metalist)

			if n == 0:
				print "There was an error getting the list of data nodes."
				return
			else:
				i = 0
				for block in fileblocks(n, fsize, fp):
					# send the block to the data node
					p.RefreshPacket()
					p.BuildPutPacket(fname, fsize)

					node = metalist[i]
					sock.connect(node[0], node[1])
					sock.sendall(p.getEncodedPacket())

					msg = sock.recv(1024)

					if msg != "OK":
						print "There was an error saving a block to the data node."
						return
					else:
						# send block, receive id and save it to forward it to metadata server
						p.RefreshPacket()
						p.DecodePacket(msg)
						

					i = (i+1)%n

	# Notify the metadata server where the blocks are saved.

	# Fill code

	sock.close()

	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
	    the file fname.  Get the data blocks from the data nodes.
	    Saves the data in path.
	"""

   	# Contact the metadata server to ask for information of fname

	# Fill code

	# If there is no error response Retreive the data blocks

	# Fill code

    	# Save the file
	
	# Fill code

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()

		copyToDFS((ip, port), to_path, from_path)



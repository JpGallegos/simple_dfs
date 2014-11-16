#! /usr/bin/env python
###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	List client for the DFS
#



import socket
import sys

from Packet import *

def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def client(ip, port):

	# Contacts the metadata server and ask for list of files.
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	try:
		p = Packet()
		p.BuildListPacket()

		sock.connect((ip, port))
		sock.sendall(p.getEncodedPacket())

		msg = sock.recv(1024)
		
		if msg != "NAK":
			p.DecodePacket(msg)
			for f, size in p.getFileArray():
				print f, size
		else:
			print "Not Acknowledged"

	finally:
		sock.close()

if __name__ == "__main__":

	if len(sys.argv) < 2:
		usage()

	ip = None
	port = None 
	server = sys.argv[1].split(":")
	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server) == 2:
		ip = server[0]
		port = int(server[1])

	if not ip:
		usage()

	client(ip, port)

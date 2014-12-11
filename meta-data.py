###############################################################################
#
# Filename: meta-data.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
# Please modify globals with appropiate info.

from mds_db import *
from Packet import *
import sys
import SocketServer

def usage():
	print """Usage: python %s <port, default=8000>""" % sys.argv[0] 
	sys.exit(0)

def printExeption(e):
	print "Exception %s occurred in handle_list" % type(e)

class MetadataTCPHandler(SocketServer.BaseRequestHandler):

	def handle_reg(self, db, p):
		"""Register a new client to the DFS  ACK if successfully REGISTERED
			NAK if problem, DUP if the IP and port already registered
		"""

		try:
			if db.AddDataNode(p.getAddr(), p.getPort()) != 0:# Fill condition:
				self.request.sendall("ACK") 
			else:
				self.request.sendall("DUP")
		except Exception as e:
			print printExeption(e)
			self.request.sendall("NAK")

	def handle_list(self, db):
		"""Get the file list from the database and send list to client"""
		try:
			# Fill code here
			response = Packet()
			response.BuildListResponse(db.GetFiles())
			self.request.sendall(response.getEncodedPacket()) 	
		except Exception as e:
			printExeption(e)
			self.request.sendall("NAK")	

	def handle_put(self, db, p):
		"""Insert new file into the database and send data nodes to save
		   the file.
		"""
	       
		# Fill code
		info = p.getFileInfo()

		if db.InsertFile(info[0], info[1]):
			# Fill code
			self.request.sendall("OK")
		else:
			self.request.sendall("DUP")
	
	def handle_get(self, db, p):
		"""Check if file is in database and return list of
			server nodes that contain the file.
		"""

		# Fill code to get the file name from packet and then 
		# get the fsize and array of metadata server

		if fsize:
			# Fill code

			self.request.sendall(p.getEncodedPacket())
		else:
			self.request.sendall("NFOUND")

	def handle_blocks(self, db, p):
		"""Add the data blocks to the file inode"""

		# Fill code to get file name and blocks from
		# packet
	
		# Fill code to add blocks to file inode
		pass

		
	def handle(self):

		# Establish a connection with the local database
		db = mds_db("dfs.db")
		db.Connect()

		# Define a packet object to decode packet messages
		p = Packet()

		# Receive a msg from the list, data-node, or copy clients
		msg = self.request.recv(1024)
		print msg, type(msg)
		
		# Decode the packet received
		p.DecodePacket(msg)
	

		# Extract the command part of the received packet
		cmd = p.getCommand()
		print "cmd: %s" % cmd

		# Invoke the proper action 
		if   cmd == "reg":
			# Registration client
			self.handle_reg(db, p)

		elif cmd == "list":
			# Client asking for a list of files
			# Fill code
			self.handle_list(db)
		
		elif cmd == "put":
			# Client asking for servers to put data
			# Fill code
			self.handle_put(db, p)
		elif cmd == "get":
			# Client asking for servers to get data
			# Fill code
			self.handle_get(db, p)
		elif cmd == "dblks":
			# Client sending data blocks for file
			# Fill code
			self.handle_blocks(db, p)

		db.Close()

if __name__ == "__main__":
    HOST, PORT = "", 8000

    if len(sys.argv) > 1:
    	try:
    		PORT = int(sys.argv[1])
    	except:
    		usage()

    server = SocketServer.TCPServer((HOST, PORT), MetadataTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    print "Meta-data server is up"
    server.serve_forever()

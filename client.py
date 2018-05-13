import daemon, os, time, sys, signal, lockfile, daemon.pidfile, socket, socketserver, json, logging, random,datetime, threading
from collections import defaultdict
from multiprocessing import Process, Queue

HOST, PORT = "localhost", 5050 ##adresse und port vom server-server




def client(ip, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((ip, port))
        #### SEND DATA #####
        length = len(message)
        sock.sendall(bytes(str(length), 'utf8'))
        response = str(sock.recv(10), 'utf8')
        if response == 'ok':
        	print ('ja')
        sock.sendall(bytes(message, 'utf8'))
        #### SEND DATA #####
        
        ###Bekome data #####
        count = str(sock.recv(10).strip(), 'utf8')
        print("Received count: {}".format(count))
        sock.sendall(bytes('ok', 'utf8'))
        
        response = str(sock.recv(int(count)), 'utf8')
        print("Received count: {}".format(response))
        ###Bekome data #####
        sock.close()

a = 0

while True:
	

	data_server_prozess = Process(target=client, args=(HOST, PORT, json.dumps({'funktion':'hier client: '}))) #Starte Dataserver
	data_server_prozess.start()
	
	a = a + 1
	time.sleep(0.05)
	if a == 10:
		break

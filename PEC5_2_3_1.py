from time import sleep
import socket
import json
from opensky_api import OpenSkyApi

HOST = 'localhost'  # hostname o IP address
PORT = 20036        # puerto socket server

api = OpenSkyApi()
states = api.get_states(bbox=(36.173357, 44.024422,-10.137019, 1.736138))

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1)
while True:
    print('\Esperando un cliente a',HOST , PORT)
    conn, addr = s.accept()
    print('\Conectado a', addr)
    try:
        while(True):
            v = {}
            for vuelo in states.states:
                vuelo_dict = {
                'callsign':vuelo.callsign,
                'country': vuelo.origin_country,
                'longitude': vuelo.longitude,
                'latitude': vuelo.latitude,
                'velocity': vuelo.velocity,
                'vertical_rate': vuelo.vertical_rate,
                }
                v = json.dumps(vuelo_dict, indent=None).encode('utf-8')
                print(v)
                conn.send(v)
                conn.send(b'\n')
            sleep(10)       
    except socket.error:
        print ('Error .\n\nCliente desconectado.\n')
conn.close()
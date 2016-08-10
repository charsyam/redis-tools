import redis
import sys
import json

def get_client_lists(host, port):
    conn = redis.StrictRedis(host, port)
    return conn.client_list()

def get_client_ips(clients):
    ips = {}
    for client in clients:
        addr = client['addr']
        ip = addr.split(':')[0]
        if ip in ips:
            ips[ip] += 1
        else:
            ips[ip] = 1

    return ips


HOST = '127.0.0.1'
PORT = 6379

if len(sys.argv) > 1:
    HOST = sys.argv[1]

if len(sys.argv) > 2:
    PORT = int(sys.argv[2])

if __name__ == '__main__':
    clients = get_client_lists(HOST, PORT)
    ips = get_client_ips(clients)
    for ip in ips:
        print("IP : %s(count: %s)"%(ip, ips[ip]))

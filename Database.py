import socket
import pickle
import json
import os

profile = {}
dir_path = './'
BLOCK = 1

def main():
    global profile
    global dir_path
    dir_path = os.getcwd() + '/'
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        host = "127.0.0.11"
        port = 8086
        s.bind((host, port))
        s.listen(5)
        s.setblocking(BLOCK)

        try:
            with open(dir_path + 'profile.json') as fp:
                profile = json.load(fp)
                print(profile)
        except:
            print('User profile initialize failed.')
            exit(1)

        print("Database server starts at {}:{}".format(host, port))

        while True:
            print("Waiting for connection...")
            sock, address = s.accept()
            print("Receive connection from {}".format(address))

            status = False

            times = 5

            while not status and times > 0:
                times -= 1

                data = sock.recv(1024)
                if not data:
                    break
                data = pickle.loads(data)


                if data['method'] == 'authentication':
                    username = data['payload']['username']
                    password = data['payload']['password']
                    try:
                        profile['user'][username]
                    except:
                        payload = pickle.dumps("")
                        sock.send(payload)
                        continue

                    # Auth
                    if password == profile['user'][username]['password']:
                        payload = pickle.dumps("0")
                        sock.send(payload)
                        status = True
                    else:
                        payload = pickle.dumps("1")
                        sock.send(payload)
                        continue

                    
                else:
                    print("Unknown wrong!")
                    sock.close()
                    continue
            
            if not status:
                print("A connection is closed. [Tried too may times or abandoned by client]")
                sock.close()
                continue
            
            # Comments for variable "priority"
            # Bits: 1111
            # The highest 1 for manage database
            # The second 1 for manage user
            # The third 1 for write database
            # The lowest 1 for read database
            database_name = ""
            user_name = data['payload']['username']
            method = ['open', 'close']
            priority = profile['user'][user_name]['priority']
            if priority & 8:
                method.append('createdb')
                method.append('deletedb')
            if priority & 4:
                method.append('createuser')
                method.append('deleteuser')
            if priority & 2:
                method.append('put')
                method.append('delete')
            if priority & 1:
                method.append('get')

            while status:
                recv = sock.recv(1024)
                if not recv:
                    print("A connection is closed. [Abandoned by client]")
                    sock.close()
                    break
                recv = pickle.loads(recv)

                # TODO:
                # handle methods
                if recv['method'] in method:
                    hd = handler(sock, recv, database_name, user_name)
                    hd.handle()
                    database_name = hd.get_dbname()
                    user_name = hd.get_usrname()
                    if not user_name:
                        status = False
                    if not BLOCK:
                        with open(dir_path + 'profile.json', 'w') as fp:
                            fp.writelines(profile)
                else:
                    # Permission denied
                    if recv['method'] == 'get':
                        resp = {
                            'status': "1",
                            'payload': ""
                        }
                        resp = pickle.dumps(resp)
                        sock.send(resp)
                    else:
                        resp = pickle.dumps("1")
                        sock.send(resp)

            with open(dir_path + 'profile.json', 'w') as fp:
                json.dump(profile, fp)
            sock.close()


class handler():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data = {}
    dbname = ""
    usrname = ""
    global profile

    def __init__(self, s, recv, database_name, user_name):
        assert (self.sock.family == s.family)
        assert ('method' in recv.keys())
        self.sock = s
        self.data = recv
        self.dbname = database_name
        self.usrname = user_name

    def get_dbname(self):
        return self.dbname

    def get_usrname(self):
        return self.usrname

    def handle(self):
        resp = "3"
        if self.data['method'] == 'createdb': 
            dname = self.data['payload']
            if dname in profile.keys(): 
                resp = "2"
            else:
                profile[dname] = {}
                resp = "0"
        elif self.data['method'] == 'deletedb':
            dname = self.data['payload']
            if dname in profile.keys():
                profile.pop(dname)
                if self.dbname == dname:
                    self.dbname = ""
                resp = "0"
            else:
                resp = "2"
        elif self.data['method'] == 'createuser':
            uname = self.data['payload']['username']
            pwd = self.data['payload']['password']
            if uname in profile['user'].keys():
                resp = "2"
            else:
                profile['user'][uname] = {
                    "username": uname,
                    "password": pwd,
                    "priority": 3
                }
                resp = "0"
        elif self.data['method'] == 'deleteuser':
            uname = self.data['payload']
            if uname == 'root':
                resp = "1"
            elif uname in profile['user'].keys():
                profile['user'].pop(uname)
                if self.usrname == uname:
                    self.usrname = ""
                resp = "0"
            else:
                resp = "2"
        elif self.data['method'] == 'open':
            dname = self.data['payload']
            if dname in profile.keys():
                self.dbname = dname
                resp = dname
            else:
                resp = ""
        elif self.data['method'] == 'get':
            if not self.dbname:
                resp = {
                    'status': "3",
                    'payload': ""
                }
            else:
                key = self.data['payload']
                if key in profile[self.dbname].keys():
                    resp = {
                        'status': "0",
                        'payload': profile[self.dbname][key]
                    }
                else:
                    resp = {
                        'status': "2",
                        'payload': ""
                    }
        elif self.data['method'] == 'put':
            if not self.dbname:
                resp = "2"
            else:
                pair = self.data['payload']
                profile[self.dbname].update(pair)
                resp = "0"
        elif self.data['method'] == 'delete':
            if not self.dbname:
                resp = "2"
            else:
                key = self.data['payload']
                if key in profile[self.dbname].keys():
                    profile[self.dbname].pop(key)
                    resp = "0"
                else:
                    resp = "2"
        elif self.data['method'] == 'close':
            if self.dbname:
                self.dbname = ""
                resp = ""
            else:
                resp = "1"

        if resp == "3":
            print("Unknown error!")
        resp = pickle.dumps(resp)
        self.sock.send(resp)

if __name__ == '__main__':
    main()
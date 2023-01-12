import socket
import getpass
import hashlib
import pickle
host = "127.0.0.11"
port = 8086
method = ['createdb', 'deletedb', 'createuser', 'deleteuser', 'open', 'get', 'put', 'delete', 'close']

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
    except:
        print("Connect to server failed.")
        exit(1)

    # Authentication
    status = False
    while not status:
        username = input("username: ")
        password = hashlib.md5(getpass.getpass("password: ").encode()).hexdigest()

        query = {
            'method': 'authentication',
            'payload': {
                'username': username,
                'password': password
            }
        }
        query = pickle.dumps(query)
        s.send(query)

        auth = s.recv(1024)
        if not auth:
            print("Connection closed by server.")
            exit(1)
        err = pickle.loads(auth)
        if err == "0":
            print("Login database as user {}.".format(username))
            status = True
        else: # err == "1"
            print("Username or password wrong!")

    while True:
        operation = input("{username}@{address} > ".format(username=username, address=host))
        if operation.find("help") != -1:
            print("Usage: ")
            print('createdb database_name: Create a new database.')
            print('deletedb database_name: Delete a database.')
            print('createuser username: Create a new user.')
            print('deleteuser username: Delete a user.')
            print('open [database_name]: Open a database "database_name".')
            print('get [key]: Get the value of key.')
            print('put [key] value: Create a dict {key: value}.')
            print('delete [key]: Delete key and key\'s value from database.')
            print('close: Close the database.')
            print('exit: exit with no error.')
            continue
        command = operation.split(' ')
        if command[0] in method or command[0] == 'exit':
            req = api(s)
            if command[0] == 'createdb':
                try:
                    command[1]
                except:
                    print("Parameter wrong!")
                    continue
                err = req.createdb(command[1])
                if err == 0:
                    print("Successfully create database {}".format(command[1]))
                elif err == 1:
                    print("Failed to create database. Permission denied.")
                else: # err == 2
                    print("Failed to create database. Database exists.")
            elif command[0] == 'deletedb':
                try:
                    command[1]
                except:
                    print("Parameter wrong!")
                    continue
                err = req.deletedb(command[1])
                if err == 0:
                    print("Successfully delete database {}.".format(command[1]))
                elif err == 1:
                    print("Failed to delete database. Permission denied.")
                else: # err == 2
                    print("Failed to delete database. Database does not exist.")
            elif command[0] == 'createuser':
                try:
                    command[1]
                except:
                    print("Parameter wrong!")
                    continue
                pwd = hashlib.md5(getpass.getpass("password: ").encode()).hexdigest()
                err = req.createuser(command[1], pwd)
                if err == 0:
                    print("Successfully create user {}.".format(command[1]))
                elif err == 1:
                    print("Failed to create user. Permission denied.")
                else: # err == 2
                    print("Failed to create user. User exists.")
            elif command[0] == 'deleteuser':
                try:
                    command[1]
                except:
                    print("Parameter wrong!")
                    continue
                err = req.deleteuser(command[1])
                if err == 0:
                    print("Successfully delete user {}.".format(command[1]))
                elif err == 1:
                    print("Failed to delete user. Permission denied.")
                else: # err == 2
                    print("Failed to delete user. User does not exist.")
            elif command[0] == 'open':
                try:
                    command[1]
                except:
                    print("Parameter wrong!")
                    continue
                dbname = req.open(command[1])
                if dbname:
                    print("Successfully open database {}.".format(dbname))
                else:
                    print("Failed to open.")
            elif command[0] == 'get':
                try:
                    command[1]
                except:
                    print("Parameter wrong!")
                    continue
                data = req.get(command[1])
                if data['status'] == "0":
                    print("Successfully get. The value of key [{}] is [{}].".format(command[1], data['payload']))
                elif data['status'] == "2":
                    print("Failed. Key [{}] not found.".format(command[1]))
                elif data['status'] == "3":
                    print("Failed. Can only get a value from an opened database. No database is opened now.")
                else: # data['status'] == "1"
                    print("Failed to get. Permission denied.")
            elif command[0] == 'put':
                try:
                    command[1]
                    command[2]
                except:
                    print("Parameter wrong!")
                    continue
                key = command[1]
                value = command[2]
                err = req.put(key, value)
                if err == 1:
                    print("Failed to put. Permission denied.")
                elif err == 0: 
                    print("Successfully put.")
                else: # err == 2
                    print("Failed to put. Pairs can only be put into an opened database. No database is opened now.")
            elif command[0] == 'delete':
                try:
                    command[1]
                except:
                    print("Parameter wrong!")
                    continue
                key = command[1]
                err = req.delete(key)
                if err == 1:
                    print("Failed to delete. Permission denied.")
                elif err == 0:
                    print("Successfully delete.")
                else: # err == 2
                    print("Failed to delete. Key not found or database not opened.")
            elif command[0] == 'close':
                err = req.close()
                if err:
                    print("Failed to close. Database not opened.")
                else:
                    print("Successfully close.")
            elif command[0] == 'exit':
                s.close()
                print("Connection closed.")
                exit(0)
        else:
            print('Unknown command. Please try again. Type "help" for more information.')

class api():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def __init__(self, s):
        assert (self.sock.family == s.family)
        self.sock = s

    def createdb(self, database_name):
        query = {
            'method': 'createdb',
            'payload': database_name
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        if recv == "0":
            return 0
        elif recv == "1":
            return 1
        else: # "2"
            return 2

    def deletedb(self, database_name):
        query = {
            'method': 'deletedb',
            'payload': database_name
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        if recv == "0":
            return 0
        elif recv == "1":
            return 1
        else: # "2"
            return 2

    def createuser(self, username, password):
        query = {
            'method': 'createuser',
            'payload': {
                'username': username,
                'password': password
            }
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        if recv == "0":
            return 0
        elif recv == "1":
            return 1
        else: # "2"
            return 2

    def deleteuser(self, username):
        query = {
            'method': 'deleteuser',
            'payload': username
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        if recv == "0":
            return 0
        elif recv == "1":
            return 1
        else: # "2"
            return 2

    def open(self, database_name):
        query = {
            'method': 'open',
            'payload': database_name
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        return recv


    def get(self, key):
        query = {
            'method': 'get',
            'payload': key
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        return recv

    def put(self, key, value):
        query = {
            'method': 'put',
            'payload': {key: value}
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        if recv == "0":
            return 0
        elif recv == "1":
            return 1
        else: # "2"
            return 2

    def delete(self, key):
        query = {
            'method': 'delete',
            'payload': key
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        if recv == "0":
            return 0
        elif recv == "1":
            return 1
        else: # "2"
            return 2

    def close(self):
        query = {
            'method': 'close'
        }
        query = pickle.dumps(query)
        self.sock.send(query)

        recv = self.sock.recv(1024)
        if not recv:
            self.sock.close()
            print("Connection lost.")
            exit(0)
        recv = pickle.loads(recv)

        return recv



if __name__ == '__main__':
    main()
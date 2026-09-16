import uuid
import socket
import threading
import json
import time
import sys
from datetime import datetime

class Message:
    def __init__(self, uuid_obj, flag):
        self.uuid = uuid_obj
        self.flag = flag

    def to_json(self):
        return json.dumps({
            'uuid': str(self.uuid),
            'flag': self.flag
        })

    @staticmethod
    def from_json(json_str):
        data = json.loads(json_str)
        return Message(uuid.UUID(data['uuid']), data['flag'])

class Node:
    def __init__(self, config_file, log_file):
        self.config_file = config_file
        self.log_file = log_file
        self.node_id = uuid.uuid4()
        self.leader_id = None
        self.state = 0  # 0: finding leader, 1: leader found
        self.active = True

        # read config
        with open(config_file, 'r') as f:
            lines = f.readlines()
            self.server_addr = lines[0].strip().split(',')
            self.server_ip = self.server_addr[0]
            self.server_port = int(self.server_addr[1])
            self.client_addr = lines[1].strip().split(',')
            self.client_ip = self.client_addr[0]
            self.client_port = int(self.client_addr[1])

        self.server_socket = None
        self.client_socket = None
        self.incoming_conn = None
        self.lock = threading.Lock()
        self.server_ready = threading.Event()
        self.client_ready = threading.Event()

        # log initial id
        self._log(f"process started with id={self.node_id}")

    def _log(self, msg):
        with open(self.log_file, 'a') as f:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            f.write(f"[{timestamp}] {msg}\n")

    def _compare_uuid(self, other_uuid):
        if self.node_id > other_uuid:
            return "greater"
        elif self.node_id < other_uuid:
            return "less"
        else:
            return "same"

    def start_server(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.server_ip, self.server_port))
            self.server_socket.listen(1)
            self._log(f"server listening on {self.server_ip}:{self.server_port}")

            conn, addr = self.server_socket.accept()
            self._log(f"accepted connection from {addr}")
            self.incoming_conn = conn
            self.server_ready.set()
        except Exception as e:
            self._log(f"server error: {e}")

    def start_client(self):
        try:
            # wait for server to be ready
            time.sleep(2)
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._log(f"connecting to {self.client_ip}:{self.client_port}")
            self.client_socket.connect((self.client_ip, self.client_port))
            self._log(f"connected to server at {self.client_ip}:{self.client_port}")

            # send initial message with own uuid
            init_msg = Message(self.node_id, 0)
            self.client_socket.send(init_msg.to_json().encode())
            self._log(f"Sent: uuid={self.node_id}, flag=0")
            self.client_ready.set()
        except Exception as e:
            self._log(f"client error: {e}")

    def handle_incoming(self):
        try:
            while self.active:
                try:
                    # set timeout to avoid blocking forever
                    self.incoming_conn.settimeout(1.0)
                    data = self.incoming_conn.recv(1024)
                    if not data:
                        break

                    msg = Message.from_json(data.decode())
                    comparison = self._compare_uuid(msg.uuid)

                    with self.lock:
                        if msg.flag == 1:
                            # leader announcement message
                            if msg.uuid == self.node_id:
                                # our own announcement went all the way around, we're done
                                self._log(f"Received: uuid={msg.uuid}, flag=1, {comparison}, 1, leader_id={self.leader_id}")
                                self.active = False
                            else:
                                if self.state == 0:
                                    self.leader_id = msg.uuid
                                    self.state = 1
                                self._log(f"Received: uuid={msg.uuid}, flag=1, {comparison}, 1, leader_id={self.leader_id}")
                                # pass the announcement along and we are done, since
                                # we already know the leader and have nothing left to say
                                self.client_socket.send(msg.to_json().encode())
                                self._log(f"Sent: uuid={msg.uuid}, flag=1")
                                self.active = False
                        elif self.state == 1:
                            # leader already decided, this is a stray election message
                            # that was still in flight, just drop it
                            self._log(f"Received: uuid={msg.uuid}, flag=0, {comparison}, 1, leader_id={self.leader_id}")
                            self._log(f"Ignored: uuid={msg.uuid}, flag=0 (leader already decided)")
                        else:
                            self._log(f"Received: uuid={msg.uuid}, flag=0, {comparison}, 0")

                            if msg.uuid > self.node_id:
                                # a stronger candidate, keep passing it along
                                self.client_socket.send(msg.to_json().encode())
                                self._log(f"Sent: uuid={msg.uuid}, flag=0")
                            elif msg.uuid == self.node_id:
                                # our own id made it all the way around, we are the leader
                                self.leader_id = self.node_id
                                self.state = 1
                                self._log(f"Leader is decided to {self.leader_id}.")
                                leader_msg = Message(self.node_id, 1)
                                self.client_socket.send(leader_msg.to_json().encode())
                                self._log(f"Sent: uuid={self.node_id}, flag=1")
                            else:
                                # weaker candidate, drop it
                                self._log(f"Ignored: uuid={msg.uuid}, flag=0 (smaller uuid)")
                except socket.timeout:
                    continue
        except Exception as e:
            self._log(f"incoming handler error: {e}")

    def run(self):
        self._log(f"id={self.node_id}")

        # start server thread
        server_thread = threading.Thread(target=self.start_server, daemon=True)
        server_thread.start()

        # start client thread
        client_thread = threading.Thread(target=self.start_client, daemon=True)
        client_thread.start()

        # wait for both connections to be established before reacting to messages
        self.server_ready.wait()
        self.client_ready.wait()

        # handle incoming messages
        self.handle_incoming()

        self._log(f"process terminated")

def main():
    if len(sys.argv) != 3:
        print("Usage: python myleprocess.py <config_file> <log_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    log_file = sys.argv[2]

    node = Node(config_file, log_file)
    node.run()

if __name__ == "__main__":
    main()

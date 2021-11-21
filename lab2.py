#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  8 00:19:24 2021

@author: marwa saleh

Note: Extra credit is done for probe message
"""

import pickle
import socket
import sys
from datetime import datetime

import asyncio
import time
import random
import threading
from threading import Thread, Lock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import functools  # at the top with the other imports
import types

class ElectManager:
    def __init__(self, my_process_id):
        self.elect_mode = True
        self.cond = threading.Condition()
        self.probe_cond = threading.Condition()
        self.mutex = Lock()
        self.process_id = my_process_id
        self.members = {}
        self.executor = ThreadPoolExecutor(100)
        self.bully_mutex = Lock()
        self.bully = None

        self.loop = asyncio.get_event_loop()


    def update_members(self, more_members):
        self.members.update(more_members)


    def update_bully(self, bully):
        self.bully = bully
        log_server("Leader is: ", bully)

        self.probe_cond.acquire()
        self.probe_cond.notify()
        self.probe_cond.release()


    def is_leader(self):
        return self.bully and compare_id(self.bully, self.process_id) == 0

    def set_elect_mode(self):
        self.cond.acquire()
        try:
            # Produce an item
            self.mutex.acquire()
            self.elect_mode = True
            self.mutex.release()

            # Notify that an item  has been produced
            self.cond.notifyAll()
        finally:
            # Releasing the lock after producing
            self.cond.release()

    def unset_elect_mode(self):
        self.mutex.acquire()
        self.elect_mode = False
        self.mutex.release()

    def wait_a_change(self) :
        print("Manager: Waiting for changing to elect mode")
        self.cond.acquire()
        self.cond.wait()
        self.cond.release()

    def wait_for_leader_change(self) :
        self.probe_cond.acquire()
        self.probe_cond.wait()
        self.probe_cond.release()



    async def elect_and_announce(self):
        is_leader = await self.elect()

        if (is_leader) :
            # FIXME open thread async and no need to wait.
            self.bully_mutex.acquire()
            self.bully = self.process_id
            log_success("I am the leader.")
            self.bully_mutex.release()

            tasks = []
            for member in self.members:
                if compare_id(member, self.process_id) != 0:
                    task = self.loop.run_in_executor(self.executor, self.send_announce_async, member)
                    tasks.append(task)

            if (len(tasks) > 0):
                await asyncio.wait(tasks)

        return is_leader

    # return true if I am a leader
    async def elect(self):

        tasks = []
        for member in self.members:
            if compare_id(member, self.process_id) > 0:
                task = self.loop.run_in_executor(self.executor, self.send_elect_async, member)
                tasks.append(task)
        results = []
        if(len(tasks) > 0):
            completed, pending = await asyncio.wait(tasks)
            results = [t.result() for t in completed]

        if (len(tasks) == 0 or not any(results)) :
            return True
        return False

    def send_elect_async(self, peer):
        response = self.connect_and_send(peer, ('ELECTION', self.members))
        return response == "OK"

    def send_announce_async(self, peer) :
        response = self.connect_and_send(peer, ('COORDINATOR', self.members))
        return True

    async def send_probe_async(self, peer):
        response = self.connect_and_send(peer, ('PROBE', self.members))
        log_client("send_prob_Async result", response)
        return response == "OK"

    def connect_and_send(self, peer, data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_sock:
            peer_sock.settimeout(1.5)
            try :
                peer_sock.connect(self.members[peer]) ## fixme try and catch
                return message(peer_sock, data)
            except :
                log_warn("Client Warning : Failed to connect or send to ", peer, " with address ", self.members[peer], " message", data)

        return None

class ActionFactory:

    def create(self, raw, manager):
        message, data = raw
        log_server("Server : Event type = ", message)
        if message == 'ELECTION' :
            return ElectAction(data, manager)
        elif message == 'COORDINATOR' :
            return CoordinatorAction(data, manager)
        elif message == 'PROBE' :
            return ProbeAction(data, manager)
        else :
            raise "Wrong message type"


class IMessageAction:

    def __init__(self, data, manager):
        self.members = data
        self.elect_manager = manager

    def execute(self):
        pass

class ElectAction(IMessageAction):

    def execute(self):
        log_server ("Start Election Action execution")

        self.elect_manager.update_members(self.members)
        log_server ("Finish update member")

        self.elect_manager.set_elect_mode()
        log_server ("Done Election Action execution")

        return "OK"

class ProbeAction(IMessageAction):

    def execute(self):
        log_server ("Start Probe Action execution")
        return "OK"

class CoordinatorAction(IMessageAction):
    def execute(self):

        log_server ("Server : Start Coordinator Action execution")

        self.elect_manager.update_members(self.members)

        bully = self.elect_manager.process_id
        for member in self.members:
            if compare_id(member, bully) > 0:
                bully = member

        self.elect_manager.update_bully(bully)

        self.elect_manager.unset_elect_mode()
        log_server ("Server : Done Coordinator Action execution")
        return None


class Server(Thread):
    def __init__(self, manager):
        Thread.__init__(self)
        self.daemon = True
        self.address, self.listener = self.init()
        self.selector =  selectors.DefaultSelector()
        self.actionFactory = ActionFactory()
        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor(1)
        self.elect_manager = manager
        self.start_failure_time = 0
        self.failure_length = 0
        
    def run(self):

        ## Register to the notification
        self.selector.register(self.listener, selectors.EVENT_READ)
        try:
            while True:
                events = self.selector.select() ## wait notification
                for key, mask in events:
                    if key.fileobj == self.listener:
                        self.accept_peer(key.fileobj)
                    elif mask & selectors.EVENT_READ:
                        self.receive_message(key, mask)
                    else:
                        self.send_message(key, mask)
        finally :
            self.selector.close()

    def receive_message(self, key, mask):
        sock = key.fileobj
        data = key.data
        recv_data = None
        self.fail_if_test(sock)

        try :
            recv_data = sock.recv(9999)  # Should be ready to read
        except:
            log_warn("Server failed to receive the data from ", data.addr)

        close = True
        if recv_data:
            ## FIX ME accumelate
            response = self.handle(pickle.loads(recv_data))
            if response :
                data.outb = pickle.dumps(response)
                close = False
        if close :
            try :
                self.selector.unregister(sock)
                log_server('Server: closing connection to', data.addr)
            except:
                log_warn("Server: [Receive-message]warning failed to unregister event ", data.addr)
            sock.close()

    def send_message(self, key, mask):
        sock = key.fileobj
        data = key.data

        self.fail_if_test(sock)

        if data.outb:
            try:
                sent = sock.send(data.outb)  # FIXME me if connection closed or dropped
                data.outb = data.outb[sent:]
            except:
                log_warn('Server: Failed to send data to ', data.addr)
                try :
                    sock.close()
                    self.selector.unregister(sock)
                except:
                    log_warn("Server: [Send-message] warning failed to unregister event ", data.addr)

    def accept_peer(self, sock):
        self.fail_if_test(sock)
        try:
            conn, addr = sock.accept()
            log_server('Server: accepted connection from', addr)
            conn.setblocking(False)
            data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
            self.selector.register(conn, events, data=data)
        except:
            pass

    def handle(self, obj):
        action = self.actionFactory.create(obj, self.elect_manager)
        return action.execute()

    def fail_if_test(self, sock):
        log_server("Checking failure test mode")
        fail_mode = False
        current_time = time.time() * 1000

        if (current_time < self.start_failure_time + self.failure_length) :
            fail_mode = True
        else:
            self.start_failure_time = current_time + random.randrange(0, 10000)
            self.failure_length = random.randrange(1000, 40000)
        fail_mode = False
        if (fail_mode) :
            log_fail("Closed Connection due to Failure Mode for testing")
            sock.close()

    @staticmethod
    def init():
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(('localhost', 0))
        listener.listen(100)
        listener.setblocking(False)
        log_server("##### Start Server #################")
        log_server("Socket name = ", listener.getsockname())
        log_server("#####################################")

        return listener.getsockname(), listener

class ProbeClient(Thread):
    def __init__(self, my_process_id, manager):
        Thread.__init__(self)
        self.name = "Probe-Client-Thread"
        self.process_id = my_process_id
        self.loop = asyncio.get_event_loop()
        self.elect_manager = manager

    def run(self):
        log_probe("############ Probe Client ################################")
        log_probe("############################################################")

        while True :
            if self.elect_manager.bully and not self.elect_manager.is_leader():
                is_alive = self.loop.run_until_complete(self.elect_manager.send_probe_async(self.elect_manager.bully))
                if not is_alive:
                    log_probe("Leader isn't a live, set elect mode")
                    self.elect_manager.set_elect_mode()
                log_probe("Client: Waiting to probe for seconds")
                time.sleep(3)
            else :
                log_probe("Probe: Waiting because I don't know who is leader or I am the leader")
                self.elect_manager.wait_for_leader_change()
                log_probe("Probe: Leader changed and I will probe again!")

    """
       #################################Class to perform the specified behavior for Lab 2. ################
    """

class Lab2(Thread):

    def __init__(self, gcd_address, my_process_id, server_address, manager):
        Thread.__init__(self)
        self.name = "Client-Thread"
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        self.process_id = my_process_id
        self.loop = asyncio.get_event_loop()
        self.address = server_address
        self.elect_manager = manager
        #self.daemon = True


    def run(self):
        log_client("############ Client Initiate ################################")
        log_client('Client: STARTING WORK for pid {} on {}'.format(self.process_id,self.address))
        log_client("############################################################")

        self.join_group()

        while True :
            log_client ("Client : elect_mode = ", self.elect_manager.elect_mode)
            if self.elect_manager.elect_mode:
                is_leader = self.loop.run_until_complete(self.elect_manager.elect_and_announce())
                self.elect_manager.unset_elect_mode()
            else :
                log_client("Client: Waiting the non election mode")
                self.elect_manager.wait_a_change()
                log_client("Client: Get Notification to start again")


    def join_group(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
                gcd.connect(self.gcd_address)
                self.elect_manager.update_members(message(gcd, ('JOIN', (self.process_id, self.address))))
        except:
            log_warn("Make sure gcd process is opened. run python3 gcd2.py 51510 &")
            exit(0)

def message(sock, send_data, buffer_size=9999):
    log_client("Client: Sending data  ", send_data)
    sock.sendall(pickle.dumps(send_data))
    log_success ("Client: Sent Succeeded")
    # FIX ME loop of receive data if >> buffer size
    received_data = sock.recv(buffer_size)
    if (len(received_data) == 0):
        log_success ("Client: Client Received an empty data")
        return None
    else :
        log_success ("Client: Received data sussesfully")

    return pickle.loads(received_data)


def compare_id(id1, id2):
    if (id1[0] == id2[0]):
        return id1[1] - id2[1]
    return id1[0] - id2[0]

def log_client(*message):
    log_console("CLIENT", message)

def log_success(*message):
    log_console("SUCCESS", message)

def log_server(*message):
    log_console("SERVER", message)

def log_warn(*message):
    log_console("WARN", message)

def log_probe(*message):
    log_console("PROBE", message)

def log_fail(*message):
    log_console("FAIL", message)

def log_console(log_type, *message):
    if (log_type == "SERVER") :
        print("\033[0;35m", datetime.now(), message, "\033[0;00m")
    elif (log_type == "SUCCESS") :
        print("\033[0;32m", datetime.now(), message, "\033[0;00m")
    elif(log_type == "WARN") :
        print("\033[0;33m", datetime.now(), message, "\033[0;00m")
    elif(log_type == "PROBE") :
        print("\033[0;34m", datetime.now(), message, "\033[0;00m")
    elif(log_type == "FAIL") :
        print("\033[0;31m", datetime.now(), message, "\033[0;00m")
    else :
        print(datetime.now()," ] ", message)



if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python lab2.py GCDHOST GCDPORT NEXTBIRTHDAY SUID")
        exit(1)

    next_birthday = datetime.strptime(sys.argv[3],'%Y-%m-%d')
    birthday = (next_birthday - datetime.now()).days
    su_id = int(sys.argv[4])
    process_id = (birthday, int(su_id))
    elect_manager = ElectManager(process_id)
    server = Server(elect_manager)
    server.start()
    print(server.address)
    lab2 = Lab2(sys.argv[1:3], process_id, server.address, elect_manager)
    lab2.start()
    probe_client = ProbeClient(process_id,elect_manager)
    probe_client.start()

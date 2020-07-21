import math
import time
import socket
from random import randint
from time import sleep
from threading import Thread
from collections import Counter
from ast import literal_eval

class Node:
    BUFFER_SIZE = 2048

    def __init__(self, id, ip, port, number_of_neighbors):
        self.id = id
        self.address = (ip, port)
        self.number_of_neighbors = number_of_neighbors
        self.neighbors = []
        self.send_times = {}
        self.receive_times = {}
        self.sent_packets = Counter()
        self.received_packets = Counter()
        self.last_received_packets = {}
        self.to_be_neighbors = []
        self.is_disabled = False
        self.network_nodes = None

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setblocking(0)
        self.udp_socket.bind(address=self.address)

    def _run_receiver(self):
        while True:
            message, address = self.udp_socket.recvfrom(BUFFER_SIZE)
            self.receive_times[address] = time.time()
            packet = HelloPacket.from_byte_string(message)


    def _run_sender(self):
        while True:
            sleep(2)
            self._send()

    def run(self):
        while(True):
            for nd in self.neighbors:
                self._send_to(nd)
            if len(self.neighbors) < self.number_of_neighbors:
                self._get_new_neighbors()
            received_packets = {}
            for nd in self.network_nodes:
                try:
                    message, address = self.udp_socket.recvfrom(self.BUFFER_SIZE)
                except Exception as e:
                    print(e)
                received_packets[address] = HelloPacket.from_byte_string(message)
            self._process_received_packets(received_packets)
            sleep(2)
        
    def _process_received_packets(self, received_packets):
        for nd in self.neighbors:
            if nd in received_packets.keys():
                self.receive_times[nd] = time.time()
                self.last_received_packets[nd] = received_packets[nd]
        if len(self.neighbors) < self.number_of_neighbors:
            for i, nd in enumerate(self.to_be_neighbors):
                if nd in received_packets.keys():
                    self.neighbors.append(self.to_be_neighbors[i])

    def _send_to(self, node):
        self.udp_socket.sendto(HelloPacket(
                self.id,
                self.address,
                self.neighbors,
                node,
                self.get_last_send_time_to(node),
                self.get_last_receive_time_from(node),
            ).get_byte_string(), node.address)
        self.send_times[node.address] = time.time()

    def _send(self):
        if self.is_disabled:
            return

        for node in self.neighbors:
            Thread(target=self._send_to, args=(node)).start()

    def get_last_send_time_to(self, receiver):
        return self.send_times.get(receiver.address, None)

    def get_last_receive_time_from(self, receiver):
        return self.receive_times.get(receiver.address, None)

    def disable(self):
        self.is_disabled = True

    def enable(self):
        self.is_disabled = False


class HelloPacket:
    def __init__(
        self, 
        sender_id, 
        sender_address, 
        sender_neighbors, 
        receiver, 
        last_packet_send_time, 
        last_packet_receive_time, 
        ):
        self.packet_type = 'hello'
        self.sender_id = sender_id
        self.sender_address = sender_address
        self.sender_neighbors = sender_neighbors
        self.last_packet_send_time = last_packet_send_time
        self.last_packet_receive_time = last_packet_receive_time

    def get_byte_string(self):
        return ';'.join(
            map(
                str, 
                [
                    self.packet_type,
                    self.sender_id,
                    self.sender_address,
                    self.sender_neighbors,
                    self.last_packet_send_time,
                    self.last_packet_receive_time,
                ],
            )
        ).encode()

    @staticmethod
    def from_byte_string(byte_string):
        data = byte_string.decode().split(';')
        return HelloPacket(
            sender_id=int(data[1]),
            sender_address=literal_eval(data[2]),
            sender_neighbors=literal_eval(data[3]),
            last_packet_send_time=float(data[4]),
            last_packet_receive_time=float(data[5])
        )


class Network:
    BASE_PORT = 16000

    def __init__(self, number_of_nodes, number_of_neighbors):
        self.number_of_nodes = number_of_nodes
        self.nodes = []
        for i in range(number_of_nodes):
            self.nodes.append(
                Node(
                    id=i,
                    ip='127.0.0.1',
                    port=Network.BASE_PORT + i,
                    number_of_neighbors=number_of_neighbors,
                )
            )
        for i, node in enumerate(self.nodes):
            node.network_nodes = [nd for nd in self.nodes if nd.id != i]

    def run(self):
        for node in self.nodes:
            node.run()

        Thread(target=self.random_disabler).start()

    def random_disabler(self):
        disabled_nodes_indices = []

        turn = 0
        while True:
            sleep(10)

            turn += 1
            if turn > 2 and len(disabled_nodes_indices) > 0:
                self.nodes[disabled_nodes_indices.pop(0)].enable()

            rand_index = randint(0, self.number_of_nodes - 1)
            while rand_index in disabled_nodes_indices:
                rand_index = randint(0, self.number_of_nodes - 1)

            self.nodes[rand_index].disable()
            disabled_nodes_indices.append(rand_index)


Network(
    number_of_nodes=6,
    number_of_neighbors=3
).run()

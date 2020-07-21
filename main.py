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

        self.random_strangers = []
        self.to_be_neighbors = []
        self.is_disabled = False
        self.network_nodes = None

        self.udp_socket = socket.socket(
            family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.udp_socket.setblocking(0)
        self.udp_socket.bind(self.address)

    def _get_new_neighbors(self):

        for address in self.random_strangers:
            self.to_be_neighbors.append(address)
            self._send_to_address(address)

        for i in range(self.number_of_neighbors - len(self.neighbors)):
            rand_node_index = randint(0, len(self.network_nodes) - 1)
            while self.network_nodes[rand_node_index].address in self.neighbors:
                rand_node_index = randint(0, len(self.network_nodes) - 1)

            rand_node = self.network_nodes[rand_node_index]
            self.to_be_neighbors.append(rand_node.address)
            self._send_to(rand_node)

    def run(self):
        while (True):
            # print(self.id, len(self.neighbors), len(
            #     self.to_be_neighbors), len(self.random_strangers))

            for address in self.neighbors:
                self._send_to_address(address)

            if len(self.neighbors) < self.number_of_neighbors:
                old_to_be_neighbors = self.to_be_neighbors
                self.to_be_neighbors = []
                self._get_new_neighbors()

            received_packets = {}
            for i in range(20):
                try:
                    message, address = self.udp_socket.recvfrom(
                        self.BUFFER_SIZE)
                    received_packets[address] = HelloPacket.from_byte_string(
                        message)
                except Exception as e:
                    pass

            print(self.id, received_packets)
            self._process_received_packets(
                old_to_be_neighbors, received_packets)

            sleep(2)

    def _process_received_packets(self, old_to_be_neighbors, received_packets):
        for address in self.neighbors:
            if address in received_packets.keys():
                self.receive_times[address] = time.time()
                self.last_received_packets[address] = received_packets[address]

        if len(self.neighbors) < self.number_of_neighbors:
            for address in old_to_be_neighbors:
                if address in received_packets.keys():
                    self.neighbors.append(address)

        if len(self.neighbors) < self.number_of_neighbors:
            for address in received_packets:
                if address not in self.neighbors:
                    self.random_strangers.append(address)

    def _send_to_address(self, address):
        print('_send_to_address', self.id, address)
        self.udp_socket.sendto(HelloPacket(
            self.id,
            self.address,
            self.neighbors,
            node,
            self.get_last_send_time_to(address),
            self.get_last_receive_time_from(address),
        ).get_byte_string(), address)
        self.send_times[address] = time.time()

    def _send_to(self, node):
        sent_bytes = self.udp_socket.sendto(HelloPacket(
            self.id,
            self.address,
            self.neighbors,
            node,
            self.get_last_send_time_to(node),
            self.get_last_receive_time_from(node),
        ).get_byte_string(), node.address)
        print('_send_to', self.id, node.address, sent_bytes)
        self.send_times[node.address] = time.time()

    def get_last_send_time_to_address(self, address):
        return self.send_times.get(address, None)

    def get_last_receive_time_from_address(self, address):
        return self.receive_times.get(address, None)

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
                    ip='localhost',
                    port=Network.BASE_PORT + i,
                    number_of_neighbors=number_of_neighbors,
                )
            )
        for node in self.nodes:
            node.network_nodes = [nd for nd in self.nodes if nd.id != node.id]

    def run(self):
        for node in self.nodes:
            Thread(target=node.run).start()

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

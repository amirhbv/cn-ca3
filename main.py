import math
import time
import socket
from random import randint
from time import sleep
from threading import Thread


class Node:
    BUFFER_SIZE = 1024

    def __init__(self, id, ip, port, number_of_neighbors):
        self.id = id
        self.address = (ip, port)
        self.number_of_neighbors = number_of_neighbors
        self.neighbors = []
        self.send_times = {}
        self.receive_times = {}
        self.is_disabled = False

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind(address=self.address)

    def _run_receiver(self):
        while True:
            message, address = self.udp_socket.recvfrom(BUFFER_SIZE)
            self.receive_times[address] = time.time()

    def _run_sender(self):
        while True:
            sleep(2)
            self._send()

    def run(self):
        Thread(target=node._run_sender).start()
        Thread(target=node._run_receiver).start()

    def _send_to(self, node):
        self.udp_socket.sendto(HelloPacket(
            self, node).get_byte_string(), node.address)
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
    def __init__(self, sender, receiver):
        self.packet_type = 'hello'
        self.sender_id = sender.id
        self.sender_address = sender.address
        self.sender_neighbors = sender.neighbors
        self.last_packet_send_time = sender.get_last_send_time_to(receiver)
        self.last_packet_receive_time = sender.get_last_receive_time_from(
            receiver)

    def get_byte_string(self):
        # TODO
        return self.packet_type.encode()


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
                    number_of_neighbors=number_of_neighbors
                )
            )

    def run(self):
        for node in self.nodes:
            node.run()

        Thread(target=self.random_disabler).start()

    def random_disabler(self):
        disabled_nodes_indices = []

        turn = 0
        while True:
            turn += 1
            sleep(10)

            if turn > 2 == 1 and len(disabled_nodes_indices) > 0:
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

import json
import math
import socket
import time
from ast import literal_eval
from collections import Counter, defaultdict
from random import choice, randint
from threading import Thread


class Node:
    BUFFER_SIZE = 2048

    def __init__(self, id, ip, port, number_of_neighbors):
        self.id = id
        self.address = (ip, port)

        self.number_of_neighbors = number_of_neighbors
        self.neighbors = []
        self.have_been_neighbors = set()

        self.send_times = {}
        self.receive_times = {}

        self.sent_packets = Counter()
        self.received_packets = Counter()

        self.neighbor_duration = Counter()
        self.neighbor_start_time = {}

        self.last_received_packets = {}

        self.random_strangers = []
        self.to_be_neighbors = []
        self.is_disabled = False
        self.is_stopped = False
        self.network_nodes = None
        self.nodes_to_be_connected = []

        self.udp_socket = socket.socket(
            family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.udp_socket.setblocking(0)
        self.udp_socket.bind(self.address)

    def run(self):
        now = time.time()
        while not self.is_stopped:
            if self.is_disabled:
                continue
            for address, _time in self.receive_times.items():
                if time.time() - _time > 8:
                    if address in self.neighbors:
                        self.neighbors.remove(address)
                        self.neighbor_duration[address] += time.time() - \
                            self.neighbor_start_time[address]
                    elif address in self.to_be_neighbors:
                        self.to_be_neighbors.remove(address)

            if time.time() - now > 2:
                print(self.id, self.is_disabled, len(self.neighbors),
                      self.neighbors, self.nodes_to_be_connected)
                now = time.time()

                for address in self.neighbors:
                    self._send_to_address(address)
                if len(self.neighbors) < self.number_of_neighbors:
                    for i in self.nodes_to_be_connected:
                        self._send_to_address(i[0])

            if (len(self.neighbors) < self.number_of_neighbors):
                # print(self.id, 'going new neighbors')
                self._get_new_neighbors()

            nodes_to_remove = []
            for i, nd in enumerate(self.nodes_to_be_connected):
                if time.time() - nd[1] > 8:
                    nodes_to_remove.append(nd)
            for i in nodes_to_remove:
                self.nodes_to_be_connected.remove(i)

            # for i in range(20):
            try:
                if randint(1, 100) <= 5:
                    continue
                message, address = self.udp_socket.recvfrom(100)
                packet = HelloPacket.from_byte_string(message)
                # print(self.id, received_packets)
                self._process_received_packet(packet)
                pass
            except Exception as e:
                pass
        for node in self.neighbors:
            self.neighbor_duration[node] += time.time() - \
                self.neighbor_start_time[node]

    def _get_new_neighbors(self):
        nds = [nd[0] for nd in self.nodes_to_be_connected]
        remaining = self.number_of_neighbors - len(self.neighbors)
        for i in range(remaining - len(self.nodes_to_be_connected)):
            address = None
            while address is None or address in self.neighbors or address in nds:
                if len(self.to_be_neighbors):
                    address = choice(self.to_be_neighbors)
                    self.to_be_neighbors.remove(address)
                else:
                    node = choice(self.network_nodes)
                    address = node.address
                    while address in self.neighbors:
                        node = choice(self.network_nodes)
                        address = node.address

            self.nodes_to_be_connected.append((address, time.time()))

    def _process_received_packet(self, packet):
        self.receive_times[packet.sender_address] = time.time()
        self.last_received_packets[packet.sender_address] = packet
        self.received_packets[packet.sender_address] += 1

        if packet.sender_address not in self.neighbors:
            if packet.sender_address in [nd[0] for nd in self.nodes_to_be_connected]:
                self.neighbors.append(packet.sender_address)
                self.have_been_neighbors.add(packet.sender_address)
                self.neighbor_start_time[packet.sender_address] = time.time()
            else:
                self.to_be_neighbors.append(packet.sender_address)

        # if len(self.neighbors) < self.number_of_neighbors:
        #     for address in old_to_be_neighbors:
        #         if address in received_packets.keys():
        #             self.neighbors.append(address)

        # if len(self.neighbors) < self.number_of_neighbors:
        #     for address in received_packets:
        #         if address not in self.neighbors:
        #             self.random_strangers.append(address)

    def get_neighbor_packets_count(self):
        return {
            ':'.join(map(str, address)): {
                'sent_packets_count': self.sent_packets[address],
                'received_packets_count': self.received_packets[address]
            } for address in self.have_been_neighbors
        }

    def get_accessibility_ratio(self):
        print(self.id, self.neighbor_duration)
        return {
            ':'.join(map(str, node.address)): self.neighbor_duration.get(node.address, 0) for node in self.network_nodes
        }

    def _send_to_address(self, address):
        if self.is_disabled:
            return
        self.sent_packets[address] += 1
        # print('_send_to_address', self.id, address)
        self.udp_socket.sendto(HelloPacket(
            self.id,
            self.address,
            self.neighbors,
            self.get_last_send_time_to_address(address),
            self.get_last_receive_time_from_address(address),
        ).get_byte_string(), address)
        self.send_times[address] = time.time()

    def get_last_send_time_to_address(self, address):
        return self.send_times.get(address, None)

    def get_last_receive_time_from_address(self, address):
        return self.receive_times.get(address, None)

    def get_last_send_time_to(self, receiver):
        return self.send_times.get(receiver.address, None)

    def get_last_receive_time_from(self, receiver):
        return self.receive_times.get(receiver.address, None)

    def disable(self):
        print('DISABLING', self.id)
        self.is_disabled = True

    def enable(self):
        print('ENABLING', self.id)
        self.is_disabled = False

    def stop(self):
        self.is_stopped = True

    def get_current_topology(self):
        bidirectionals, unidirectionals = defaultdict(list), defaultdict(list)

        for address in self.last_received_packets:
            last_packet = self.last_received_packets.get(address)
            last_packet_time = self.receive_times.get(address)
            if time.time() - last_packet_time < 8:
                print(last_packet.sender_neighbors)
                for neighbor_address in last_packet.sender_neighbors:
                    if last_packet.sender_address in unidirectionals[neighbor_address]:
                        bidirectionals[neighbor_address].append(
                            last_packet.sender_address)
                        bidirectionals[last_packet.sender_address].append(
                            neighbor_address)
                    else:
                        unidirectionals[last_packet.sender_address].append(
                            neighbor_address)
                print(unidirectionals, bidirectionals)

        for address in self.neighbors:
            if self.address in unidirectionals[address]:
                bidirectionals[self.address].append(address)
                bidirectionals[address].append(self.address)
            else:
                unidirectionals[self.address].append(address)

        all_network_nodes = sorted(
            [*self.network_nodes, self], key=lambda x: x.id)
        return [
            [
                2 if row_node.address in bidirectionals[
                    col_node.address] else 1 if row_node.address in unidirectionals[col_node.address] else 0
                for col_node in all_network_nodes
            ]
            for row_node in all_network_nodes
        ]


class HelloPacket:
    def __init__(
        self,
        sender_id,
        sender_address,
        sender_neighbors,
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
            last_packet_send_time=float(
                data[4]) if data[4] != 'None' else None,
            last_packet_receive_time=float(
                data[5]) if data[5] != 'None' else None,
        )


class Network:
    BASE_PORT = 11000

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
        for node in self.nodes:
            node.network_nodes = [nd for nd in self.nodes if nd.id != node.id]

        self.is_stopped = False

    def run(self, duration):
        for node in self.nodes:
            Thread(target=node.run).start()

        Thread(target=self.random_disabler).start()

        time.sleep(duration)

        for node in self.nodes:
            node.stop()
        self.is_stopped = True
        self.log(duration)

    def log(self, duration):
        final_log = {
            node.id: {
                'address': ':'.join(map(str, node.address)),
                'neighbor_packets_count': node.get_neighbor_packets_count(),
                'final_neighbors': node.neighbors,
                'accessibility_ratio': {addr: dur / duration for addr, dur in node.get_accessibility_ratio().items()},
                'topology': node.get_current_topology(),
            } for node in self.nodes
        }
        with open('log.json', 'w+') as f:
            json.dump(final_log, f)

    def random_disabler(self):
        disabled_nodes_indices = []

        turn = 0
        now = time.time()
        while not self.is_stopped:
            if time.time() - now > 10:
                now = time.time()

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
).run(30)

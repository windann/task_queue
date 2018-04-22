import socket

from storage import add_in_storage, find_id_in_queue, ack, get_from_queue


def run():
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    connection.bind(('127.0.0.1', 5555))
    connection.listen(10)

    while True:
        current_connection, address = connection.accept()

        while True:

            data = current_connection.recv(2048)

            data_list = data.split()
            command = data_list[0]
            queue = data_list[1].decode('utf-8')

            if command == b'ADD':

                length = data_list[2].decode('utf-8')
                task_data = data_list[3].decode('utf-8')

                current_connection.send(bytes(add_in_storage(queue, task_data, length)+ '\n', 'utf8'))

            elif command == b'GET':

                current_connection.send(get_from_queue(queue))

            elif command == b'ACK':

                id = data_list[2].decode('utf-8')
                current_connection.send(ack(queue, id))

            elif command == b'IN':

                id = data_list[2].decode('utf-8')
                current_connection.send(find_id_in_queue(queue, id))

            current_connection.shutdown(1)
            current_connection.close()
            break


if __name__ == '__main__':
    run()
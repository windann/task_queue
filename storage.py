import shelve
import datetime


class Task:

    id = 0

    def __init__(self, queue, length, task_data):
        self.length = length
        self.task_data = task_data
        self.process = False
        self.done = False
        self.get_time = None

        Task.id += 1
        self.id = '{}.{}'.format(queue,Task.id)


def add_in_storage(queue, task_data, length):

    tasks = []

    with shelve.open('data_storage') as d:
        task = Task(queue, length, task_data)

        if queue not in d.keys():
            tasks.append(task)
            d.update({queue: tasks})

        else:
            tasks = d[queue]
            tasks.append(task)
            d[queue] = tasks

    return bytes(str(task.id) + '\n', 'utf8')


def find_id_in_queue(queue, id):

    with shelve.open('data_storage') as d:
        id_list = [task.id for task in d[queue]]

        if queue in d.keys() and id in id_list:
            return b'YES'
        else:
            return b'NO'


def ack(queue, id):

    with shelve.open('data_storage') as d:

        id_list = [task.id for task in d[queue]]

        if queue in d.keys() and id in id_list:
            d[queue] = [task for task in d[queue] if task.id != id]

            return b'YES'
        else:
            return b'NO'


def get_from_queue(queue):
    with shelve.open('data_storage') as d:

        tasks = []

        if queue in d.keys() and len(d[queue]) != 0:

            for task in d[queue]:

                # для удобства проверки 5 минут заменено на 10 секунд
                #if task.get_time is not None and datetime.datetime.now() - task.get_time > datetime.timedelta(minutes=5):
                if task.get_time is not None and datetime.datetime.now() - task.get_time > datetime.timedelta(seconds=10):
                    task.process = False
                    task.get_time = None

                if task.process is True:
                    continue

                tasks.append(task)

            if len(tasks) != 0:
                get_task = tasks[0]
            else:
                return b'NONE'

            get_task.process = True
            get_task.get_time = datetime.datetime.now()

            d[queue] = tasks

            information = '{}\n {} {}'.format(get_task.id, get_task.length, get_task.task_data)

            return bytes(information, 'utf8')

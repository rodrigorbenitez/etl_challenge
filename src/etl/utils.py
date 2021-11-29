import datetime


def log(message):
    print(f'{datetime.datetime.now()} - {message}.')


def execution_time(start):
    log(f'Execution Time: {(datetime.datetime.now() - start).seconds} seconds')
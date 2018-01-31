import nsq
import tornado.ioloop
import time


def pub_message():
    writer.pub('test', time.strftime('%H:%M:%S'), finish_pub)


def finish_pub(conn, data):
    print(data)

writer = nsq.Writer(['127.0.0.1:4150'])
tornado.ioloop.PeriodicCallback(pub_message, 10).start()
nsq.run()

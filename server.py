import zerorpc
import logging
from analyze_meme import analyze_meme,test_com

logging.basicConfig()

class HelloRPC(object):
    def test_com(self, data):
        analyze_meme(data["name"])
        return "Hello, %s" % data["name"]


s = zerorpc.Server(HelloRPC())
s.bind("tcp://0.0.0.0:4242")
s.run()
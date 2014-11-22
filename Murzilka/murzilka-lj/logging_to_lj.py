__author__ = 'peterlink'

import tornado.ioloop
import urllib
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPResponse

username_file = open("lj_user_name", "r")
username = username_file.readline()
username_file.close()
username = urllib.pathname2url(username)

password_file = open("lj_user_pass", "r")
password = password_file.readline()
password_file.close()
password = urllib.pathname2url(password)

print "logging with %s and %s" % (username, password)

login_string = 'mode=login&user={}&password={}\r\n'.format(username,password)

login_to_LJ_request = HTTPRequest("http://www.livejournal.com/interface/flat", method = "POST", body=login_string)

def handle_request(response):
    if response.error:
        print "Error:", response.error
    else:
        print response.body


http_client = AsyncHTTPClient()
http_client.fetch(login_to_LJ_request, handle_request)
tornado.ioloop.IOLoop.instance().start()

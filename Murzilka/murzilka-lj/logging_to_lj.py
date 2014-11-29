__author__ = 'peterlink'

import tornado.ioloop
import urllib, hashlib
from tornado.httpclient import HTTPClient, HTTPRequest, HTTPResponse

LJ_API_address = "https://www.livejournal.com/interface/flat"

def load_LJ_username():
    username_file = open("lj_user_name", "r")
    username = username_file.readline()
    username_file.close()
    username = urllib.pathname2url(username)
    return username

def load_LJ_password():
    password_file = open("lj_user_pass", "r")
    password = password_file.readline()
    password_file.close()

    m = hashlib.md5()
    m.update(password)
    print m.hexdigest()
    hpassword = m.hexdigest()

    return hpassword

def login_to_LJ(username, hpassword):
    print "===logging with %s and %s" % (username, hpassword)

    login_string = 'mode=login&auth_method=clear&user={}&hpassword={}'.format(username, hpassword)

    login_to_LJ_request = HTTPRequest(LJ_API_address, method = "POST", body=login_string)

    response = http_client.fetch(login_to_LJ_request)
    if response.error:
        print "Error:", response.error
    else:
        print "===login OK"
#        print response.body

def test(username, hpassword):
    login_string = 'mode=getfriends&auth_method=clear&user={}&hpassword={}'.format(username, hpassword)
    print "===getting friends"
    test_request = HTTPRequest(LJ_API_address, method = "POST", body=login_string)

    response = http_client.fetch(test_request)
    if response.error:
        print "Error:", response.error
    else:
        print ""
        print response.body

def post(username, hpassword):
    login_string = 'mode=postevent&auth_method=clear&user={}&hpassword={}&event=test&subject=TEST&year=2014&mon=11&day=23&hour=21&min=43'.format(username, hpassword)
    print "===posting"
    test_request = HTTPRequest(LJ_API_address, method = "POST", body=login_string)

    response = http_client.fetch(test_request)
    if response.error:
        print "Error:", response.error
    else:
        print ""
        print response.body

http_client = HTTPClient()
username = load_LJ_username()
hpassword = load_LJ_password()

login_to_LJ(username, hpassword)
test(username, hpassword)
post(username, hpassword)

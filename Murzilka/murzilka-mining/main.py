import re, os, time, glob, psycopg2
from tornado.httpclient import HTTPClient, HTTPRequest, HTTPResponse, HTTPError

try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

print "analysis"
user_name = "fritzmorgen"

http_client = HTTPClient()

data_directory_path = "/home/peterlink/Development/Python/Murzilka/murzilka-parsing/data/{}_posts/*".format(user_name)
users_file_name = "all_users.txt"
users_file = None
users_set = set()

def load_users():
    global users_file
    users_file = open(users_file_name, "r")
    strings = users_file.readlines()
    for i in range(0, len(strings)):
        users_set.add(strings[i])
        i = i + 2
    users_file.close()
    pass

def extract_users_from_file(file_name):
    post_commentators_file = open(file_name, "r")
    result = []
    strings = post_commentators_file.readlines()
    i = 0
    for string in strings:
        if i % 2 == 0:
            result.append(strings[i].rstrip())
        i += 1

    return result
class user_info:
    def __init__(self, name, created, posts, comments_in, comments_out, friends_in):
        self.name = name
        self.registration_date = created
        self.posts_count = posts
        selt.comments_in = comments_in
        self.comments_out = comments_out
        self.friends_in = friends_in


def get_user_info(user):
    journal_profile_pattern = "http://{}.livejournal.com/profile"
    journal_posts_total_beginning = "<div class=\"b-profile-stat-value\">"
    journal_created_beginning = "<span>Created on"
    journal_created_pattern = re.compile("\d{1,2} (January|February|March|April|May|June|July|August|September|October|November|December) \d{4}")
    journal_posts_total_pattern = re.compile("\d{1,10}")
    profile_request = HTTPRequest(journal_profile_pattern.format(user), method = "GET")
    try:
        journal_profile_page = http_client.fetch(profile_request)
        page_to_parse = BeautifulSoup(journal_profile_page.body)

        created = None
        total_posts_from_profile = 0
        total_comments_in = 0
        total_comments_out = 0

        for span_tag in page_to_parse.findAll("span", {"class" : "b-account-level"}):
            date_string = span_tag.contents[3].contents[0].lstrip()
            date_string = re.search(journal_created_pattern, str(date_string)).group(0)
            created = time.strptime(date_string, "%d %B %Y")

        for li_tag in page_to_parse.findAll("li", {"class" : "b-profile-stat-item b-profile-stat-entrycount"}):
            total_posts_from_profile = int(li_tag.contents[1].contents[0])

        for li_tag in page_to_parse.findAll("li", {"class" : "b-profile-stat-item b-profile-stat-posted"}):
            total_comments_out = int(li_tag.contents[1].contents[0].replace(",",""))

        for li_tag in page_to_parse.findAll("li", {"class" : "b-profile-stat-item b-profile-stat-received"}):
            total_comments_in = int(li_tag.contents[1].contents[0].replace(",",""))

        return user_info(user, created, total_posts_from_profile, total_comments_in, total_comments_out)

    except HTTPError as message:
        print "process profile" + str(message)
        return False;

def get_first_comment_date(post_link):
    pass

def read_user_comments_counts():
    pass

def get_posts_by_time_density(blogger):
    pass

def get_comments_by_time_density(blogger):
    pass

def get_commentators_kernel(blogger):
    pass

def add_user_to_db(user):
    pass
'''
try:

    db_connection = psycopg2.connect(database='murzilka', user='postgres')
    cur = db_connection.cursor()
    cur.execute("GRANT ALL PRIVILEGES ON TABLE posts TO peterlink;")
#    cur.execute("INSERT INTO posts VALUES( '1', 'test', 'test', '20120618 10:34:09 AM')")
    ver = cur.fetchone()
    print ver


except psycopg2.DatabaseError, e:
    print 'Error %s' % e


finally:

    if db_connection:
        db_connection.close()'''

load_users()

for every_file in glob.glob(data_directory_path):
    current_post_commentators = extract_users_from_file(every_file)
    for every_user in current_post_commentators:
        users_set.add(every_user)

print len(users_set)
for every_user in users_set:
    print get_user_info(every_user)

get_posts_by_time_density(user_name)
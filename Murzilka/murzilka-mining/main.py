import re, os, time, datetime, psycopg2
from tornado.httpclient import HTTPClient, HTTPRequest, HTTPResponse, HTTPError

try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

print "analysis"
bloggers = ["borisakunin", "ntv"]

http_client = HTTPClient()

'''
    db
'''

def create_commentators_table():
    try:
        db_connection = psycopg2.connect(database='murzilka', user='postgres')
        cur = db_connection.cursor()
        cur.execute("DROP TABLE commentators")
        db_connection.commit()
        cur.execute("CREATE TABLE commentators ( username character(64), registration_date timestamp with time zone, posts_count integer, comments_in_count integer, comments_out_count integer, friends_in integer) WITH ( OIDS=FALSE ); ALTER TABLE commentators OWNER TO postgres;")
        db_connection.commit()

    except psycopg2.DatabaseError, e:
        print 'Error %s' % e

    finally:
        if db_connection:
            db_connection.close()

def create_comments_table():
    try:
        db_connection = psycopg2.connect(database='murzilka', user='postgres')
        cur = db_connection.cursor()
        cur.execute("DROP TABLE comments")
        db_connection.commit()
        cur.execute("CREATE TABLE comments ( post_id bigint, author character(64), time timestamp with time zone) WITH ( OIDS=FALSE ); ALTER TABLE comments OWNER TO postgres;")
        db_connection.commit()

    except psycopg2.DatabaseError, e:
        print 'Error %s' % e

    finally:
        if db_connection:
            db_connection.close()

def create_posts_table():
    try:
        db_connection = psycopg2.connect(database='murzilka', user='postgres')
        cur = db_connection.cursor()
        cur.execute("DROP TABLE posts")
        db_connection.commit()
        cur.execute("CREATE TABLE posts(post_id bigserial PRIMARY KEY, user_name character(64), link character(128), time timestamp with time zone) WITH ( OIDS=FALSE); ALTER TABLE posts OWNER TO postgres; GRANT ALL ON TABLE posts TO postgres;")
        db_connection.commit()

    except psycopg2.DatabaseError, e:
        print 'Error %s' % e

    finally:
        if db_connection:
            db_connection.close()

def load_users():
    global users_file
    users_file = open(users_file_name, "r")
    strings = users_file.readlines()
    for i in range(0, len(strings)):
        users_set.add(strings[i])
        i = i + 2
    users_file.close()

def get_time_from_tm_dump(string):
    numbers = re.findall("(\d+)", string)
    year = int(numbers[0])
    month = int(numbers[1])
    day = int(numbers[2])
    hour = int(numbers[3])
    minutes = int(numbers[4])
    seconds = int(numbers[5])

    return datetime.datetime(year, month, day, hour, minutes, seconds)

def load_comments_from_file_to_db(blogger, file_name):
    comments_file = open("../murzilka-parsing/data/{}_posts/".format(blogger) + file_name, "r")
    strings = comments_file.readlines()

    if len(strings) <= 2:
        print "post without comments"
        return

    print "loading", len(strings)/2, "comments to db"

    try:
        db_connection = psycopg2.connect(database="murzilka", user="postgres")
        cursor = db_connection.cursor()

        link = "m.livejournal.com/read/user/{}/{}".format(blogger, file_name)

        comment_timestamp = get_time_from_tm_dump(strings[1])

        cursor.execute("INSERT INTO posts (user_name, link, time) VALUES ('{}', '{}', '{}')".format(blogger, link, comment_timestamp))
        db_connection.commit()

        cursor.execute("SELECT max(post_id) FROM posts")
        answer = str(cursor.fetchall()[0])
        id = re.search("(\d+)", answer).group(0)
        post_id = int(id)

        for i in range(0, len(strings), 2):
            cursor.execute("INSERT INTO comments (post_id, author, time) VALUES('{}', '{}', '{}')".format(post_id, strings[i].replace("\r\n", ""), get_time_from_tm_dump(strings[i+1])))
            db_connection.commit()


    except psycopg2.DatabaseError, e:
        print e;

    finally:
        if db_connection:
            db_connection.close()

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
    def __init__(self, name, created, posts, comments_in, comments_out, friends_in, friends_out):
        self.name = name
        self.registration_date = created
        self.posts_count = posts
        self.comments_in = comments_in
        self.comments_out = comments_out
        self.friends_in = friends_in
        self.friends_out = friends_out

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

#        for span_tag in page_to_parse.findAll("span", {"class" : "b-profile-count ng-binding", "ng-bind" : "tab.commafy_count"}):
#            friends_out

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

def add_posts_and_comments_to_db(blogger):
    for every_file in os.listdir("../murzilka-parsing/data/{}_posts".format(blogger)):
        print "loading comments from", every_file, "to db"
        load_comments_from_file_to_db(blogger, every_file)

def get_all_posts_ids():
    try:
        db_connection = psycopg2.connect(database="murzilka", user="postgres")
        cursor = db_connection.cursor()

        cursor.execute("SELECT post_id FROM posts")
        print cursor.fetchall()

    except psycopg2.DatabaseError, e:
        print e;

    finally:
        if db_connection:
            db_connection.close()

def get_all_comments_for_post_id(post_id):
    try:
        db_connection = psycopg2.connect(database="murzilka", user="postgres")
        cursor = db_connection.cursor()

        cursor.execute("SELECT * FROM comments WHERE post_id = {}".format(post_id))
        print cursor.fetchall()

    except psycopg2.DatabaseError, e:
        print e;

    finally:
        if db_connection:
            db_connection.close()

def update_commentators():
    posts_ids = get_all_posts_ids()

create_posts_table()
create_comments_table()
create_commentators_table()

for every_blogger in bloggers:
    add_posts_and_comments_to_db(every_blogger)

#update_commentators()


#get_posts_by_time_density(user_name)
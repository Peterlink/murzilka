import re, os, time, datetime, psycopg2, socket
from tornado.httpclient import HTTPClient, HTTPRequest, HTTPResponse, HTTPError
from socket import gethostbyname, gaierror

try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

print "analysis"
bloggers = ["borisakunin", "ntv", "fritzmorgen", "colonelcassad", "teh_nomad", "avmalgin", "tema"]

http_client = HTTPClient()

'''
    db
'''
db_connection = None
cursor = None

def create_commentators_table():
    try:
        db_connection = psycopg2.connect(database='murzilka', user='postgres')
        cur = db_connection.cursor()
#        cur.execute("DROP TABLE commentators")
#        db_connection.commit()
        cur.execute("CREATE TABLE commentators ( username character(64), registration_date timestamp with time zone, posts_count integer, comments_in_count integer, comments_out_count integer, friends_in integer) WITH ( OIDS=FALSE ); ALTER TABLE commentators OWNER TO postgres;")
        db_connection.commit()

    except psycopg2.DatabaseError, e:
        print 'Error %s' % e

    finally:
        if db_connection:
            db_connection.close()

def create_deleted_users_table():
    try:
        db_connection = psycopg2.connect(database='murzilka', user='postgres')
        cur = db_connection.cursor()
        cur.execute("DROP TABLE deleted_users")
        db_connection.commit()
        cur.execute("CREATE TABLE deleted_users ( username character(64), type character(16)) WITH ( OIDS=FALSE ); ALTER TABLE commentators OWNER TO postgres;")
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

def delete_all_external_users_from_commentators_table():
    try:
        db_connection = psycopg2.connect(database='murzilka', user='postgres')
        cur = db_connection.cursor()
        cur.execute("DELETE FROM commentators WHERE username like 'ext_*';")
        db_connection.commit()

    except psycopg2.DatabaseError, e:
        print 'Error %s' % e

    finally:
        if db_connection:
            db_connection.close()

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
class comment_info:
    def __init__(self, author, time):
        self.author = author
        self.time = time

class user_info:
    def __init__(self, name, created, posts, comments_in, comments_out, friends_in):
        self.name = name
        self.registration_date = created
        self.posts_count = posts
        self.comments_in = comments_in
        self.comments_out = comments_out
        self.friends_in = friends_in

def get_user_info(user):
    journal_profile_pattern = "http://users.livejournal.com/{}/profile"
    journal_posts_total_beginning = "<div class=\"b-profile-stat-value\">"
    journal_created_beginning = "<span>Created on"
    journal_created_pattern = re.compile("\d{1,2} (January|February|March|April|May|June|July|August|September|October|November|December) \d{4}")
    journal_posts_total_pattern = re.compile("\d{1,10}")
    profile_request = HTTPRequest(journal_profile_pattern.format(user), method = "GET")
    try:
        journal_profile_page = http_client.fetch(profile_request)
        page_to_parse = BeautifulSoup(journal_profile_page.body)

        created = datetime.datetime(1970, 1, 1)
        total_posts_from_profile = 0
        total_comments_in = 0
        total_comments_out = 0
        friends_in = 0

        for span_tag in page_to_parse.findAll("span", {"class" : "b-account-level"}):
            if user.startswith("ext_"):
                date_string = span_tag.contents[1].contents[0].lstrip()
                if date_string == u'Plus Account ':
                    date_string = span_tag.contents[3].contents[0].lstrip()
            else:
                date_string = span_tag.contents[3].contents[0].lstrip()
            date_string = re.search(journal_created_pattern, str(date_string)).group(0)
            creation_date = time.strptime(date_string, "%d %B %Y")
            created = datetime.datetime(creation_date.tm_year, creation_date.tm_mon, creation_date.tm_mday)

        for li_tag in page_to_parse.findAll("li", {"class" : "b-profile-stat-item b-profile-stat-entrycount"}):
            total_posts_from_profile = int(li_tag.contents[1].contents[0].replace(",", ""))

        for li_tag in page_to_parse.findAll("li", {"class" : "b-profile-stat-item b-profile-stat-posted"}):
            total_comments_out = int(li_tag.contents[1].contents[0].replace(",",""))

        for li_tag in page_to_parse.findAll("li", {"class" : "b-profile-stat-item b-profile-stat-received"}):
            total_comments_in = int(li_tag.contents[1].contents[0].replace(",",""))

        friends_count_pattern = re.search('commafy_count\":\"(\d+)', journal_profile_page.body)
        if friends_count_pattern == None:
            friends_in = 0
        else:
            friends_in = int(friends_count_pattern.group(1))

        return user_info(user, created, total_posts_from_profile, total_comments_in, total_comments_out, friends_in)

    except HTTPError as message:
        if message.code == 410:
            print "purged journal"
            add_user_to_db_as_deleted(user, "purged")
            return
        if message.code == 403:
            print "suspended journal"
            add_user_to_db_as_deleted(user, "suspended")
            return
        if message.code == 404:
            print "deleted journal"
            add_user_to_db_as_deleted(user, "deleted")
            return
        print "process profile error", str(message)
        get_user_info(user)

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

def user_is_deleted(user):
    global cursor
    cursor.execute("SELECT * FROM deleted_users WHERE username = '{}'".format(user))
    if cursor.fetchone() == None:
        return False
    else:
        return True

def user_is_already_in_db(user):
    global cursor
    cursor.execute("SELECT * FROM commentators WHERE username = '{}'".format(user))
    if cursor.fetchone() == None:
        if user_is_deleted(user):
            return True
        return False
    else:
        return True

def add_user_to_db_as_deleted(user, deletion_type):
    cursor.execute("INSERT INTO deleted_users (username, type) VALUES ('{}', '{}')".format(user, deletion_type))
    db_connection.commit()
    print "user", user, "added as", deletion_type

def add_user_to_db(user):
    if user_is_already_in_db(user):
        return
    else:
        retry = True
        while retry:
            try:
                info = get_user_info(user)
                retry = False
            except (IOError, socket.gaierror) as e:
                print "get user info retry", e

        if info:
            cursor.execute("INSERT INTO commentators (username, registration_date, posts_count, comments_in_count, comments_out_count, friends_in) VALUES ('{}', '{}', '{}', '{}', '{}', '{}')".format(info.name, info.registration_date, info.posts_count, info.comments_in, info.comments_out, info.friends_in))
            db_connection.commit()
            print user, "added to db"

def add_posts_and_comments_to_db(blogger):
    for every_file in os.listdir("../murzilka-parsing/data/{}_posts".format(blogger)):
        print "loading comments from", every_file, "to db"
        load_comments_from_file_to_db(blogger, every_file)

def get_all_posts_ids():
    global db_connection, cursor
    try:
        db_connection = psycopg2.connect(database="murzilka", user="postgres")
        cursor = db_connection.cursor()

        all_post_ids = []
        cursor.execute("SELECT post_id FROM posts")
        for every_string in cursor.fetchall():
            all_post_ids.append(int(every_string[0]))

        return all_post_ids

    except psycopg2.DatabaseError, e:
        print e;

    finally:
        if db_connection:
            db_connection.close()

def get_all_comments_for_post_id(post_id):
    try:
        db_connection = psycopg2.connect(database="murzilka", user="postgres")
        cursor = db_connection.cursor()

        authors = []

        cursor.execute("SELECT author, time FROM comments WHERE post_id = {}".format(post_id))
        for every_entry in cursor.fetchall():
            authors.append(every_entry[0].rstrip())

        return authors

    except psycopg2.DatabaseError, e:
        print e;

    finally:
        if db_connection:
            db_connection.close()

def save_last_processed_post_id(post_number):
    file = open("last_processed_post_number.txt", "w")
    file.write(str(post_number))
    file.close()

def get_last_processed_post_id():
    file = open("last_processed_post_number.txt", "r")
    return int(file.readline())

def update_commentators():
    print "updating commentators started"
    global db_connection, cursor
    posts_ids = get_all_posts_ids()
    last_processed_post_id = get_last_processed_post_id()
    for every_post_id in posts_ids:
        if every_post_id > last_processed_post_id:
            print every_post_id
            comments_authors = get_all_comments_for_post_id(every_post_id)

            try:
                db_connection = psycopg2.connect(database="murzilka", user="postgres")
                cursor = db_connection.cursor()

                for every_comment_author in comments_authors:
                    add_user_to_db(every_comment_author)

            except psycopg2.DatabaseError, e:
                print e;

            finally:
                if db_connection:
                    db_connection.close()

            save_last_processed_post_id(every_post_id)

    print "update complete"

#create_posts_table()
#create_comments_table()
#create_commentators_table()
#create_deleted_users_table()

#for every_blogger in bloggers:
#    add_posts_and_comments_to_db(every_blogger)

update_commentators()


#get_posts_by_time_density(user_name)
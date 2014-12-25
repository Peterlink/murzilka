import os, re, time, json

try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

from tornado.httpclient import HTTPClient, HTTPRequest, HTTPResponse, HTTPError

data_path = u"data"
user_name = u"ntv"

journal_pattern = "http://m.livejournal.com/read/user/{}"
journal_profile_pattern = "http://{}.livejournal.com/profile"

journal_entry_link_beginning = "http:\/\/\m.livejournal.com\/read\/user/{}\/".format(user_name) + "\d{1,}"
journal_entry_link_to_format = "http://m.livejournal.com/read/user/{}/".format(user_name)
journal_entry_pattern = re.compile(journal_entry_link_beginning)

entry_comments_suffix = "/comments"

previous_post_link = "<a href=\"http:\/\/m.livejournal.com\/read\/user\/{}\/".format(user_name) + "\d{1,}\">Previous post<\/a>"
previous_post_link_pattern = re.compile(previous_post_link)

journal_created_beginning = "<span>Created on"
journal_created_pattern = re.compile("\d{1,2} (January|February|March|April|May|June|July|August|September|October|November|December) \d{4}")
journal_posts_total_beginning = "<div class=\"b-profile-stat-value\">"
journal_posts_total_pattern = re.compile("\d{1,10}")

non_existing_entry = "The page was not found!"

http_client = HTTPClient()

posts = 0
total_posts_from_profile = 0
posts_links_file = ""

def process_profile(username):
    global posts_links_file, data_path
    posts_links_file = open(data_path + "/{}".format(user_name),"a+")

    profile_request = HTTPRequest(journal_profile_pattern.format(username), method = "GET")
    try:
        journal_profile_page = http_client.fetch(profile_request)
        page_to_parse = BeautifulSoup(journal_profile_page.body)

        global total_posts_from_profile

        for span_tag in page_to_parse.findAll("span", {"class" : "b-account-level"}):
            if re.search(journal_created_pattern, str(span_tag)):
                print re.search(journal_created_pattern, str(span_tag)).group(0)
                break

        for div_tag in page_to_parse.findAll("div", {"class" : "b-profile-stat-value"}):
            if str(div_tag).startswith(journal_posts_total_beginning):
                total_count = str(div_tag).replace(",","")
                total_posts_from_profile = int(re.search(journal_posts_total_pattern, total_count).group(0))
                print "posts:" + str(total_posts_from_profile)
                return True

    except HTTPError as message:
        print "process profile" + str(message)
        return False;

def check_all_possible_post_pages(post_link):
    result = ""
    post_number = int(re.search(re.compile("\d{3,}"), post_link).group(0))
    fail = True
    print "frozen post detected: " + str(post_number) + " it's a final countdown!"

    while fail or post_number == 0:
        try:
            post_request = HTTPRequest(journal_entry_link_to_format + str(post_number), method = "GET")
            post_page = http_client.fetch(post_request)

            post_number = post_number - 1
            if str(non_existing_entry) in post_page.body:
                print post_number
            else:
                fail = False
                result = journal_entry_link_to_format + str(post_number)
                print "next post found:" + result
                return result
        except HTTPError as message:
            print str(message)
            continue

def get_previous_post_link(post_link):
    post_request = HTTPRequest(post_link, method = "GET")
    try:
        post_page = http_client.fetch(post_request)
        page_to_parse = BeautifulSoup(post_page.body)

        global posts, posts_links_file
        result = ""
        for link_tag in page_to_parse.findAll("td", {"class":"paging-prev"}):
            if re.search(previous_post_link, str(link_tag)):
                result = re.search(journal_entry_pattern, str(link_tag)).group(0)
                posts = posts + 1
                print posts
            else:
                return ""
    except HTTPError as message:
        print message
        print "previous post link retry"
        result = get_previous_post_link(post_link)
    return result

def get_all_user_posts_links(username):
    journal_request = HTTPRequest(journal_pattern.format(username), method = "GET")

    links_for_entries = posts_links_file.readlines()
    if len(links_for_entries) > 0:
        current_post_link = links_for_entries[-1].rstrip()
    else:
        current_post_link = ""

    global posts
    posts = len(links_for_entries)

    try:
        journal_recent_entries = http_client.fetch(journal_request)
        page_to_parse = BeautifulSoup(journal_recent_entries.body)

        if current_post_link == "":
            global total_posts_from_profile
            links_for_entries = []
            link_for_first_post = ""
            for link_tag in page_to_parse.findAll("h3", {"class":"item-header"}):
                if re.search(journal_entry_pattern, str(link_tag)):
                    link_for_first_post = re.search(journal_entry_pattern, str(link_tag)).group(0)
                    links_for_entries.append(link_for_first_post)
                    posts_links_file.write(link_for_first_post + '\r\n')
                    break

            current_post_link = link_for_first_post

        while True:
            new_link = get_previous_post_link(current_post_link)

            if new_link != "":
                posts_links_file.write(new_link + '\r\n')
                links_for_entries.append(new_link)
                current_post_link = new_link
            else:
                if posts < total_posts_from_profile:
                    current_post_link = check_all_possible_post_pages(current_post_link);
                    new_post_number = int(re.search("\d{1,}", str(current_post_link)).group(0))
                    last_post_number = int(re.search("\d{1,}", str(links_for_entries[-1])).group(0))
                    if new_post_number + 1 == last_post_number:
                        print "all posts parsed"
                        break
                else:
                    break

    except HTTPError as message:
        print message

    return links_for_entries

def get_comments_count(parsed_page):
    for tag in parsed_page.findAll("h2", {"id":"comments", "class":"p-head"}):
        return int(re.findall("\d{1,}", str(tag))[1])

def get_post_date(parsed_page):
    for tag in parsed_page.findAll("p", {"class":"item-meta"}):
        date = "".join(tag.findAll(text=True)).lstrip().rstrip()
        return time.strptime(date, "%b %d, %Y %H:%M")

def get_next_comments_page(parsed_page):
    for tag in parsed_page.findAll("td", {"class":"paging-next"}):
        if tag.find("a"):
            return str(tag.find("a", href=True))
    return ""

class comment:
    def __init__(self, author, date_time):
        self.comment_author = author.rstrip().lstrip()
        self.timestamp = date_time
    def author(self):
        return self.comment_author
    def time(self):
        return self.timestamp

def get_commentators(parsed_page, post_number, page_number, commentators):
    a_tags = parsed_page.findAll("a")
    threads_numbers = []
    for thread_tag in a_tags:
        try:
            thread_tag["name"]
            if str(thread_tag["name"]).isdigit():
                threads_numbers.append(thread_tag["name"])
        except:
            continue
    #all threads got

    global user_name
    for thread_number in threads_numbers:
        thread_link = "http://m.livejournal.com/read/user/{}/{}/comments/p{}/{}".format(user_name, post_number, page_number, thread_number)
        get_request = HTTPRequest(thread_link, method="GET")
        try:
            thread_page = http_client.fetch(get_request)
            page_to_parse = BeautifulSoup(thread_page.body)
            header_divs = page_to_parse.findAll("div", {"class":"item-header"})
            users_tags = []
            for header_div in header_divs:
                users_tags.append(header_div.find("strong", {"class":"lj-user"}))
            comments_dates = page_to_parse.findAll("span", {"class":"item-meta"})
            for i in range(0, len(users_tags)):
                comment_author = "".join(users_tags[i].findAll(text=True)).rstrip()
                comment_date = "".join(comments_dates[i].findAll(text=True)).rstrip()
                comment_date = time.strptime(comment_date, "%B %d %Y, %H:%M:%S UTC")
                commentators.append(comment(comment_author, comment_date))

        except HTTPError as message:
            print message, "get commentators retry"
            return get_commentators(parsed_page, post_number, page_number, commentators)

    return commentators

def check_file_with_comments(file_name, n_comments):
    try:
        file = open(file_name, "r")
        n = len(file.readlines())
        file.close()
        if n/2 == n_comments:
            return True;
        else:
            print file_name + "-not OK"
            return False;
    except:
        print "no file with commentators"
        return False;

def process_post_commentators(entry_link):
    entry_link = entry_link.rstrip()
    get_request = HTTPRequest(entry_link + entry_comments_suffix, method = "GET")
    try:
        comments_page = http_client.fetch(get_request)
        page_to_parse = BeautifulSoup(comments_page.body)

        comments_count = get_comments_count(page_to_parse)
        date = get_post_date(page_to_parse)
        print entry_link, comments_count, date

        post_number = int(re.search("\d{1,}", entry_link).group(0))
        if not os.path.exists(data_path + "/" + user_name + "_posts"):
            os.makedirs(data_path + "/" + user_name + "_posts")

        file_with_comments_name = data_path + "/" + user_name + "_posts" + "/" + str(post_number)
        if check_file_with_comments(file_with_comments_name, comments_count):
            return
        else:
            comments_file = open(file_with_comments_name, "w")

        comments_page_number = 0

        commentators = []
        not_all_comments_parsed = True
        while not_all_comments_parsed:
            next_comments_page_link = get_next_comments_page(page_to_parse)
            if str(next_comments_page_link) != "":
                next_comments_page_link = re.search("(read\/.+?)#comments", str(next_comments_page_link)).group(1)
            else:
                break

            commentators = get_commentators(page_to_parse, post_number, comments_page_number, commentators)
            comments_page_number += 1

            next_page_get_request = HTTPRequest("http://m.livejournal.com/" + next_comments_page_link, method = "GET")
            try:
                next_comments_page = http_client.fetch(next_page_get_request)
                page_to_parse = BeautifulSoup(next_comments_page.body)
            except HTTPError as error:
                print error
                continue

            if get_next_comments_page(page_to_parse) == "":
                commentators = get_commentators(page_to_parse, post_number, comments_page_number, commentators)
                not_all_comments_parsed = False

        for every_comment in commentators:
            comments_file.write(every_comment.author())
            comments_file.write("\r\n")
            comments_file.write(str(every_comment.time()))
            comments_file.write("\r\n")
        comments_file.close()
    except HTTPError as error:
        print error
        process_post_commentators(entry_link) # recursion

def process_one_user(username):
    all_user_posts_links = get_all_user_posts_links(username)
    for link in all_user_posts_links:
        process_post_commentators(link)

if not os.path.exists(data_path):
    os.makedirs(data_path)

if process_profile(user_name):
    process_one_user(user_name)
    posts_links_file.close()
else:
    print "error during processing profile"
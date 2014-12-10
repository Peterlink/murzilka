import re, pdb

try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

from tornado.httpclient import HTTPClient, HTTPRequest, HTTPResponse, HTTPError

user_name = "ntv"

journal_pattern = "http://m.livejournal.com/read/user/{}"
journal_profile_pattern = "http://{}.livejournal.com/profile"

journal_entry_link_beginning = "http:\/\/\m.livejournal.com\/read\/user/{}\/".format(user_name) + "\d{1,}"
journal_entry_link_to_format = "http://m.livejournal.com/read/user/{}/".format(user_name)
journal_entry_pattern = re.compile(journal_entry_link_beginning)

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
posts_links_file = open("{}".format(user_name),"a+")

def process_profile(username):
    profile_request = HTTPRequest(journal_profile_pattern.format(username), method = "GET")
    try:
        journal_profile_page = http_client.fetch(profile_request)
        page_to_parse = BeautifulSoup(journal_profile_page.body)

        global total_posts_from_profile

        for span_tag in page_to_parse.findAll("span"):
            if re.search(journal_created_pattern, str(span_tag)):
                print re.search(journal_created_pattern, str(span_tag)).group(0)
                break

        for div_tag in page_to_parse.findAll("div"):
    #        print str(link_tag)
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

    while fail:
        try:
            post_request = HTTPRequest(journal_entry_link_to_format + str(post_number), method = "GET")
            post_page = http_client.fetch(post_request)

            if str(non_existing_entry) in post_page.body:
                post_number = post_number - 1
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
        for link_tag in page_to_parse.findAll("a"):
            if re.search(previous_post_link, str(link_tag)):
                result = re.search(journal_entry_pattern, str(link_tag)).group(0)
                posts = posts + 1
                print posts
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
            for link_tag in page_to_parse.findAll("a"):
                if re.search(journal_entry_pattern, str(link_tag)):
                    link_for_first_post = re.search(journal_entry_pattern, str(link_tag)).group(0)
                    links_for_entries.append(link_for_first_post)
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
                    # its frozen post
                    current_post_link = check_all_possible_post_pages(current_post_link);

    except HTTPError as message:
        print message

    print links_for_entries
    return []#links_for_entries

def process_post_commentators(link):
    n_pages = get_count_of_comment_pages(link)
    for i in range(0,n_pages):
        parse_all_comments(link, i)

def process_one_user(username):
    all_user_posts_links = get_all_user_posts_links(username)
    for link in all_user_posts_links:
        process_post_commentators(link)

if process_profile(user_name):
    process_one_user(user_name)
    posts_links_file.close()
else:
    print "error during processing profile"
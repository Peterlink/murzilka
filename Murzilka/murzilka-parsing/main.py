import re, pdb

try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

from tornado.httpclient import HTTPClient, HTTPRequest, HTTPResponse

user_name = "tema"

journal_pattern = "http://m.livejournal.com/read/user/{}"

journal_entry_link_beginning = "http:\/\/\m.livejournal.com\/read\/user/{}\/".format(user_name) + "\d{1,}"
journal_entry_pattern = re.compile(journal_entry_link_beginning)

previous_post_link = "<a href=\"http:\/\/m.livejournal.com\/read\/user\/tema\/\d{1,}\">Previous post<\/a>"
previous_post_link_pattern = re.compile(previous_post_link)

http_client = HTTPClient()

posts = 0

def get_previous_post_link(post_link):
    post_request = HTTPRequest(post_link, method = "GET")
    post_page = http_client.fetch(post_request)
    page_to_parse = BeautifulSoup(post_page.body)

    global posts
    result = ""
    for link_tag in page_to_parse.findAll("a"):
        if re.search(previous_post_link, str(link_tag)):
            result = re.search(journal_entry_pattern, str(link_tag)).group(0)
            posts = posts + 1
            print posts
    return result

def get_all_user_posts_links(username):
    journal_request = HTTPRequest(journal_pattern.format(username), method = "GET")
    journal_recent_entries = http_client.fetch(journal_request)
    page_to_parse = BeautifulSoup(journal_recent_entries.body)

    links_for_entries = set()
    link_for_first_post = ""
    for link_tag in page_to_parse.findAll("a"):
        if re.search(journal_entry_pattern, str(link_tag)):
            link_for_first_post = re.search(journal_entry_pattern, str(link_tag)).group(0)
            links_for_entries.add(link_for_first_post)
            break# breaking cycle

    current_post_link = link_for_first_post
    while True:
        new_link = get_previous_post_link(current_post_link)
        if new_link != "":
            links_for_entries.add(new_link)
            current_post_link = new_link
        else:
            break

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

process_one_user(user_name)
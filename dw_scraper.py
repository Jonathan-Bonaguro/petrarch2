from __future__ import unicode_literals
from bs4 import BeautifulSoup
import requests
import json
import re
import datetime

from pymongo import MongoClient

connection = MongoClient()

db = connection.event_scrape
collection = db["dw_test"]


url_list = ["http://www.dw.com/search/?languageCode=en&item=refugees&searchNavigationId=9097&sort=RELEVANCE&resultsCounter=1900",
"http://www.dw.com/search/?languageCode=en&item=asylum&searchNavigationId=9097&sort=RELEVANCE&resultsCounter=405"]
#"http://www.dw.com/search/?languageCode=en&item=crime&searchNavigationId=9097&sort=RELEVANCE&resultsCounter=1900",
#"http://www.dw.com/search/?languageCode=en&item=pegida&searchNavigationId=9097&sort=RELEVANCE&resultsCounter=1900",
#"http://www.dw.com/search/?languageCode=en&item=anti-semitism&searchNavigationId=9097&sort=RELEVANCE&resultsCounter=1900",
#"http://www.dw.com/search/?languageCode=en&item=hate crime&searchNavigationId=9097&sort=RELEVANCE&resultsCounter=1900"]

def get_links(url):
    headers = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.107 Safari/537.36"}
    result = requests.get(url, headers=headers)
    soup = BeautifulSoup(result.content, "lxml")
    article_boxes = soup.find_all("div", {"class" : "searchResult"})
    
    link_stubs = []
    for a in article_boxes:
        b = a.find('a', href = True)
        link_stubs.append(b['href'])
        
    links = [''.join(["http://www.dw.com", i]) for i in link_stubs]
    
    links = [i for i in links if re.search("/av-\d+$", i) is None]
    links = [i for i in links if re.search("/g-\d+$", i) is None]
    
    return links

# Taken verbatim from https://github.com/openeventdata/scraper/
def _check_mongo(url, db_collection):
    """
    Private function to check if a URL appears in the database.
    Parameters
    ----------
    url: String.
            URL for the news stories to be scraped.
    db_collection: pymongo Collection.
                        Collection within MongoDB that in which results are
                        stored.
    Returns
    -------
    found: Boolean.
            Indicates whether or not a URL was found in the database.
    """

    if db_collection.find_one({"url": url}):
        found = True
    else:
        found = False

    return found

# Taken verbatim from https://github.com/openeventdata/scraper/
def add_entry(collection, text, title, url, date, website, lang):
    """
    Function that creates the dictionary of content to add to a MongoDB
    instance, checks whether a given URL is already in the database, and
    inserts the new content into the database.
    Parameters
    ----------
    collection : pymongo Collection.
                    Collection within MongoDB that in which results are stored.
    text : String.
            Text from a given webpage.
    title : String.
            Title of the news story.
    url : String.
            URL of the webpage from which the content was pulled.
    date : String.
            Date pulled from the RSS feed.
    website : String.
                Nickname of the site from which the content was pulled.
    Returns
    -------
    object_id : String
    """
    toInsert = {"url": url,
                "title": title,
                "source": website,
                "date": date,
                "date_added": datetime.datetime.utcnow(),
                "content": text,
                "stanford": 0,
                "language": lang}
    object_id = collection.insert_one(toInsert)
    return object_id

# This is the part that's customized to DW article pages
def scrape_article(article_url, test = False):
    if not test and _check_mongo(article_url, collection):
        print("Duplicate URL. Skipping...")
        return
    headers = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.107 Safari/537.36"}
    result = requests.get(article_url, headers=headers)
    soup = BeautifulSoup(result.content, "lxml")
    
    try:
        body = soup.find("div", {"class" : "longText"}).find_all("p", recursive=False)
        # recursive = False keeps it from going into the picture divs and pulling out the captions
        # It also gets rid of the social media stuff.
        body_trimmed = [i.text for i in body if re.search("<strong>", repr(i)) is None]
        article_body = "\n\n".join(body_trimmed)
    except Exception as e:
        print("Couldn't scrape url {0} with error {1}").format(article_url, e)
        return {} # is this the best behavior?
    
    title = soup.find("div", {"id": "bodyContent"}).find("h1").text
    
    try:
        date_raw = soup.find("div", {"class" : "col1 dim"}).find("li").text
        date_pieces = re.findall("\d+", date_raw)
        date_pieces = [int(i) for i in date_pieces]
        date = datetime.datetime(date_pieces[2], date_pieces[1], date_pieces[0])
    except Exception as e:
        print("Problem getting date, returning null. {0}").format(e)
        return {}
    
    if not test:
        add_entry(collection, article_body, title, 
                   article_url,  date,  "deutsche_welle",  
                  "english")
    if test:
        article = {"url": article_url,
                    "title": title,
                    "source": "deutsche_welle",
                    "date": date,
                    "date_added": datetime.datetime.utcnow(),
                    "content": article_body,
                    "stanford": 0,
                    "language": "english"}
        print(article)

if __name__ == "__main__":
    results = get_links(url_list[0])
    print("Testing on one article:")
    scrape_article(results[22], test = True)
    
    print("\n\nNow downloading and loading into Mongo.")
    for search in url_list:
        results = get_links(search)
        for art in results:
            scrape_article(art)
    print("Complete. The Mongo collection now has {0} articles in it").format(collection.count())

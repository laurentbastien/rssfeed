import feedparser
import psycopg2
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import datetime


print("Running script at " + str(datetime.datetime.now()))

try:
    conn = psycopg2.connect("dbname='postgres' user='username' host='ip' password='password'")
except:
    print("not connected!")

cur = conn.cursor()

cur.execute('SELECT * FROM newsfeed')
feeds = cur.fetchall()

for feed, name in feeds:
    d = feedparser.parse(feed)
    print("In feed " + feed)
    for post in d.entries:
        host=urlparse(feed).hostname.split('.')[1]
        key = host+"-"+post['id']
        print("Key is " + key)

        author = ""
        if "author" in post:
            author = post["author"]

        try:
            cur.execute('insert into newspiece(id, title, summary, published, author, feed, keyid) values (%s, %s, %s, %s, %s, %s, %s)',
                    (post['id'], post['title'], BeautifulSoup(post['summary'], 'html.parser').get_text(), post['published'], author, feed, key))
            print("Inserted" + key)
            conn.commit()
        except psycopg2.Error as e:
            print("Caught exception while attempting to insert " + key)
            print(e)
            conn.rollback()
        except KeyError as ke:
            print("Caught KeyErrpr while attempting to insert " + key)
            print(e)
            conn.rollback()

#conn.commit()
cur.close()
conn.close()
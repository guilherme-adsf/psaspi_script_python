from datetime import datetime
import requests

def create_user_collect(cur, conn, totalTweets, profileId, userId, followers):
    sql = "INSERT INTO collection (status, referenceDate, totalTweets, profileId, userId, followers) VALUES (%s, %s, %s, %s, %s, %s)"
    val = ('Em Andamento', datetime.now(), totalTweets, profileId, userId, followers)
    cur.execute(sql, val)
    conn.commit()
    return cur.lastrowid

def update_user_collect(cur, conn, status, id):
    sql = "UPDATE collection SET status = %s WHERE id = %s"
    val = (status, id)
    cur.execute(sql, val)
    conn.commit()

def get_number_of_tweets_collected(cur, collectionId):
    sql = "SELECT COUNT(*) FROM collections_tweets WHERE collectionId = {};".format(collectionId)
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def update_totaltweets_collect(cur, conn, totalTweets, id):
    sql = "UPDATE collection SET totalTweets = %s WHERE id = %s"
    val = (totalTweets, id)
    cur.execute(sql, val)
    conn.commit()

def insert_tweet_replie(cur, conn, collectionId, profileId, replyStatusId, replyText, sentiment, timeStamp, apiKey):
    sql = "INSERT INTO replies_tweets (collectionId, profileId, replyStatusId, replyText, sentiment, timeStamp, apiKey) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (collectionId, profileId, replyStatusId, replyText, sentiment, timeStamp, apiKey)
    cur.execute(sql, val)
    conn.commit()

def insert_retweets_with_comments(cur, conn, collectionId, profileId, value, gspSequence, tweetId):
    sql = "INSERT INTO retweets_with_comments (collectionId, profileId, value, gspSequence, tweetId) VALUES (%s, %s, %s, %s, %s)"
    val = (collectionId, profileId, value, gspSequence, tweetId)
    cur.execute(sql, val)
    conn.commit()

def find_last_replie(cur):
    sql = "SELECT apiKey FROM replies_tweets ORDER BY id DESC LIMIT 1;"
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def get_active_ibm_keys(cur):
    sql = "SELECT label FROM ibm_tokens WHERE status = 'Ativo'"
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def update_ibm_key(cur, conn, status, label):
    sql = "UPDATE ibm_tokens SET status = %s WHERE label = %s"
    val = (status, label)
    cur.execute(sql, val)
    conn.commit()

def find_collect(cur, collect_id):
    sql = "SELECT * FROM collection WHERE id = {};".format(collect_id)
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def find_collect_by_rtk(cur, collect_id, tweetId):
    sql = "SELECT * FROM retweets_with_comments WHERE collectionId = {} and tweetId = {};".format(collect_id, tweetId)
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def update_status_collect_replies(cur, conn, status, id):
    sql = "UPDATE collection SET status_collect_replies = %s WHERE id = %s"
    val = (status, id)
    cur.execute(sql, val)
    conn.commit()

def update_status_collect_retweets_with_comments(cur, conn, status, id):
    sql = "UPDATE collection SET status_collect_retweets_with_comment = %s WHERE id = %s"
    val = (status, id)
    cur.execute(sql, val)
    conn.commit()

def get_all_tweets_of_collect(cur, collect_id):
    sql = "SELECT id, tweetId, gspSequence FROM collections_tweets WHERE collectionId = {};".format(collect_id)
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def get_count_replies_of_tweet(cur, replyTweetId, collectionId):
    sql = "SELECT COUNT(*) FROM replies_tweets WHERE replyStatusId = {} and collectionId = {};".format(replyTweetId, collectionId)
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def get_count_replies_of_tweet_with_sentiment(cur, replyTweetId, collectionId, sentiment):
    sql = "SELECT COUNT(*) FROM replies_tweets WHERE replyStatusId = {} and collectionId = {} and sentiment = '{}';".format(replyTweetId, collectionId, sentiment)
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def update_replies_of_tweet_collect(cur, conn, numOfReplies, gspSequence, K_positive, K_neutral, K_negative, K_nonclass, retweetsWithComments, collectTweetId):
    sql = "UPDATE collections_tweets SET comments = %s, gspSequence = %s, K_positive = %s, K_neutral = %s, K_negative = %s, K_nonclass = %s, retweetsWithComments = %s WHERE id = %s"
    val = (numOfReplies, gspSequence, K_positive, K_neutral, K_negative, K_nonclass, retweetsWithComments, collectTweetId)
    cur.execute(sql, val)
    conn.commit()

def update_retweets_with_comment_of_tweet_collect(cur, conn, retweetsWithComments, gspSequence, tweetId):
    sql = "UPDATE collections_tweets SET retweetsWithComments = %s, gspSequence = %s WHERE tweetId = %s"
    val = (retweetsWithComments, gspSequence, tweetId)
    cur.execute(sql, val)
    conn.commit()

def delete_replies_of_tweet(cur, conn, collect_id):
    sql = "DELETE FROM replies_tweets WHERE collectionId = %s"
    val = (collect_id)
    cur.execute(sql, val)
    conn.commit()

def get_tweet_collected(cur, tweetId):
    sql = "SELECT * FROM collections_tweets WHERE tweetId = {};".format(tweetId)
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def get_count_number_of_tweets_collected(cur, collect_id):
    sql = "SELECT COUNT(*) FROM collections_tweets WHERE collectionId = {};".format(collect_id)
    cur.execute(sql)
    myresult = cur.fetchall()
    return myresult

def send_request_update_collect_values(collect_id, origin):
    url = 'http://127.0.0.1:5000/update_collect_values'
    myobj = {'collect_id': collect_id, 'origin': origin}
    requests.post(url, json=myobj)

def send_request_collect_report(collect_id):
    request_url = "http://localhost:3333/collectionreport/{}".format(collect_id)
    requests.get(request_url)
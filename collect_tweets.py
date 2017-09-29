#!/usr/bin/python
# -*- coding: utf-8 -*-

from final.keys import SQL_HOST, SQL_USER, SQL_PW, SQL_DB
from final.keys import TW_CKEY, TW_CSEC, TW_ATKEY, TW_ATSEC
import pymysql.cursors
from twitter import Api
import datetime
import dateutil.parser
import time
import html
import traceback


# 最新のtable_numを取得
def get_table_num():
    sql_select = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%tweets%' ORDER BY table_name DESC"
    cursor.execute(sql_select)
    tables_results = cursor.fetchall()
    table_num = int(tables_results[0]['table_name'].replace('tweets_branch', ''))

    return table_num


# 最新のtableのcount(*)を取得
def get_tweet_count(table_num):
    sql_select = "SELECT count(*) FROM tweets%s"
    cursor.execute(sql_select, (table_num))
    tweet_count_results = cursor.fetchall()
    tweet_count = int(tweet_count_results[0]['count(*)'])
    # print('table{0}:'.format(table_num), tweet_count)

    return tweet_count


# 新しいtableを作成
def create_table(table_num):
    # tableを作るときにtable_num + 1
    table_num += 1
    sql_create = "CREATE TABLE tweets%s LIKE tweets%s"
    cursor.execute(sql_create, (table_num, table_num - 1))
    connection.commit()
    sql_create = "CREATE TABLE tweets_branch%s LIKE tweets_branch%s"
    cursor.execute(sql_create, (table_num, table_num - 1))
    connection.commit()

    print('Create new tables\n')


# どのツイートまでbranchを取得したかを調べる
def get_branch_check_id():
    sql_select = "SELECT id FROM ids WHERE name = 'branch_check_id'"
    cursor.execute(sql_select)
    branch_check_id_results = cursor.fetchall()
    branch_check_id = int(branch_check_id_results[0]['id'])
    # print('branch_check_id:', branch_check_id, '\n')

    return branch_check_id


# 最後に取得したツイートのtweet_idを調べる
def get_last_id():
    sql_select = "SELECT id FROM ids WHERE name = 'last_id'"
    cursor.execute(sql_select)
    last_id_results = cursor.fetchall()
    last_id = int(last_id_results[0]['id'])
    # print('last_id:', last_id, '\n')

    return last_id


# tweetsを取得
def get_tweets(table_num, last_id):
    now_time = datetime.datetime.now()
    print('Start searching tweets', now_time.strftime('%Y-%m-%d %H:%M:%S'))
    # print("-----------------------------------------\n")

    # ツイートを59件集める
    search_str = '#nowplaying -RT -歌ってみた -nicobox'
    found = api.GetSearch(term=search_str,lang='ja',count=59,result_type='recent',since_id=last_id+1)
    i = 0
    for f in found:
        tweet_time = dateutil.parser.parse(f.created_at).strftime('%Y-%m-%d %H:%M:%S')
        user = f.user.screen_name
        tweet = html.unescape(f.text)
        tweet_id = f.id

        dt = datetime.datetime.strptime(tweet_time, '%Y-%m-%d %H:%M:%S')
        time_JST = dt + datetime.timedelta(hours=9)

        # tweetをSQLに保存
        sql_insert = "INSERT IGNORE INTO tweets%s(tweet_id, time, user, tweet) VALUES(%s, %s, %s, %s)"
        cursor.execute(sql_insert, (table_num, tweet_id, time_JST, user, tweet))
        connection.commit()

        # print(i+1, time_JST, user, tweet)
        i = i + 1

    # SQL内の最新のtweet_idの取得
    sql_select = "SELECT tweet_id FROM tweets%s ORDER BY time DESC LIMIT 1"
    cursor.execute(sql_select, (table_num))
    last_id_results = cursor.fetchall()
    last_id = int(last_id_results[0]['tweet_id'])
    # SQL内のlast_idの更新
    sql_update = "UPDATE ids SET id = %s WHERE name = 'last_id'"
    cursor.execute(sql_update, (last_id))
    connection.commit()

    # print('\n-----------------------------------------')
    now_time = datetime.datetime.now()
    print('Finish searching tweets', now_time.strftime('%Y-%m-%d %H:%M:%S'), '\n')


# tweets_branchを取得
def get_tweets_branch(table_num, branch_check_id):
    now_time = datetime.datetime.now()
    print('Start searching branch', now_time.strftime('%Y-%m-%d %H:%M:%S'))
    print('-----------------------------------------\n')

    # SQLに保存されたツイートを59件取り出す
    sql_select = "SELECT * FROM tweets%s WHERE tweet_id > (%s) ORDER BY CAST(tweet_id AS signed) LIMIT 59"
    cursor.execute(sql_select, (table_num, branch_check_id))
    results = cursor.fetchall()

    for r in results:
        root_time = r['time']
        user = r['user']
        root_tweet = r['tweet']
        root_tweet_id = r['tweet_id']
        root_tweet_id_int = int(root_tweet_id)

        print('@ root: {0} {1} {2}'.format(root_time, user, root_tweet))

        # root_tweetの前後5分のtweetをbranchとする
        dp = root_time + datetime.timedelta(minutes=5)
        dm = root_time - datetime.timedelta(minutes=5)
        since = dm.strftime('%Y-%m-%d_%H:%M:%S')
        until = dp.strftime('%Y-%m-%d_%H:%M:%S')

        # branchを取得する
        search_str = 'from:{0} since:{1}_JST until:{2}_JST -nowplaying -RT'.format(user, since, until)
        found = api.GetSearch(term=search_str, lang='ja', count=100, result_type='recent')
        for f in found:
            if f.id != root_tweet_id_int and not f.user_mentions:
                tweet_time = dateutil.parser.parse(f.created_at).strftime('%Y-%m-%d %H:%M:%S')
                user = f.user.screen_name
                tweet = html.unescape(f.text)
                tweet_id = f.id_str

                dt = datetime.datetime.strptime(tweet_time, '%Y-%m-%d %H:%M:%S')
                time_JST = dt + datetime.timedelta(hours=9)

                print('branch: {0} {1} {2}'.format(time_JST, user, tweet))

                # branchをSQLに保存
                sql_insert = "INSERT IGNORE INTO tweets_branch%s VALUES(%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql_insert, (table_num, tweet_id, time_JST, user, tweet, root_tweet_id, root_time, root_tweet))
                connection.commit()

        # branch_check_idの更新
        if branch_check_id < root_tweet_id_int:
            branch_check_id = root_tweet_id_int

    # SQL内のbranch_check_idの更新
    sql_update = "UPDATE ids SET id = %s WHERE name = 'branch_check_id'"
    cursor.execute(sql_update, (branch_check_id))
    connection.commit()


if __name__ == '__main__':
    # MySQLに接続する
    connection = pymysql.connect(host=SQL_HOST,
                                 user=SQL_USER,
                                 password=SQL_PW,
                                 db=SQL_DB,
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    # TwitterAPIに接続
    api = Api(base_url='https://api.twitter.com/1.1',
              consumer_key=TW_CKEY,
              consumer_secret=TW_CSEC,
              access_token_key=TW_ATKEY,
              access_token_secret=TW_ATSEC)

    count = 0
    with connection.cursor() as cursor:
        # RateLimitに引っかからないように 60*3 = 180 で回す
        # get_tweets:1 + get_tweets_branch:59 = 60 -> 5 minutes
        while True:
            try:
                table_num = get_table_num()
                branch_check_id = get_branch_check_id()
                last_id = get_last_id()
                tweet_count = get_tweet_count(table_num)

                # tweetが1,000,000件を超えると新しいtableを作成
                if tweet_count >= 1000000:
                    create_table(table_num)

                # すべてのbranchを取得したらget_tweets
                if branch_check_id >= last_id:
                    print('Finish checking all branch\n')
                    get_tweets(table_num, last_id)
                else:
                    count += 1
                    print('loop count:', count)
                    get_tweets_branch(table_num, branch_check_id)
                    next_time = datetime.datetime.now() + datetime.timedelta(minutes=5)
                    print('\nWait until', next_time.strftime('%Y-%m-%d %H:%M:%S'))
                    print('-----------------------------------------')
                    # 5分ごとに休む
                    time.sleep(5 * 60)
            except:
                # エラーが発生したらエラー文を表示して5分後に再開
                print('-----------------------------------------')
                print(traceback.format_exc())
                next_time = datetime.datetime.now() + datetime.timedelta(minutes=5)
                print('\nWait until', next_time.strftime('%Y-%m-%d %H:%M:%S'))
                print('-----------------------------------------')
                time.sleep(5*60)

    # MySQLから切断する
    connection.close()

#!/usr/bin/python
# -*- coding: utf-8 -*-

from final.keys import SQL_HOST, SQL_USER, SQL_PW, SQL_DB
from final.situation_dict import SITUATION_DICT
import time
import pymysql.cursors
import MeCab
import urllib.parse
import mojimoji
from rdflib import Graph, Namespace, Literal, BNode
from rdflib.namespace import FOAF, DC


def work(table_num):
    sql_select = "SELECT * FROM tweets%s WHERE title != '' ORDER BY CAST(tweet_id AS signed)"
    # and time < '2016-09-04' 区間指定
    cursor.execute(sql_select, (table_num))
    results = cursor.fetchall()

    sql_select = "SELECT root_tweet_id, tweet FROM tweets_branch%s"
    cursor.execute(sql_select, (table_num))
    branch_results = cursor.fetchall()

    count = 0
    count_remnant = 0
    for r in results:
        tweet_id   = r['tweet_id']
        tweet_time = r['time']
        artist     = r['artist']
        title      = r['title']
        remnant    = r['remnant']

        # branch_tweetをremnantに付帯
        for br in branch_results:
            root_tweet_id = br['root_tweet_id']
            branch_tweet  = br['tweet']
            if root_tweet_id == tweet_id:
                remnant = remnant + ' ' + branch_tweet

        # remnantの整形
        remnant = remnant.replace(title, '').replace(artist, '')
        remnant = mojimoji.zen_to_han(remnant, kana=False)
        remnant = mojimoji.han_to_zen(remnant, digit=False, ascii=False)

        situation_weight = {}

        # timeをsituationに変換
        situ_by_time = time_to_tag(tweet_time)
        for situation_word in situ_by_time.values():
            situation_weight[situation_word] = TIME_WEIGHT

        # remnantが存在していた場合
        if remnant:
            count_remnant += 1
            situ_by_rem = remnant_to_tag(remnant)
            # remnantからsituationがとれた場合
            if situ_by_rem:
                for situation_word in situ_by_rem.keys():
                    if situation_word in situation_weight:
                        situation_weight[situation_word] += REMNANT_WEIGHT
                    else:
                        situation_weight[situation_word] = REMNANT_WEIGHT

                # 以下のブロックをインデントして使うtripleを調節
                count += 1
                print(count, 'tweet_time:', tweet_time)
                print('tweet:', r['tweet'])
                print('artist:', artist)
                print('title:', title)
                print('remnant:', remnant)
                print('situation', situation_weight)
                print('--------------------------------')
                music_id = urllib.parse.quote_plus(artist + '_' + title)
                for tag, weight in situation_weight.items():
                    insert_triples(music_id, title, artist, tag, weight)

    print('count_remnant:', count_remnant)


def time_to_tag(time_str):
    return_situation_dict = {}
    return_situation_dict['rough_time'] = hour[time_str.hour]
    return_situation_dict['day_of_week'] = week[time_str.weekday()]
    return_situation_dict['month'] = str(time_str.month) + '月'

    return return_situation_dict


def remnant_to_tag(remnant):
    return_situation_dict = {}

    wakati = mecab.parse(remnant)
    wakati_list = wakati.split()

    for situation, tag_list in SITUATION_DICT.items():
        for situation_word in tag_list[0]:
            if situation_word in wakati_list:
                return_situation_dict[situation] = situation_word
        for situation_word in tag_list[1]:
            if situation_word in remnant:
                return_situation_dict[situation] = situation_word

    return return_situation_dict


# weightのupdateがされなければ新しいnodeをadd
def insert_triples(music_id, title, artist, tag, weight):
    update_is = weight_update(music_id, tag, weight)
    if not update_is:
        add_triples(music_id, title, artist, tag, weight)


def add_triples(music_id, title, artist, tag, weight):
    graph.add((MUSICID[music_id], DC.title, Literal(title)))
    graph.add((MUSICID[music_id], FOAF.maker, Literal(artist)))

    situbn = BNode()
    graph.add((MUSICID[music_id], SITUATION.blank, situbn))
    graph.add((situbn, SITUATION.tag, TAG[tag]))
    graph.add((situbn, SITUATION.weight, Literal(weight)))


def weight_update(music_id, tag, add_weight):
    update_is = False
    for s, p, o in graph.triples((MUSICID[music_id], SITUATION.blank, None)):
        if graph.value(o, SITUATION.tag) == TAG[tag]:
            weight = graph.value(o, SITUATION.weight)
            weight += add_weight
            graph.set((o, SITUATION.weight, Literal(weight)))
            update_is = True

    return update_is


if __name__ == '__main__':
    start_time = time.time()

    TIME_WEIGHT    = 1
    REMNANT_WEIGHT = 10

    MUSICID   = Namespace("http://music.metadata.database.musicid/")
    TAG       = Namespace("http://music.metadata.database.tag/")
    SITUATION = Namespace("http://music.metadata.database.situation/")

    hour = [''] * 24
    hour[0]  = hour[1]  = hour[2]  = "深夜"
    hour[3]  = hour[4]  = hour[5]  = "明け方"
    hour[6]  = hour[7]  = hour[8]  = hour[9] = "朝"
    hour[10] = hour[11] = hour[12] = hour[13] = hour[14] = "昼"
    hour[15] = hour[16] = hour[17] = "夕方"
    hour[18] = hour[19] = hour[20] = hour[21] = hour[22] = hour[23] = "夜"
    week = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日曜日"]

    mecab = MeCab.Tagger("-d /usr/lib/mecab/dic/mecab-ipadic-neologd -Owakati")
    connection = pymysql.connect(host=SQL_HOST,
                                 user=SQL_USER,
                                 password=SQL_PW,
                                 db=SQL_DB,
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        graph = Graph()
        rdf_file = "music_rdf_test.ttl"
        graph.parse("music_rdf_prefix.ttl", format="n3")

        work(table_num=1)

        f = open(rdf_file, 'w')
        f.write(graph.serialize(format='turtle').decode('UTF-8'))
        f.close()
        graph.close()

    # MySQLから切断する
    connection.close()

    elapsed_time = time.time() - start_time
    print("Processing finished in {0:.3f} [sec]".format(elapsed_time))

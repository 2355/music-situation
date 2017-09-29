#!/usr/bin/python
# -*- coding: utf-8 -*-

from final.keys import SQL_HOST, SQL_USER, SQL_PW, SQL_DB
from final.keys import LF_KEY, LF_SEC, GN_CID, GN_UID
import pymysql.cursors
import pylast
from pygn import pygn
import musicbrainzngs
import urllib.request
import urllib.parse
import urllib.error
import re
import html
import json
import traceback
import time
import datetime


# ツイートの整形
def parse_tweet(tmp):
    tmp = html.unescape(tmp)

    sub_list = [r'http[s]?[\S]*',
                r' on #[\S]*',
                r'#[\S]*',
                r'[#|＃]?[\s]?now[\s]?playing[:]?',
                r'RT @[\w]*:',
                r' via @[\w]*',
                r'[\W]@[\w]*',
                r'^@[\w]*',
                r'\(再生回数:? ?\d+回?\)',
                r'\d+回再生',
                r'バッテリー残量:? ?\d+%',
                r'(\d+%)? ?充電切断',
                r'(\d+%)? ?充電中',
                r'充電： ?\d+%',
                r'\d\d\.?\d? ?kHz/\d\d?bit',
                r'\d\d\.?\d? ?kHz',
                r'◖ฺ.+?◗',
                r'◖.+?◗',
                r'\[NOA\]:\(.+?\)',
                r'title ?[:|：]?',
                r'album ?[:|：]',
                r'artist ?[:|：]?']

    replace_list = ['\xa0',
                    '🎧',
                    '♪',
                    '♫',
                    '♩',
                    '♬',
                    '…',
                    '今かかっている曲は？',
                    '現在再生中',
                    'を聴いています',
                    ' in UBiO',
                    ' in KaiserTone',
                    "I'm listening to",
                    'iMac',
                    'プチリリ',
                    'The song I am listening now.',
                    'Playingなう',
                    "₍₍ ◝('ω'◝) ⁾⁾ ₍₍ (◟'ω')◟ ⁾⁾",
                    '現在 bottleman では',
                    'が流れておりますDeath！',
                    'iTunes再生中']

    for l in sub_list:
        tmp = re.sub(l, '', tmp, flags=re.I)

    tmp = tmp.replace('“', '"')\
        .replace(' "', ' ').replace('" ', ' ').replace('　', ' ')

    for l in replace_list:
        tmp = tmp.replace(l, '')

    # str -> list
    tmp2 = re.split('\n| \| | : | - | − | ― | ー | / | ／ | on album |聴いている曲は|  ', tmp)

    i = 0
    for t in tmp2:
        # 先頭・末尾の空白削除
        t = re.sub(r'^[\s]*', '', t)
        t = re.sub(r'[\s]*$', '', t)
        # 先頭・末尾の " 削除
        t = re.sub(r'^["]?', '', t)
        t = re.sub(r'["]?$', '', t)
        tmp2[i] = t
        i += 1
    while tmp2.count('') > 0:
        # 空要素があれば削除
        tmp2.remove('')

    return tmp2


# Last.fm API
def lastfm_search(artist, title):
    response = {'artist': '', 'title': ''}
    network = pylast.LastFMNetwork(api_key=LF_KEY, api_secret=LF_SEC)

    while True:
        try:
            search = network.search_for_track(artist, title)
            results = search.get_next_page()
            if results:
                response['artist'] = html.unescape(results[0].artist.name)
                response['title'] = html.unescape(results[0].title)

            return response
        except pylast.MalformedResponseError as err:
            print('--------------------------------')
            print('Last.fm API error')
            print(err)
            print('--------------------------------')
            time.sleep(10)


# iTunes API
def itunes_search(artist, title):
    response = {'artist': '', 'title': ''}
    base   = 'https://itunes.apple.com/search?'
    params = 'country=jp&lang=ja_jp&media=music&entity=musicTrack&limit=10&term='
    term = artist + ' ' + title
    term = urllib.parse.quote_plus(term)

    err_count = 0
    while True:
        try:
            post = urllib.request.urlopen(base + params + term)
            pos = post.read().decode('utf-8')
            j = json.loads(pos)
            results = j['results']
            for r in results:
                if r['kind'] == 'song':
                    response['artist'] = html.unescape(r['artistName'])
                    response['title'] = html.unescape(r['trackName'])
                    break

            return response
        except urllib.error.HTTPError as err:
            err_count += 1
            if err_count >= 5:
                # エラーが5回以上続いたらSLEEP_TIME後に再開
                next_time = datetime.datetime.now() + datetime.timedelta(minutes=SLEEP_TIME)
                print('--------------------------------')
                print('iTunes API error')
                print(err)  # HTTP Error 403: Forbidden
                print('Wait until', next_time.strftime('%Y-%m-%d %H:%M:%S'))
                print('--------------------------------')
                time.sleep(60*SLEEP_TIME)


# Gracenote API
def gracenote_search(artist, title):
    response = {'artist': '', 'title': ''}

    while True:
        try:
            result = pygn.search(clientID=GN_CID, userID=GN_UID,
                                 artist=artist, album='', track=title)
            if result:
                response['artist'] = html.unescape(result['album_artist_name']).replace('\u3000', ' ')
                response['title'] = html.unescape(result['track_title']).replace('\u3000', ' ')

            return response
        except Exception as err:
            print('--------------------------------')
            print('Gracenote API error')
            print(err)
            print('--------------------------------')
            time.sleep(10)


# MusicBrainz API
def musicbrainz_search(artist, title):
    response = {'artist': '', 'title': ''}
    musicbrainzngs.set_useragent("Example music app", "0.6",
                                 "http://example.com/music")

    err_count = 0
    while True:
        try:
            result = musicbrainzngs.search_recordings(artist=artist,
                                                      recording=title)
            if result['recording-list']:
                res = result['recording-list'][0]
                response['artist'] = html.unescape(res['artist-credit-phrase'])
                response['title'] = html.unescape(res['title'])

            return response
        except musicbrainzngs.musicbrainz.NetworkError as err:
            err_count += 1
            if err_count >= 5:
                print('--------------------------------')
                print('MusicBrainz API error')
                print(err)
                print('--------------------------------')
                return response


# 検索APIの選択
def select_api(artist, title, api):
    if api == 'lastfm':
        response = lastfm_search(artist, title)
        return response

    if api == 'itunes':
        response = itunes_search(artist, title)
        return response

    if api == 'gracenote':
        response = gracenote_search(artist, title)
        return response

    if api == 'musicbrainz':
        response = musicbrainz_search(artist, title)
        return response


# 複数パターンで検索
def get_track(lists, api):
    artist = ''
    title = ''
    remnant = ''
    response = {'artist': '', 'title': ''}

    # listが空になった場合は空のままreturn
    if not lists:
        return response, remnant, artist, title

    # 2要素以上あればartistも検索
    if len(lists) >= 2:
        artist = lists[1]
    title = lists[0]
    response = select_api(artist, title, api)

    # responseなしかつ2要素以上あれば再検索
    if not response['title'] and len(lists) >= 2:
        artist = lists[0]
        title = lists[1]
        response = select_api(artist, title, api)

    # responseなしかつ3要素以上あれば再検索
    if not response['title'] and len(lists) >= 3:
        artist = lists[2]
        title = lists[0]
        response = select_api(artist, title, api)

    # responseなしかつ3要素以上あれば再検索
    if not response['title'] and len(lists) >= 3:
        artist = lists[2]
        title = lists[1]
        response = select_api(artist, title, api)

    # responseなしかつ3要素以上あれば再検索
    if not response['title'] and len(lists) >= 3:
        artist = lists[-1]
        title = lists[-2]
        response = select_api(artist, title, api)

    # responseなしかつ3要素以上あれば再検索
    if not response['title'] and len(lists) >= 3:
        artist = lists[-2]
        title = lists[-3]
        response = select_api(artist, title, api)

    # responseなしかつ3要素以上あれば再検索
    if not response['title'] and len(lists) >= 3:
        artist = lists[-2]
        title = lists[-1]
        response = select_api(artist, title, api)

    # responseなしかつ3要素以上あれば再検索
    if not response['title'] and len(lists) >= 3:
        artist = lists[-3]
        title = lists[-2]
        response = select_api(artist, title, api)

    # responseがあればlistからartist, titleを削除
    if response['title']:
        if artist:
            lists.remove(artist)
        lists.remove(title)
        remnant = ' '.join(lists)

    return response, remnant, artist, title


# tracksなしならば再検索
def re_get_track(tweet2, api, ans_dict, sub_dict):
    tracks = {'artist': '', 'title': ''}

    # artist2: 検索クエリとして使ったartist
    # title2: 検索クエリとして使ったtitle
    # remnant = tweet2 - [artist2, title2]
    artist2 = ''
    title2 = ''
    remnant = ''

    # tweet3 = 前回のparse時のtweet2
    # tweet3とtweet2の値が変わらなかったら検索を省略
    tweet3 = tweet2.copy()

    # 楽曲検索
    if api == 'lastfm' or api == 'itunes':
        tracks, remnant, artist2, title2 = get_track(tweet2, api)

    # tracksなしならばさらに分割して再検索
    if not tracks['title']:
        t = ' by '.join(tweet2)
        tweet2 = re.split(' by | from | via|/|／|  ', t)
        i = 0
        for t in tweet2:
            # 先頭・末尾の空白削除
            t = re.sub(r'^[\s]*', '', t)
            t = re.sub(r'[\s]*$', '', t)
            tweet2[i] = t
            i += 1
        while tweet2.count('') > 0:
            # 空要素があれば削除
            tweet2.remove('')
        if (api == 'lastfm' or api == 'itunes') and (tweet2 != tweet3):
            if DEBUG_FLUG:
                print(tweet2)
            tweet3 = tweet2.copy()
            tracks, remnant, artist2, title2 = get_track(tweet2, api)

    # tracksなしならば (text) を削除して再検索
    if not tracks['title']:
        i = 0
        for t in tweet2:
            t = re.sub(r'\(.+?\)', '', t)
            t = re.sub(r'（.+?）', '', t)
            t = re.sub(r'\[.+?\]', '', t)
            t = re.sub(r'［.+?］', '', t)
            t = re.sub(r'【.+?】', '', t)
            # 先頭・末尾の空白削除
            t = re.sub(r'^[\s]*', '', t)
            t = re.sub(r'[\s]*$', '', t)
            tweet2[i] = t
            i += 1
        while tweet2.count('') > 0:
            # 空要素があれば削除
            tweet2.remove('')
        if (api == 'lastfm' or api == 'itunes') and (tweet2 != tweet3):
            if DEBUG_FLUG:
                print(tweet2)
            tweet3 = tweet2.copy()
            tracks, remnant, artist2, title2 = get_track(tweet2, api)

    # tracksなしならばさらに分割して再検索
    if not tracks['title']:
        t = ' on '.join(tweet2)
        tweet2 = re.split(' on | in |by | with|｢|｣|「|」|『|』|-|‐|−|―|\||:|：|  ', t)
        i = 0
        for t in tweet2:
            # 先頭・末尾の空白削除
            t = re.sub(r'^[\s]*', '', t)
            t = re.sub(r'[\s]*$', '', t)
            tweet2[i] = t
            i += 1
        while tweet2.count('') > 0:
            # 空要素があれば削除
            tweet2.remove('')
        if tweet2 != tweet3:
            if DEBUG_FLUG:
                print(tweet2)
            tweet3 = tweet2.copy()
        tracks, remnant, artist2, title2 = get_track(tweet2, api)

    # tracksなしならば (text or text) を削除して再検索
    if not tracks['title']:
        i = 0
        for t in tweet2:
            t = re.sub(r'\(.*$', '', t)
            t = re.sub(r'^.*\)', '', t)
            t = re.sub(r'（.*$', '', t)
            t = re.sub(r'^.*）', '', t)
            t = re.sub(r'\d+%', '', t)
            t = re.sub(r'via [\w]*', '', t)
            # 先頭・末尾の空白削除
            t = re.sub(r'^[\s]*', '', t)
            t = re.sub(r'[\s]*$', '', t)
            tweet2[i] = t
            i += 1
        while tweet2.count('') > 0:
            # 空要素があれば削除
            tweet2.remove('')
        if tweet2 != tweet3:
            if DEBUG_FLUG:
                print(tweet2)
            tracks, remnant, artist2, title2 = get_track(tweet2, api)

    # trackがあればans_dictにartist, titleを保存
    # trackがあればremnant_dictにremnantを保存
    if tracks['title']:
        ans_dict[api]['artist'] = tracks['artist']
        ans_dict[api]['title'] = tracks['title']
        if DEBUG_FLUG:
            print('◆', api.rjust(11), ':', ans_dict[api])
        sub_dict[api]['remnant'] = remnant
        sub_dict[api]['artist2'] = artist2
        sub_dict[api]['title2'] = title2

    if DEBUG_FLUG:
        print()

    return ans_dict, sub_dict


# 正解楽曲を取得
def decide_ans(ans_dict, sub_dict):
    ans = music_dict.copy()
    sub = remnant_dict.copy()

    # ansの多数決
    # lf = 'LF' -> 'LF'
    # lf = '', it = 'IT' -> 'IT'
    # lf = 'LF', it = gn = 'IT', mb = 'IT' OR 'MB' -> 'IT'
    # lf = 'LF', it = mb = 'IT', gn = 'IT' OR 'GN' -> 'IT'
    # lf = 'LF', it = 'IT', gn = mb = 'GN' -> 'GN'
    # lf = it = '', gn = mb = 'GN' OR '' -> 'GN'
    # どれも帰ってこなかった場合は空のまま
    if ans_dict['lastfm']['artist']:
        ans = ans_dict['lastfm']
        sub = sub_dict['lastfm']
    if not ans_dict['lastfm']['artist'] and ans_dict['itunes']['artist']:
        ans = ans_dict['itunes']
        sub = sub_dict['itunes']
    if ans_dict['lastfm'] != ans_dict['itunes'] \
            and ans_dict['lastfm'] != ans_dict['musicbrainz'] \
            and ans_dict['itunes'] == ans_dict['gracenote']:
        ans = ans_dict['itunes']
        sub = sub_dict['itunes']
    if ans_dict['lastfm'] != ans_dict['itunes'] \
            and ans_dict['lastfm'] != ans_dict['gracenote'] \
            and ans_dict['itunes'] == ans_dict['musicbrainz']:
        ans = ans_dict['itunes']
        sub = sub_dict['itunes']
    if ans_dict['lastfm'] != ans_dict['itunes'] \
            and ans_dict['lastfm'] != ans_dict['gracenote'] \
            and ans_dict['lastfm'] != ans_dict['musicbrainz'] \
            and ans_dict['itunes'] != ans_dict['gracenote'] \
            and ans_dict['itunes'] != ans_dict['musicbrainz'] \
            and ans_dict['gracenote'] == ans_dict['musicbrainz']:
        ans = ans_dict['gracenote']
        sub = sub_dict['gracenote']
    if not ans_dict['lastfm']['artist'] and not ans_dict['itunes']['artist'] \
            and ans_dict['gracenote'] == ans_dict['musicbrainz']:
        ans = ans_dict['gracenote']
        sub = sub_dict['gracenote']

    return ans, sub


# どのツイートまでmusicを取得したかを調べる
def get_music_check_id():
    sql_select = "SELECT id FROM ids WHERE name = 'music_check_id'"
    cursor.execute(sql_select)
    music_check_id_results = cursor.fetchall()
    music_check_id = int(music_check_id_results[0]['id'])
    # print('music_check_id:', music_check_id, '\n')

    return music_check_id


# SQLにartist, title, remnantを追加
def insert_music():
    table_num = 1
    music_check_id = get_music_check_id()
    sql_select = "SELECT tweet, tweet_id, time FROM tweets%s WHERE tweet_id > (%s) " \
                 "ORDER BY CAST(tweet_id AS signed) limit 10"
    cursor.execute(sql_select, (table_num, music_check_id))
    results = cursor.fetchall()
    if results:
        if DEBUG_FLUG:
            print('table_num:', table_num)
            print('========================================================\n')
        for r in results:
            tweet_id = r['tweet_id']
            tweet = r['tweet']
            if DEBUG_FLUG:
                tweet_time = r['time']
                print('tweet_time:', tweet_time, 'tweet_id:', tweet_id)
            print(tweet)
            ans_dict = {'lastfm': music_dict.copy(),
                        'itunes': music_dict.copy(),
                        'gracenote': music_dict.copy(),
                        'musicbrainz': music_dict.copy()}
            sub_dict = {'lastfm': remnant_dict.copy(),
                        'itunes': remnant_dict.copy(),
                        'gracenote': remnant_dict.copy(),
                        'musicbrainz': remnant_dict.copy()}

            # 各APIからtrackを取得
            for api in api_list:
                # lf = gn = 'LF' -> dont use mb
                if api == 'musicbrainz' and \
                        ans_dict['lastfm']['artist'] and\
                        ans_dict['lastfm'] == ans_dict['gracenote']:
                    pass
                # lf = gn = 'LF' OR lf = mb = 'LF' -> dont use it
                elif api == 'itunes' and \
                        ans_dict['lastfm']['artist'] and (
                            ans_dict['lastfm'] == ans_dict['gracenote'] or
                            ans_dict['lastfm'] == ans_dict['musicbrainz']):
                    pass
                # gn = mb = 'GN' -> dont use it
                elif api == 'itunes' and \
                        ans_dict['gracenote']['artist'] and \
                            ans_dict['gracenote'] == ans_dict['musicbrainz']:
                    pass
                else:
                    tweet2 = parse_tweet(tweet)
                    if DEBUG_FLUG:
                        print(tweet2)
                    ans_dict, sub_dict = re_get_track(tweet2, api, ans_dict, sub_dict)

            # ansの決定
            ans, sub = decide_ans(ans_dict, sub_dict)
            if ans['title']:
                print('● ANSWER :', ans)
                print(sub)
            print('========================================================\n')

            # SQL内にartist, title, remnantを追加
            sql_update = "UPDATE tweets%s SET artist = %s, title = %s, " \
                         "remnant = %s, artist2 = %s, title2 = %s WHERE tweet_id = %s"
            cursor.execute(sql_update, (table_num, ans['artist'], ans['title'],
                                        sub['remnant'], sub['artist2'], sub['title2'], tweet_id))
            connection.commit()

            # SQL内のmusic_check_idの更新
            sql_update = "UPDATE ids SET id = %s WHERE name = 'music_check_id'"
            cursor.execute(sql_update, (tweet_id))
            connection.commit()
    else:
        table_num += 1


if __name__ == '__main__':
    DEBUG_FLUG = True
    SLEEP_TIME = 5
    api_list = ['lastfm', 'gracenote', 'musicbrainz', 'itunes']
    music_dict = {'artist': '', 'title': ''}
    remnant_dict = {'remnant': '', 'artist2': '', 'title2': ''}

    # MySQLに接続する
    connection = pymysql.connect(host=SQL_HOST,
                                 user=SQL_USER,
                                 password=SQL_PW,
                                 db=SQL_DB,
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)

    count = 0
    with connection.cursor() as cursor:
        while True:
            try:
                count += 1
                now_time = datetime.datetime.now()
                print(now_time.strftime('%Y-%m-%d %H:%M:%S'), 'loop count:', count)
                insert_music()
            except:
                # エラーが発生したらエラー文を表示してSLEEP_TIME後に再開
                print('--------------------------------')
                print(traceback.format_exc())
                next_time = datetime.datetime.now() + datetime.timedelta(minutes=SLEEP_TIME)
                print('\nWait until', next_time.strftime('%Y-%m-%d %H:%M:%S'))
                print('--------------------------------\n')
                time.sleep(60*SLEEP_TIME)

    # MySQLから切断する
    connection.close()
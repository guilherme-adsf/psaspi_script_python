import os
import pymysql
import tweepy
from flask import Flask, request, jsonify
from flaskext.mysql import MySQL
from rq import Queue
from redis import Redis
from threading import Thread
import locale
from utils.helpers import format_created_at
from utils.repository import *
from utils.watson import analyze_sentiment

app = Flask(__name__)
queue_collect_comments = Queue('queueComments', connection=Redis())
queue_collect_retweets = Queue('queueRetweets', connection=Redis())
queue_update_comments = Queue('queueUpdateComments', connection=Redis())
# queue_collect_comments.empty()
# queue_collect_retweets.empty()
# queue_update_comments.empty()

# Dados Twitter API
# Para fins de escalonamento de chaves, foi utilizado duas instancias da API do Twitter
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

consumer_key_two = ''
consumer_secret_two = ''
access_token_two = ''
access_token_secret_two = ''
auth_two = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth_two.set_access_token(access_token, access_token_secret)
api_two = tweepy.API(auth_two, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#Instanciando Dados do Banco
mysql = MySQL()
app.config['MYSQL_DATABASE_HOST'] = 'DATABASE_HOST'
app.config['MYSQL_DATABASE_USER'] = 'DATABASE_USER'
app.config['MYSQL_DATABASE_PASSWORD'] = 'DATABASE_PASSWORD'
app.config['MYSQL_DATABASE_DB'] = 'coletas'
mysql.init_app(app)

@app.route('/update_collect_values', methods=['POST'])
def request_update_replies_values_of_collect():
    data = request.json
    collect_id = data['collect_id']
    origin = data['origin']
    Thread(target=update_replies_values_of_collect, kwargs={'collect_id': collect_id, 'origin': origin}).start()
    return jsonify('')

def update_replies_values_of_collect(collect_id, origin):
    # Cursores
    conn = mysql.connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    # Buscar Coleta
    print('LOG :: Buscando Coleta')
    collect_result = find_collect(cur, collect_id)

    # Verificar Status ['Em Andamento', 'Finalizada', 'Interrompida']
    print('LOG :: Coleta ', collect_result[0])

    print('LOG :: Origin: ', origin)
    if (origin == 'CollectTweets'):
        # Se status da coleta de comentarios for finalizado
        get_total_tweets = get_number_of_tweets_collected(cur, collect_id)
        total_tweets = get_total_tweets[0]['COUNT(*)']
        update_totaltweets_collect(cur, conn, total_tweets, collect_id)
        collect_status = True if collect_result[0]['status_collect_replies'] == 'Finalizada' and collect_result[0][
            'status_collect_retweets_with_comment'] == 'Finalizada' else False
    elif (origin == 'CollectOfReplies'):
        # Se status da coleta de tweets for finalizado
        collect_status = True if collect_result[0]['status'] == 'Finalizada' and collect_result[0][
            'status_collect_retweets_with_comment'] == 'Finalizada' else False
    else:
        # Se status da coleta de retweets for finalizado
        collect_status = True if collect_result[0]['status'] == 'Finalizada' and collect_result[0][
            'status_collect_replies'] == 'Finalizada' else False

    print('LOG :: Status da Coleta: ', collect_status)

    if (collect_status == True):
        print('LOG :: Coleta Finalizada')
        collect_tweets = get_all_tweets_of_collect(cur, collect_id)
        print('LOG :: Iterando Tweets da Coleta: ', len(collect_tweets))

        for x in collect_tweets:
            print('\n')
            print('LOG :: Tweet: ', x)
            print('LOG :: TweetId: {} - GspSequence: {} - Id: {}'.format(x['tweetId'], x['gspSequence'], x['id']))
            num_replies_of_tweet = get_count_replies_of_tweet(cur, x['tweetId'], collect_id)
            extract_num_replies_of_tweet = num_replies_of_tweet[0]['COUNT(*)']
            discret_value_of_replies = '!K' if int(extract_num_replies_of_tweet) == 0 else 'K'
            print('LOG :: Numero de Comentarios: ', extract_num_replies_of_tweet, discret_value_of_replies)

            get_collect_rtk = find_collect_by_rtk(cur, collect_id, x['tweetId'])
            extract_num_rtk = 0 if len(get_collect_rtk) == 0 else get_collect_rtk[0]['value']
            discret_value_of_rtk = '!RtK' if int(extract_num_rtk) == 0 else 'RtK'
            print('LOG :: Numero de Retweets com Comentario: ', len(get_collect_rtk), extract_num_rtk, discret_value_of_rtk)

            split_gsp_seq = x['gspSequence'].split(' ')
            print('Split GSP Sequence: ', split_gsp_seq)

            gsp_seq = '{} {} {} {}'.format(split_gsp_seq[0], split_gsp_seq[1], discret_value_of_rtk,
                                           discret_value_of_replies)

            num_replies_of_tweet_positive = get_count_replies_of_tweet_with_sentiment(cur, x['tweetId'], collect_id,
                                                                                      'positive')
            extract_num_replies_of_tweet_positive = num_replies_of_tweet_positive[0]['COUNT(*)']
            num_replies_of_tweet_neutral = get_count_replies_of_tweet_with_sentiment(cur, x['tweetId'], collect_id,
                                                                                     'neutral')
            extract_num_replies_of_tweet_neutral = num_replies_of_tweet_neutral[0]['COUNT(*)']
            num_replies_of_tweet_negative = get_count_replies_of_tweet_with_sentiment(cur, x['tweetId'], collect_id,
                                                                                      'negative')
            extract_num_replies_of_tweet_negative = num_replies_of_tweet_negative[0]['COUNT(*)']
            num_replies_of_tweet_nonclass = get_count_replies_of_tweet_with_sentiment(cur, x['tweetId'], collect_id,
                                                                                      'não classificado')
            extract_num_replies_of_tweet_nonclass = num_replies_of_tweet_nonclass[0]['COUNT(*)']

            print('LOG :: Gsp Sequence ', gsp_seq)
            print('LOG :: Atualizando Num de Comentarios e GspSequence')
            update_replies_of_tweet_collect(cur, conn, int(extract_num_replies_of_tweet), gsp_seq,
                                            int(extract_num_replies_of_tweet_positive),
                                            int(extract_num_replies_of_tweet_neutral),
                                            int(extract_num_replies_of_tweet_negative),
                                            int(extract_num_replies_of_tweet_nonclass), int(extract_num_rtk), x['id'])
        send_request_collect_report(collect_id)

def get_user(profile):
    try:
        user = api.get_user(profile)
        return {'status': True, 'userObj': user}
    except:
        return {'status': False}

def get_user_followers(profile):
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    user = api.get_user(profile)
    followers_count = user._json['followers_count']
    followers_count_formatted = locale.currency(followers_count, grouping=True, symbol=None)
    return followers_count_formatted.replace(',00', '')

@app.route('/coletar_dados', methods=['POST'])
def return_status():
    # Dados do Body
    data = request.json
    profile_id = data['id_perfil']
    profile_exists = get_user(profile_id)

    if profile_exists['status']:
        quantidade_tweets = int(data['quantidade_tweets']) if int(data['quantidade_tweets']) < int(profile_exists['userObj'].statuses_count) else int(profile_exists['userObj'].statuses_count)
        user_id = int(data['user_id'])
        start = int(data['start'])
        id = data['id']
        Thread(target=handle_collect, kwargs={'profile_id': profile_id, 'quantidade_tweets': quantidade_tweets, 'user_id': user_id,
                                   'start': start, 'id': id}).start()
        return jsonify('Este perfil existe.')
    else:
        return jsonify('Este perfil não existe.')

def collect_retweets_with_comment(collect_id, profile_id, number_of_tweets):
    # Cursores
    conn = mysql.connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    update_status_collect_retweets_with_comments(cur, conn, 'Em Andamento', collect_id)

    for status in tweepy.Cursor(api.user_timeline, profile_id, tweet_mode='extended').items(number_of_tweets):

        # Se for um retweet não coletar.
        if hasattr(status, 'retweeted_status'):
            continue

        # JSON do Tweet
        status_json = status._json

        # Número de Interações
        num_rtweets = int(status_json['retweet_count'])
        num_retweets_with_comments = 0

        # URL do Tweet
        url = 'https://twitter.com/{}/status/{}'.format(profile_id, status_json['id'])

        # Nome do CSV
        csv_name = str(profile_id) + '-' + str(status_json['id']) + '.csv'

        try:
            # Se possui retweets, então verificar se possui retweets com comentarios
            if num_rtweets != 0:
                # Abrir URL no UIVision
                print('Abrindo Navegador')
                command_cmd = 'google-chrome --password-store=basic "{}?direct=1&cmd_var1={}&cmd_var2={}&closeBrowser=1"'.format(
                    'file:///home/guilherme/flask_api_Tcc/UiVisionScript.html', url, csv_name)
                print(command_cmd)
                os.system(command_cmd)

                # Path aonde será salvo/lido o CSV com valor de retweets com comentario
                csv_path = "/home/guilherme/Desktop/uivision/datasources/" + csv_name

                if os.path.exists(csv_path):
                    # Abrir CSV
                    with open(csv_path) as f:
                        lis = [line.split() for line in f]
                        for i, x in enumerate(lis):
                            try:
                                if x[0] != '#LNF':
                                    value = x[0].replace('"', "")
                                    value = value.strip()
                                    if value == 'Curtida' or value == 'Curtidas':
                                        num_retweets_with_comments = 0
                                    elif value == 'Tweets' or value == 'Tweet' or value == 'de' or value == 'comentário':
                                        num_retweets_with_comments = num_retweets_with_comments
                                    else:
                                        if i == 0:
                                            num_retweets_with_comments = int(value)
                                else:
                                    num_retweets_with_comments = 0
                            except Exception as e:
                                if e.__str__() == 'list index out of range':
                                    num_retweets_with_comments = 0
                                else:
                                    print('Exceção na leitura CSV: ', e)
                                    num_retweets_with_comments = 0
                else:
                    print("CSV não encontrado", "ID Tweet: ", status_json['id'])
                    num_retweets_with_comments = 0

            tweet_collected = get_tweet_collected(cur, status_json['id'])
            print('Tweet Collected: ', status_json['id'], tweet_collected[0])
            split_gsp_seq = tweet_collected[0]['gspSequence'].split(' ')

            # Montar GSP Sequence
            if (num_retweets_with_comments == 0):
                gsp_seq = '{} {} {} {}'.format(split_gsp_seq[0], split_gsp_seq[1], '!Rtk', split_gsp_seq[3])
            else:
                gsp_seq = '{} {} {} {}'.format(split_gsp_seq[0], split_gsp_seq[1], 'Rtk', split_gsp_seq[3])

            insert_retweets_with_comments(cur, conn, collect_id, profile_id, num_retweets_with_comments, gsp_seq, status_json['id'])
        except Exception as e:
            print('Catch Retweets: ', e)

    update_status_collect_retweets_with_comments(cur, conn, 'Finalizada', collect_id)

    send_request_update_collect_values(collect_id, 'CollectOfRetweetsWithComment')

def collect_replies(collect_id, profile_id):
    print('LOG :: Coletando Comentarios do Perfil {}, da Coleta de ID: {}'.format(profile_id, collect_id))

    # Cursores
    conn = mysql.connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    # Buscar Ultima Inserção para Saber Qual Foi a Ultima Api Key Utilizada
    last_replie = find_last_replie(cur)

    print('LOG :: Última Inserção na Tabela de Comentario: ', last_replie)

    if (len(last_replie) == 1):
        last_api_key = last_replie[0]['apiKey']
        if(last_api_key == 'api_key_one'):
            twitter_api_instance = api_two.search
            apiKey = 'api_key_two'
        else:
            twitter_api_instance = api.search
            apiKey = 'api_key_one'
    else:
        twitter_api_instance = api.search
        apiKey = 'api_key_one'

    print('LOG :: Twitter API Key da Vez: {}'.format(apiKey))

    # Buscar Comentarios
    comentario_count = 0

    try:
        # Setando Status da Coleta de Comentarios para 'Em Andamento'
        print('LOG :: Atualizando Status da Coleta de Comentarios para Em Andamento')
        update_status_collect_replies(cur, conn, 'Em Andamento', collect_id)

        for tweet in tweepy.Cursor(twitter_api_instance, q='to:' + profile_id, timeout=999999).items(5000):
            print(comentario_count)
            if hasattr(tweet, 'in_reply_to_status_id_str'):
                json_tweet = tweet._json
                if not (json_tweet['in_reply_to_status_id_str'] is None):
                    timestamp_final = format_created_at(json_tweet['created_at'])
                    try:
                        sentiment = analyze_sentiment(mysql, json_tweet['text'])
                    except Exception as e:
                        sentiment = 'não classificado'
                    insert_tweet_replie(cur, conn, collect_id, profile_id, json_tweet['in_reply_to_status_id_str'], json_tweet['text'], sentiment, timestamp_final, apiKey)
            comentario_count = comentario_count + 1

        # Update no Status da Coleta de Comentarios para 'Finalizada'
        print('LOG :: Atualizando Status da Coleta de Comentarios para Finalizado')
        update_status_collect_replies(cur, conn, 'Finalizada', collect_id)

        # Se Coleta de Tweets estiver com status de finalizado, então atualiza os valores das colunas de comentario e solicita o relatorio final.
        # Script para Atualizar Valores Relacionados ao Comentario
        send_request_update_collect_values(collect_id, 'CollectOfReplies')
    except Exception as e:
        print('Exception', e)
        # Update no Status da Coleta de Comentarios para 'Finalizada'
        print('LOG :: Ocorreu um Erro no Job, Atualizando Status da Coleta de Comentarios para Finalizado')
        update_status_collect_replies(cur, conn, 'Finalizada', collect_id)

def handle_collect(profile_id, quantidade_tweets, user_id, start, id):
    # Cursores
    conn = mysql.connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    if id:
        id_coleta = int(id)
        # Deletar Comentarios Já Coletados, para não afetar na contagem.
        delete_replies_of_tweet(cur, conn, id_coleta)
        update_user_collect(cur=cur, conn=conn, status='Em Andamento', id=int(id))
        # Pegando valor da ultima iteração
        count_tweets_collected = get_count_number_of_tweets_collected(cur, id_coleta)
        start = int(count_tweets_collected[0]['COUNT(*)'])
    else:
        # Criando Coleta
        followers_count = get_user_followers(profile_id)
        id_coleta = create_user_collect(cur=cur, conn=conn, totalTweets=quantidade_tweets, profileId=profile_id, userId=user_id, followers=followers_count)

    print(id_coleta)

    from app import collect_replies, collect_retweets_with_comment

    cont = 0

    try:
        for status in tweepy.Cursor(api.user_timeline, profile_id, tweet_mode='extended').items(quantidade_tweets):
            print('Iteração', cont)

            # Espaço para iniciar coleta de retweets, para evitar de tentar dar update em algo vazio.
            if cont == round(quantidade_tweets * 0.1):
                job_collect_retweets = queue_collect_retweets.enqueue(collect_retweets_with_comment,
                                                                      args=(id_coleta, profile_id, quantidade_tweets),
                                                                      job_timeout='10h')
                print('Cont: {} - Iniciando Coleta Retweets - Job Id: {}'.format(cont, job_collect_retweets.get_id()))
                job_collect_comments = queue_collect_comments.enqueue(collect_replies, args=(id_coleta, profile_id),
                                                                      job_timeout='20h')
                print(job_collect_comments.get_id())

            # Se for um retweet não coletar.
            if hasattr(status, 'retweeted_status'):
                cont = cont + 1
                continue

            if cont >= start:
                # JSON do Tweet
                status_json = status._json

                # Número de Interações
                num_curtidas = int(status_json['favorite_count'])
                num_rtweets = int(status_json['retweet_count'])
                num_retweets_with_comments = 0

                if hasattr(status, 'retweeted_status'):
                    tweet_text = status.retweeted_status.full_text
                else:
                    tweet_text = status.full_text

                print('Saiu')

                try:
                    sentiment = analyze_sentiment(mysql, tweet_text)
                except Exception as e:
                    sentiment = 'não classificado'

                num_replies = 0

                print(id_coleta, profile_id, status_json['id'], sentiment, num_curtidas, num_rtweets,
                      num_retweets_with_comments, num_replies)

                l_discret_value = 'L' if int(num_curtidas) != 0 else '!L'
                rt_discret_value = 'Rt' if int(num_rtweets) != 0 else '!Rt'
                rtk_discret_value = 'Rtk' if num_retweets_with_comments != 0 else '!Rtk'
                replies_discret = 'K' if num_replies != 0 else '!K'
                gsp_sequence = l_discret_value + ' ' + rt_discret_value + ' ' + rtk_discret_value + ' ' + replies_discret
                print(gsp_sequence)

                timestamp_final = format_created_at(status_json['created_at'])

                sql = "INSERT INTO collections_tweets (collectionId, profileId, tweetId, sentiment, likes, retweets, retweetsWithComments, comments, gspSequence, timeStamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (
                    id_coleta, profile_id, status_json['id'], sentiment, num_curtidas, num_rtweets,
                    num_retweets_with_comments,
                    num_replies, gsp_sequence, timestamp_final)
                cur.execute(sql, val)
                conn.commit()
            cont = cont + 1
        # Update no Status da Coleta de Tweets para 'Finalizada'
        update_user_collect(cur=cur, conn=conn, status='Finalizada', id=id_coleta)

        # Se Coleta de Comentarios Estiver Finalizado Então Atualiza os Valores das Colunas de Comentario e Solicitar o Relatorio Final
        # Script para Atualizar Valores Relacionados ao Comentario
        send_request_update_collect_values(id_coleta, 'CollectTweets')

        cur.close()
        return 'Fim'
    except Exception as e:
        print(e)
        update_user_collect(cur=cur, conn=conn, status='Interrompida', id=id_coleta)
        return 'Fim'

if __name__ == '__main__':
    app.run()

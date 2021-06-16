import random
import pymysql
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson.natural_language_understanding_v1 import Features, SentimentOptions
from utils.repository import get_active_ibm_keys, update_ibm_key

# IBM Watson NLU
# https://www.ibm.com/br-pt/cloud/watson-natural-language-understanding
# Para fins de escalonamento de limites da API foi utilizado cinco instancias da API IBM Watson

authenticator = IAMAuthenticator('API_KEY')
service = NaturalLanguageUnderstandingV1(
    version='VERSION',
    authenticator=authenticator
)
service.set_service_url('SERVICE_URL')

authenticator_two = IAMAuthenticator('API_KEY')
service_two = NaturalLanguageUnderstandingV1(
    version='VERSION',
    authenticator=authenticator_two
)
service_two.set_service_url('SERVICE_URL')

authenticator_three = IAMAuthenticator('API_KEY')
service_three = NaturalLanguageUnderstandingV1(
    version='VERSION',
    authenticator=authenticator_three
)
service_three.set_service_url('SERVICE_URL')

authenticator_four = IAMAuthenticator('API_KEY')
service_four = NaturalLanguageUnderstandingV1(
    version='VERSION',
    authenticator=authenticator_four
)
service_four.set_service_url('SERVICE_URL')

authenticator_five = IAMAuthenticator('API_KEY')
service_five = NaturalLanguageUnderstandingV1(
    version='VERSION',
    authenticator=authenticator_five
)
service_five.set_service_url('SERVICE_URL')

# Essa função faz a analise de sentimento e também realiza o sorteio da chave da vez, caso ocorra uma exceção é devido a limite ter excedido sendo assim o status
# daquela chave vai ser atualizado no banco de dados para inativo.

# OBS: Cadastrar suas chaves na tabela ibm_tokens, na coluna label foi usado as letras A,B,C,D,E para marcar as chaves e identificar as mesma no sorteio da chave da vez.
def analyze_sentiment(mysql, replyText):
    # Cursores
    conn = mysql.connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    try:
        api_key_random = get_active_ibm_keys(cur)
        keys = [{'label': 'E'}] if len(api_key_random) == 0 else api_key_random
        random_choice = random.choice(keys)['label']
        print('IBM_KEY: {}'.format(random_choice))
        status = 'Ativo'

        if random_choice == 'A':
            response = service.analyze(text=replyText, features=Features(sentiment=SentimentOptions()),language='pt').get_result()
        elif random_choice == 'B':
            response = service_two.analyze(text=replyText, features=Features(sentiment=SentimentOptions()),
                                       language='pt').get_result()
        elif random_choice == 'C':
            response = service_three.analyze(text=replyText, features=Features(sentiment=SentimentOptions()),
                                       language='pt').get_result()
        elif random_choice == 'D':
            response = service_four.analyze(text=replyText, features=Features(sentiment=SentimentOptions()),
                                       language='pt').get_result()
        else:
            response = service_five.analyze(text=replyText, features=Features(sentiment=SentimentOptions()),language='pt').get_result()
        sentiment = response['sentiment']['document']['label']
    except Exception as ex:
        # if str(ex.code) == '403':
        status = 'Inativo'
        sentiment = 'não classificado'

    update_ibm_key(cur, conn, status, random_choice)
    return sentiment
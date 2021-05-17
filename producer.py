# bibliotecas
from __future__ import unicode_literals
from datetime import datetime
import tweepy
from json import dumps
from kafka import KafkaProducer
import os
from tweepy.streaming import StreamListener
from tweepy import Stream
import json



# chaves de autenticação do twitter
# consumer_key = 'qw0doPiN5dJafZSTtwM312gJp'
# consumer_secret = 'pnqArL9J9hCICCRcsLTGpVg0nfzwolf0A0ttp53NCAQ8jaVNsc'
# access_token = '910302589685813248-gGJd9jHFsGBbybGkwx9LXNNZyCynPJB'
# access_token_secret = 'vw4wp3cTYLiVNuqFiduWQwHJU8l5ChPg42S2Z1Vc5rVs0'

consumer_key = os.environ.get("API_KEY")
consumer_secret = os.environ.get("API_SECRET_KEY")
access_token = os.environ.get("ACCESS_TOKEN")
access_token_secret = os.environ.get("ACCESS_TOKEN_SECRET")

# configuração da API twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


class StdOutListener(StreamListener):
  def on_data(self, tweet):
    tweet = json.loads(tweet)
    frase = str(tweet['text'])
    data_e_hora_completa = datetime.now()
    data_string = data_e_hora_completa.strftime('%Y-%m-%d %H:%M:%S')
    dados = {"tweet": frase, "horario": data_string}
    print("Sending to producer", dados)
    producer.send(topico, value=dados)
    return True

  def on_error(self, status):
    print (status)

broker = 'localhost:9092'
topico = 'tweets'
producer = KafkaProducer(bootstrap_servers=[broker],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

l = StdOutListener()
stream = Stream(auth, l)
stream.filter(track=["bolsonaro"])


# api = tweepy.API(auth)
# tweets = api.search('machine learning')

# # colhendo os dados conforme texto desejado
# for tweet in tweets:
#   frase = str(tweet.text)
#   data_e_hora_completa = datetime.now()
#   data_string = data_e_hora_completa.strftime('%Y-%m-%d %H:%M:%S')
#   dados = {"tweet": frase, "horario": data_string}
#   print("Sending to producer", dados)
#   producer.send(topico, value=dados)
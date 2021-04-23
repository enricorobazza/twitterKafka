# importando as bibliotecas
from kafka import KafkaConsumer
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import json
from IPython.display import clear_output
from predict import SentimentPrediction
from googletrans import Translator

#configuração do kafka
brokers = ['localhost:9092']
topic = 'tweets'
pred = SentimentPrediction()
translator = Translator()
print("Finished training")
consumer = KafkaConsumer(topic, group_id = None, bootstrap_servers = brokers)

positivo = 0
negativo = 0

#geração da nuvem de palavras em tempo real
frases = ''
for messagem in consumer:
    texto = json.loads(messagem.value.decode('utf-8'))
    frase = translator.translate(texto['tweet'], dest='pt').text
    print(frase)
    # print(texto['tweet'])
    clear_output()
    resp = pred.predict_text(frase)
    if resp == 1:
        positivo += 1
    else:
        negativo += 1

    print("Positivo: %d, Negativo: %d"%(positivo, negativo))
    # print(pred.predict_text(texto['tweet']))



    # wordcloud = WordCloud(max_font_size=100, width = 1520, height = 535).generate(frases)
    # plt.figure(figsize=(16,9))
    # plt.imshow(wordcloud)
    # plt.axis("off")
    # plt.show()
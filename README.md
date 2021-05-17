# Twitter Kafka

This is a Python project that utilizes Twitter API for reading streams of tweets in real time and posting to a Kafka Topic to have the text analysed through a linear regression algorithm that identifies if the text has a positive or negative sentiment.

## Tutorial:

1 - Subir o kafka e o zookeeper:

```
    docker-compose up
```

2 - Criar um tópico:

```
    docker-compose exec kafka kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic tweets
```

3- Testar tópico:

```
    docker-compose exec kafka kafka-console-producer --broker-list localhost:9092 --topic tweets
```

```
    docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic tweets --from-beginning
```

https://elkhayati.me/kafka-python-twitter/
https://medium.com/@cicerojmm/processamento-e-an%C3%A1lise-de-dados-em-tempo-real-com-kafka-e-python-952be439b0fb
https://colab.research.google.com/drive/13vrRaAInfa9umoDLUTvRDFFsmtzzd74T?authuser=1#scrollTo=C5mxorlWDSEB

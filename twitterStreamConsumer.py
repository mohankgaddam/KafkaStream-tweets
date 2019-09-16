import json
from kafka import KafkaConsumer


def main():

    consumer = KafkaConsumer("tweet")
    for msg in consumer:
        print(msg.value)


if __name__ == "__main__":

   main()
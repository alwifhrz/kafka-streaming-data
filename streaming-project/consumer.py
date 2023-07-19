from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'finnhub',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer= lambda x: loads(x.decode('utf-8')),
    group_id='consumer-id-1')

while True :
    for message in consumer:
        message_raw = message.value
        msg = loads(message_raw)
        if msg['type'] == 'trade' :
            for res in msg['data'] :
                print(res)
import json, time, random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

print("Iniciando productor de pedidos Olist...")
while True:
    pedido = {
        "id_pedido": random.randint(1000, 9999),
        "estado": random.choice(["SP", "RJ", "MG", "RS"]),
        "valor": round(random.uniform(50.0, 500.0), 2)
    }
    producer.send('topic_olist', pedido)
    print("Enviado:", pedido)
    time.sleep(2)

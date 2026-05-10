from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction(counter):
    # TWOJ KOD
    return {
        "tx_id": f"TX{counter:04d}",
        "user_id": random.choice([f"u{i:02d}" for i in range(1, 6)]),
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(["Warszawa", "Krakow", "Gdansk", "Wroclaw"]),
        "category": random.choice(["elektronika", "odziez", "zywnosc", "ksiazki"]),
        "timestamp": datetime.now().isoformat()
    }

# TWOJ KOD
# Petla: generuj transakcje, wyslij do tematu 'transactions', wypisz, sleep 1s
counter = 1
print("Producent uruchomiony...")
while True:
    tx = generate_transaction(counter)
    producer.send('transactions', tx)
    print(f"Wyslano: {tx['tx_id']} | {tx['user_id']} | {tx['amount']} PLN | {tx['store']}")
    counter += 1
    time.sleep(1)

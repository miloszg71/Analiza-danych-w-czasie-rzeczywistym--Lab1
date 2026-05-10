from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity_detector',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# TWÓJ KOD - słownik przechowujący listę timestampów per user_id
user_tx_times = defaultdict(list)

WINDOW_SECONDS = 60
MAX_TX_IN_WINDOW = 3

print("Nasłuchuję na anomalie prędkości transakcji...")

for message in consumer:
    tx = message.value

    # TWÓJ KOD - wyciągnij pola z transakcji
    user_id = tx["user_id"]
    tx_id   = tx["tx_id"]
    amount  = tx["amount"]
    store   = tx["store"]
    ts_iso  = tx["timestamp"]

    now = time.time()

    # Dodaj aktualny timestamp do historii użytkownika
    user_tx_times[user_id].append(now)

    # Usuń zdarzenia starsze niż WINDOW_SECONDS (przesuwające się okno czasowe)
    user_tx_times[user_id] = [
        t for t in user_tx_times[user_id]
        if now - t <= WINDOW_SECONDS
    ]

    tx_count = len(user_tx_times[user_id])

    print(f"[INFO] {tx_id} | {user_id} | {amount:.2f} PLN | {store} | tx w oknie: {tx_count}")

    # Jeśli liczba transakcji w oknie przekracza próg → ALERT
    if tx_count > MAX_TX_IN_WINDOW:
        print(f"ALERT: {user_id} wykonał {tx_count} transakcji w ciągu {WINDOW_SECONDS}s! | ostatnia: {tx_id} | {amount:.2f} PLN | {store} | {ts_iso}")

#!/usr/bin/env python3
"""Generateur de commandes synthetiques pour Ecosysteme Big Data 2.

Exemples :
  python producer.py --mode file --n 50000
  python producer.py --mode kafka --rate 20 --max-events 100
  python producer.py --mode kafka --rate 20

Schema commun : order_id, restaurant_id, customer_id, city, status,
amount_mad, items_count, distance_km, courier_id, event_time.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DEFAULT_RESTAURANTS = ROOT / "data" / "reference" / "restaurants.json"
DEFAULT_OUTPUT = ROOT / "data" / "generated" / "orders_history.jsonl"
STATUSES = ("CREATED", "PICKED_UP", "DELIVERED", "CANCELLED")
STATUS_WEIGHTS = (0.15, 0.15, 0.55, 0.15)
LATE_EVENT_RATE = 0.05
DUPLICATE_EVENT_RATE = 0.02


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_restaurants(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"Referentiel introuvable : {path}")
    with path.open("r", encoding="utf-8") as file:
        restaurants = json.load(file)
    if not isinstance(restaurants, list) or len(restaurants) != 200:
        raise ValueError("restaurants.json doit contenir exactement 200 objets.")
    required = {"restaurant_id", "name", "city", "category", "lat", "lon"}
    ids: set[str] = set()
    for index, restaurant in enumerate(restaurants, start=1):
        if not isinstance(restaurant, dict):
            raise ValueError(f"Restaurant #{index} : objet JSON attendu.")
        missing = required - restaurant.keys()
        if missing:
            raise ValueError(f"Restaurant #{index} : champs manquants : {', '.join(sorted(missing))}")
        restaurant_id = str(restaurant["restaurant_id"])
        if restaurant_id in ids:
            raise ValueError(f"Identifiant restaurant duplique : {restaurant_id}")
        ids.add(restaurant_id)
    return restaurants


def build_order(restaurant: dict[str, Any], number: int, event_time: datetime,
                rng: random.Random) -> dict[str, Any]:
    status = rng.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    courier_id = None if status in {"CREATED", "CANCELLED"} else f"COURIER-{rng.randint(1, 80):03d}"
    return {
        "order_id": f"ORD-{number:08d}",
        "restaurant_id": restaurant["restaurant_id"],
        "customer_id": f"CUSTOMER-{rng.randint(1, 5000):05d}",
        "city": restaurant["city"],
        "status": status,
        "amount_mad": round(rng.uniform(25.0, 450.0), 2),
        "items_count": rng.randint(1, 8),
        "distance_km": round(rng.uniform(0.5, 20.0), 2),
        "courier_id": courier_id,
        "event_time": iso_utc(event_time),
    }


def maybe_late(reference_time: datetime, rng: random.Random) -> datetime:
    if rng.random() < LATE_EVENT_RATE:
        return reference_time - timedelta(seconds=rng.randint(60, 15 * 60))
    return reference_time


def generate_history(restaurants: list[dict[str, Any]], count: int,
                     output: Path, seed: int) -> None:
    if count < 1:
        raise ValueError("--n doit etre superieur ou egal a 1.")
    rng = random.Random(seed)
    duplicate_count = round(count * DUPLICATE_EVENT_RATE)
    unique_count = count - duplicate_count
    start = datetime.now(timezone.utc) - timedelta(days=30)
    duration = 30 * 24 * 60 * 60
    duplicate_indexes = set(rng.sample(range(unique_count), k=duplicate_count)) if duplicate_count else set()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as file:
        for index in range(unique_count):
            simulated_time = start + timedelta(seconds=(index / max(unique_count - 1, 1)) * duration)
            order = build_order(rng.choice(restaurants), index + 1, maybe_late(simulated_time, rng), rng)
            line = json.dumps(order, ensure_ascii=False, separators=(",", ":"))
            file.write(line + "\n")
            if index in duplicate_indexes:
                file.write(line + "\n")
    print(f"Fichier genere : {output}")
    print(f"Lignes ecrites : {count:,}".replace(",", " "))
    print(f"Evenements uniques : {unique_count:,}".replace(",", " "))
    print(f"Doublons exacts ajoutes : {duplicate_count:,}".replace(",", " "))
    print("Periode simulee : 30 jours ; evenements en retard : environ 5 %")


def publish_to_kafka(restaurants: list[dict[str, Any]], bootstrap_servers: str,
                     topic: str, rate: float, seed: int, max_events: int | None) -> None:
    if rate <= 0:
        raise ValueError("--rate doit etre strictement positif.")
    if max_events is not None and max_events < 1:
        raise ValueError("--max-events doit etre superieur ou egal a 1.")
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("Installe la dependance avec : python -m pip install -r requirements-producer.txt") from exc

    rng = random.Random(seed)
    producer = KafkaProducer(
        bootstrap_servers=[item.strip() for item in bootstrap_servers.split(",") if item.strip()],
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all", retries=5, max_block_ms=10000,
    )
    sent = 0
    order_number = 0
    previous_event: dict[str, Any] | None = None
    interval = 1.0 / rate
    next_send = time.monotonic()
    print(f"Kafka : topic={topic}, broker(s)={bootstrap_servers}, debit cible={rate:g} msg/s")
    print("Arret : CTRL+C" if max_events is None else f"Maximum : {max_events} messages")
    try:
        while max_events is None or sent < max_events:
            if previous_event is not None and rng.random() < DUPLICATE_EVENT_RATE:
                event = previous_event.copy()
            else:
                order_number += 1
                now = datetime.now(timezone.utc)
                event = build_order(rng.choice(restaurants), order_number, maybe_late(now, rng), rng)
                event["order_id"] = f"ORD-{uuid.uuid4().hex[:12].upper()}"
                previous_event = event
            # Une meme cle (order_id) est envoyee vers la meme partition Kafka.
            producer.send(topic, key=event["order_id"], value=event).get(timeout=15)
            sent += 1
            if sent % 100 == 0:
                print(f"Messages confirmes : {sent}")
            next_send += interval
            delay = next_send - time.monotonic()
            if delay > 0 and (max_events is None or sent < max_events):
                time.sleep(delay)
            elif delay < -interval:
                next_send = time.monotonic()
    except KeyboardInterrupt:
        print("\nArret demande par l'utilisateur.")
    finally:
        producer.flush(timeout=15)
        producer.close(timeout=15)
        print(f"Publication terminee : {sent} message(s) confirme(s).")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generateur de commandes synthetiques du projet Big Data 2.")
    parser.add_argument("--mode", choices=("file", "kafka"), required=True)
    parser.add_argument("--n", type=int, default=50000, help="Nombre total de lignes JSONL, doublons inclus.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--restaurants", type=Path, default=DEFAULT_RESTAURANTS)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9094")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--rate", type=float, default=20.0, help="Messages par seconde en mode Kafka.")
    parser.add_argument("--max-events", type=int, default=None, help="Limite de messages Kafka ; CTRL+C sinon.")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    return args


def main() -> int:
    args = parse_args()
    try:
        restaurants = load_restaurants(args.restaurants)
        print(f"Referentiel charge : {len(restaurants)} restaurants")
        if args.mode == "file":
            generate_history(restaurants, args.n, args.output, args.seed)
        else:
            publish_to_kafka(restaurants, args.bootstrap_servers, args.topic,
                             args.rate, args.seed, args.max_events)
        return 0
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"ERREUR : {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERREUR inattendue : {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

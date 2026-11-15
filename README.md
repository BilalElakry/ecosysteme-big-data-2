# ecosysteme-big-data-2

## Partie 2 — Reproduire l’analyse RDD

Depuis la racine du dépôt, suivre les étapes 2 et 3 du [guide de lancement](README-partie1.md) pour préparer `.env.bilal`, construire les images et démarrer le cluster. Le service `minio-init` crée le bucket `lake` et termine avec le code 0.

### Alimenter la couche Bronze

Dans un terminal sur la machine hôte, générer l’historique, puis transférer le JSONL vers MinIO avec le client `mc` du conteneur. La commande `mc stat` affiche la taille de l’objet transféré.

```bash
python3 producer.py --mode file --n 50000
wc -l data/generated/orders_history.jsonl

docker compose --env-file .env.bilal exec -T minio sh -ec '
  mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
  mc pipe local/lake/bronze/history/orders_history.jsonl
  mc stat local/lake/bronze/history/orders_history.jsonl
' < data/generated/orders_history.jsonl
```

Dans JupyterLab, exécuter [le notebook RDD](notebooks/02_spark_core_rdd.ipynb). La cellule de lecture doit compter **50 000 événements** à l’adresse `s3a://lake/bronze/history/orders_history.jsonl`.

### Interpréter les résultats

Bronze conserve tous les événements, y compris les 1 000 doublons exacts introduits par le générateur. Le filtre `DELIVERED` retient **27 360 événements de livraison**, doublons inclus. Chaque occurrence contribue au chiffre d’affaires de **6 514 937,82 MAD**, au panier moyen par ville et au classement des restaurants. Une même commande peut donc contribuer plusieurs fois à ces indicateurs.

Le nettoyage prévu en couche Silver dédoublonnera les événements sur la paire `(order_id, status)` afin de conserver les différents changements de statut d’une commande.

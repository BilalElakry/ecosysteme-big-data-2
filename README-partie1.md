# Partie 1 — Cluster Spark et MinIO de Bilal

À lancer sur Fedora, depuis la racine du dépôt. Ce socle se teste avant l’intégration de Kafka. Utiliser Docker Engine avec le plugin Docker Compose, Bash, openssl et Git. Sur un poste disposant de 8 Gio de RAM, fermer les autres applications lourdes. Le premier build télécharge Spark et les JAR S3A et nécessite Internet et plusieurs Gio de disque libre.

## 1. Préparer la branche et les fichiers

Dans un clone existant et propre, exécuter `git switch main`, `git pull --ff-only` puis `git switch -c feature/bilal-spark-batch-graphx`. Si la branche existe, utiliser `git switch feature/bilal-spark-batch-graphx`. Si nécessaire, cloner d’abord votre dépôt GitHub commun.

Extraire l’archive dans le dépôt (`unzip -n /chemin/Partie1_Bilal.zip`). Si le dépôt possède déjà un docker-compose.yml, extraire dans un dossier temporaire, copier le notebook, infra/bilal et infra/minio, et fusionner les sections services, volumes et x-* du Compose avec celles de Haitam. Conserver une seule clé services et une seule clé volumes. Le réseau par défaut suffit quand tous les services appartiennent au même projet Compose.

## 2. Configurer la machine

Vérifier que `docker version` affiche Client et Server et que `docker compose version` fonctionne. Pour Docker Engine installé avec systemd, `sudo systemctl enable --now docker` démarre le moteur. Préparer le fichier local une fois, en tant qu’utilisateur habituel.

```bash
if [ ! -e .env.bilal ]; then
  (umask 077
   printf 'LAB_UID=%s\nLAB_GID=%s\nMINIO_ROOT_USER=bilaladmin\nMINIO_ROOT_PASSWORD=%s\n' \
     "$(id -u)" "$(id -g)" "$(openssl rand -hex 24)" > .env.bilal)
fi
for entry in .env.bilal .ipynb_checkpoints/; do
  grep -qxF "$entry" .gitignore 2>/dev/null || echo "$entry" >> .gitignore
done
dc() { docker compose --env-file .env.bilal "$@"; }
```

Redéfinir la fonction dc dans tout nouveau terminal. Le fichier .env.bilal contient les identifiants de cette instance locale. Si l’accès au socket Docker nécessite sudo, utiliser sudo docker compose dans la fonction dc. Garder les commandes de fichiers et de Git sous l’utilisateur habituel.

## 3. Démarrer le socle

```bash
dc config --quiet
dc build spark-master
dc build minio
dc up -d spark-master spark-worker-1 spark-worker-2 minio minio-init jupyter
dc ps -a
dc logs minio-init
```

Le build du Master construit l’image commune au Master, aux Workers et à Jupyter. Le Dockerfile installe la distribution Spark 3.5.8 avec Scala 2.12, Java 17, Python 3.11, JupyterLab 4.4.10 et les JAR hadoop-aws 3.3.4 et aws-java-sdk-bundle 1.12.262. Le fichier spark-defaults.conf fournit le Master, l’adresse du Driver et les paramètres S3A. Les deux Workers annoncent chacun un cœur et 1 Gio à Spark. L’application demande un cœur et 512 Mio par executor et deux cœurs au total.

Le build minio compile le serveur MinIO (RELEASE.2025-10-15T17-29-55Z) et le client mc (RELEASE.2025-08-13T08-35-41Z) depuis leurs dépôts GitHub officiels. Les services minio et minio-init partagent cette image locale. Prévoir plusieurs minutes, une connexion à Docker Hub, GitHub et au proxy de modules Go, ainsi que plusieurs Gio de disque pour la compilation.

MinIO conserve /data dans le volume lake-data. Le service ponctuel minio-init attend MinIO, configure l’alias local puis exécute mc mb --ignore-existing local/lake. Il doit terminer avec le code 0. Les cinq services de longue durée restent actifs.

## 4. Contrôler les interfaces et exécuter le notebook

| Interface | Adresse sur Fedora | Contrôle |
|---|---|---|
| Master | http://localhost:8080 | 2 Workers ALIVE, 2 cœurs au total |
| MinIO Console | http://localhost:9001 | Bucket lake visible avec les identifiants de .env.bilal |
| JupyterLab | http://localhost:8888/lab | Récupérer le token avec dc exec jupyter jupyter server list |
| Spark UI de l’application | http://localhost:4040 | Jobs, Stages et Executors pendant la session |

Le port 9000 sert à l’API S3, le 9001 au navigateur. Dans les conteneurs, Spark contacte http://minio:9000 et le Driver annonce jupyter aux Workers. Depuis JupyterLab, ouvrir 01_validation_cluster.ipynb et lancer les cellules avec Maj+Entrée. Le montage :Z rend le dossier des notebooks accessible au conteneur Jupyter sur Fedora avec SELinux.

Le calcul RDD doit retourner 500500. Les quatre valeurs attendues sont 3.5.8, spark://spark-master:7077, Bilal-Partie1-Validation et normalement 2 pour sc.defaultParallelism avec cette configuration. Ce dernier exprime un parallélisme par défaut. Contrôler directement les ressources dans le Master et les executors dans l’onglet Executors. Le test de stockage doit relire 100 lignes et une somme de 5050 dans s3a://lake/validation/partie1/nombres. Enregistrer le notebook et faire les captures avant spark.stop().

Dans le bucket lake, réserver les préfixes bronze/orders_raw/, silver/orders_clean/, gold/kpi_city_10min/ et checkpoints/ pour le pipeline. En Partie 2, l’historique ira sous bronze/history/. Les préfixes apparaissent en console à l’écriture des objets. Le test Partie 1 utilise uniquement validation/.

## 5. Expliquer le déploiement dans le rapport (une page maximum)

- Le Driver de l’application tourne côté Jupyter en mode client. Il construit le plan des calculs et coordonne l’exécution des tâches.
- Le SparkContext, accessible par spark.sparkContext, relie l’application au Master et soumet les jobs lorsque le notebook déclenche une action.
- Le Master standalone joue le rôle de Cluster Manager et attribue les ressources annoncées par les Workers.
- Les Workers lancent les processus Executors. Ces derniers exécutent les tâches sur les partitions, conservent éventuellement des données en cache et renvoient les résultats au Driver.
- MapReduce matérialise les résultats entre jobs successifs. Pour les futurs calculs itératifs de PageRank, Spark peut garder en cache les données réutilisées et réduire ces entrées/sorties.
- Relancer des jobs MapReduce batch pour chaque petit lot de commandes induit une latence de démarrage. Structured Streaming maintient une requête active et traite les événements Kafka par micro-lots, ce qui convient au suivi des commandes.

## 6. Captures, Git et intégration

Conserver une capture du Master (2 Workers ALIVE), du bucket lake et du notebook exécuté. L’onglet Executors et le préfixe validation/ complètent la preuve. Garder hors champ les jetons et mots de passe. Après les captures, arrêter la session via spark.stop(). Pour arrêter la stack utiliser dc down. Le volume MinIO reste disponible au prochain démarrage depuis le même projet Compose. L’option -v supprimerait ce volume.

```bash
git status --short
git add .gitignore docker-compose.yml infra/bilal infra/minio README-partie1.md notebooks/01_validation_cluster.ipynb
git diff --cached --stat
git commit -m "Configure Spark standalone, Jupyter and MinIO for part 1"
git push -u origin feature/bilal-spark-batch-graphx
```

Créer une PR vers main. Pour l’intégration, garder le même réseau Compose et convenir avec Haitam du nom de service Kafka et de son listener interne (par exemple kafka:9092). Quand sa configuration est fusionnée, exécuter dc up -d, puis vérifier ensemble le résultat de la commande kafka-topics de son image avec --bootstrap-server kafka:9092 --describe --topic orders, en adaptant le listener convenu. Attendre PartitionCount: 3. Cela complète la validation de la Partie 1 du binôme.

## Diagnostic rapide

| Symptôme | Premier contrôle |
|---|---|
| Port 8080 déjà pris | Sur Fedora, ss -ltnp. Si un ancien Spark local occupe 8080, arrêter cette instance avant de relancer le Compose. |
| Aucun Worker enregistré | dc logs --tail=80 spark-master spark-worker-1 spark-worker-2. Chercher l’enregistrement auprès du Master. |
| Job en attente de ressources | Ouvrir le Master et vérifier si un autre notebook occupe les deux cœurs. Libérer sa session avec spark.stop(). |
| Master affiché local[*] ou local[2] | Redémarrer le kernel Jupyter, puis exécuter la cellule fournie en premier. |
| Accès S3 refusé | dc logs minio-init. Vérifier que Workers, Jupyter et MinIO utilisent les valeurs du même .env.bilal. |
| Notebook en lecture seule | Comparer id -u et id -g aux valeurs de .env.bilal, vérifier les droits du dossier et le suffixe :Z, puis reconstruire l’image si les UID/GID ont changé. |

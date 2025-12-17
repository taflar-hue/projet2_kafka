Projet 2 — Système de Streaming Kafka
1. Description du projet

Ce projet implémente un pipeline temps réel basé sur Kafka, de la simulation de flux jusqu’au stockage et à la visualisation.

Fonctionnalités :

Producteur Python simulant des flux continus (capteurs, logs, tweets…)

Consommateur Kafka : lecture continue, filtrage, parsing JSON, agrégations légères

Stockage dans InfluxDB (base de données séries temporelles)

Visualisation via matplotlib ou dashboard Grafana

2. Architecture
Producteur Python --> Kafka --> Consommateur Python --> InfluxDB
                                            |
                                            v
                                        Visualisation (Grafana / Matplotlib)

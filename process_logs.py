# Parser avec Drain3
import logging
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

# Configuration de Drain3
config = TemplateMinerConfig()

# Définir les paramètres directement pour Drain3
config.snapshot_interval_minutes = 1
config.persist_snapshot = True
config.persist_state = True
config.log_level = logging.INFO
config.tree_max_depth = 4
config.min_similarity_threshold = 0.4
config.max_clusters = 10000

persistence = FilePersistence("drain3_state.bin")
template_miner = TemplateMiner(persistence, config)

# Configuration du logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Fonction pour lire les journaux et les parser avec Drain3
def parse_logs(file_path):
    """
    Cette fonction lit les journaux depuis le fichier spécifié par file_path
    et les parse en utilisant Drain3.
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()

    for line in lines:
        result = template_miner.add_log_message(line)
        if result["change_type"] != "none":
            # Vérifie si la clé 'template' existe dans le résultat
            if 'template' in result:
                logger.info(f"New template: {result['template']}")
            else:
                logger.warning(f"Template not found in result: {result}")
            
            # Vérifie si la clé 'clusters' existe dans template_miner.drain
            if hasattr(template_miner.drain, 'clusters'):
                logger.info(f"Clusters: {len(template_miner.drain.clusters)}")
            else:
                logger.warning("Clusters attribute not found in template_miner.drain")

            # Affiche le nombre de clusters
            logger.info(f"Clusters: {len(template_miner.drain.clusters)}")

    return template_miner

file_path = "hadoop_logs.txt"

# Parse les journaux Hadoop
template_miner = parse_logs(file_path)

# Sauvegarde les résultats
template_miner.save_state("Periodic save")

#Générer la matrice d'événements
import pandas as pd
import numpy as np

# Fonction pour convertir les logs en une matrice d'événements
def logs_to_event_matrix(template_miner, logs):
    event_matrix = []
    for log in logs:
        result = template_miner.match(log)
        cluster_id = result.cluster_id # cluster_id = result['cluster_id']
        event_matrix.append(cluster_id)

    unique_events = sorted(set(event_matrix))
    event_to_index = {event: idx for idx, event in enumerate(unique_events)}
    
    matrix = np.zeros((len(logs), len(unique_events)))

    for i, event in enumerate(event_matrix):
        matrix[i, event_to_index[event]] += 1

    return pd.DataFrame(matrix, columns=[f"Event_{event}" for event in unique_events])

# Lire les journaux et les transformer
with open("hadoop_logs.txt", 'r') as f:
    logs = f.readlines()

event_matrix_df = logs_to_event_matrix(template_miner, logs)
print(event_matrix_df)


# Calculer les statistiques de base
event_counts = event_matrix_df.sum(axis=0)
failure_events = event_matrix_df.columns[event_counts > 0]  # Supposons que les pannes sont identifiées par un certain seuil

print("Événements liés aux pannes:")
print(failure_events)

print("Distribution des pannes:")
print(event_counts[failure_events])


#Visualisation en graphe
import matplotlib.pyplot as plt

# Afficher un diagramme à barres des événements de panne
def plot_failure_distribution(event_counts):
    failure_events = event_counts.index
    counts = event_counts.values

    plt.figure(figsize=(10, 6))
    plt.bar(failure_events, counts)
    plt.xlabel('Événements')
    plt.ylabel('Nombre d\'occurrences')
    plt.title('Distribution des pannes')
    plt.xticks(rotation=45)
    plt.show()

# Afficher les statistiques des pannes
plot_failure_distribution(event_counts[failure_events])

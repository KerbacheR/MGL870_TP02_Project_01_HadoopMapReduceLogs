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

def process_log_line(line):
    """
    Traitement de chaque ligne du fichier journal.
    Vous pouvez adapter cette fonction pour le parser.
    """
    # Exemple de traitement : ici vous pouvez ajouter votre logique de parsing
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


import os

def parse_logs(logs_dir):
    """
    Parcourt tous les fichiers journaux dans les sous-répertoires de logs_dir
    et applique la fonction de traitement pour chaque ligne de chaque fichier.
    """
    # Utilisation de os.walk pour parcourir récursivement les sous-répertoires
    for root, dirs, files in os.walk(logs_dir):
        # Pour chaque fichier dans le répertoire courant
        for file in files:
            # On vérifie que le fichier correspond au format attendu (container_*.log)
            if file.startswith("container_") and file.endswith(".log"):
                file_path = os.path.join(root, file)
                print(f"Traitement du fichier : {file_path}")

                """
                Cette fonction lit les journaux depuis le fichier spécifié par file_path
                et les parse en utilisant Drain3.
                """
                with open(file_path, 'r') as f:
                    lines = f.readlines()

                    for line in lines:
                        # Traitez chaque ligne ici
                        process_log_line(line)

    return template_miner

logs_dir = '/Hadoop_logs'

# Parse les journaux Hadoop
template_miner = parse_logs(logs_dir);

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

    # Extraire les numéros d'événements à partir des chaînes (par exemple, 'Event_1' -> 1)
    event_numbers = [int(event.split('_')[1]) for event in failure_events]

    # Sélectionner les événements multiples de 5 (au lieu de 10)
    event_labels = [f'Event_{number}' for number in event_numbers if number % 5 == 0]
    event_positions = [failure_events[i] for i, number in enumerate(event_numbers) if number % 5 == 0]

    # Définir les étiquettes de l'axe x à des multiples de 5
    plt.xticks(event_positions, event_labels, rotation=45)

    plt.show()

# Afficher les statistiques des pannes
plot_failure_distribution(event_counts)

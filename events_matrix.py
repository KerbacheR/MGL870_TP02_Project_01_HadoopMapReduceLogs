import pandas as pd
import numpy as np

# Fonction pour convertir les logs en une matrice d'événements
def logs_to_event_matrix(template_miner, logs):
    event_matrix = []
    for log in logs:
        result = template_miner.match(log)
        cluster_id = result["cluster_id"]
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

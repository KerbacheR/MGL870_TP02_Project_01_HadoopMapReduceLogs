# Parser les journaux avec Drain3
import logging
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

# Configuration de Drain3
config = TemplateMinerConfig()

# Configuration de Drain3 avec ces paramètres
config.snapshot_interval_minutes = 1
config.persist_snapshot = True
config.persist_state = True
config.log_level = logging.INFO
config.tree_max_depth = 4
config.min_similarity_threshold = 0.4
config.max_clusters = 10000

# Expression régulière pour les journaux Hadoop
# 1. Remplace l'horodatage (format typique des journaux).
# 2. Remplace le niveau de journalisation (INFO, WARN, ERROR, etc.).
# 3. Remplace les parties dynamiques comme des identifiants ou des adresses spécifiques.
config.profiling_enabled = False  # Désactiver le profiling pour accélérer le traitement
config.custom_extractors = [
    {
        "regex": r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(,\d+)?",  # Horodatage
        "replacement": "<*>",
    },
    {
        "regex": r"(INFO|WARN|ERROR|DEBUG)",  # Niveau de journalisation
        "replacement": "<*>",
    },
    {
        "regex": r"appattempt_\d+_\d+_\d+",  # Identifiant dynamique
        "replacement": "<*>",
    },
]

persistence = FilePersistence("drain3_state.bin")
template_miner = TemplateMiner(persistence, config)

# Configuration du logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

import re
# Pre-processing
def preprocess_log_line(log_line):
    # Supprime l'horodatage
    log_line = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}", "", log_line)
    # Supprime le niveau de journalisation
    log_line = re.sub(r"\b(INFO|ERROR|WARN|DEBUG|TRACE)\b", "", log_line)
    # Supprime le thread
    log_line = re.sub(r"\[\w+\]", "", log_line)
    # Supprime les espaces inutiles
    log_line = re.sub(r"\s+", " ", log_line).strip()
    return log_line

def process_log_line(line):
    """
    Traitement de chaque ligne du fichier journal.
    """

    clean_line=preprocess_log_line(line)
    result = template_miner.add_log_message(clean_line)

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

# Spécifier le répertoire où se trouvent les fichiers journaux
logs_dir = 'Hadoop_logs'

# Parser(analyser) les journaux Hadoop, le résultat est l'objet template_miner
template_miner = parse_logs(logs_dir);

# Sauvegarder les résultats
template_miner.save_state("Periodic save")

#Générer la matrice d'événements
import pandas as pd

# Fonction pour générer la matrice des événements à partir de template_miner
def generate_event_matrix(template_miner):

    events = template_miner.drain.clusters # events contentiendra tous les événements traités.

    # Inspecter les attributs et méthodes disponibles dans template_miner
    print("Inspecter les attributs et méthodes disponibles dans template_miner : ")
    print(dir(template_miner))
    print("Fin de l'inspection")

    # Afficher les attributs internes de template_miner sous forme de dictionnaire
    print("Afficher les attributs internes de template_miner sous forme de dictionnaire")
    print(vars(template_miner))
    print("Fin de l'affichage des attributs sous forme de dictionnaire")
 
    # Supposons que les événements soient stockés dans `template_miner.clusters`
    print("Affichage des objets clusters")
    for cluster in template_miner.drain.clusters:
        print(cluster)  # Affiche l'objet cluster 
    print("fin de l'affichage des objets clusters")

    clusters_data = []  # Liste pour stocker les données extraites des clusters

    # Rassembler les données des clusters
    for cluster in events:
        clusters_data.append({
            "Cluster ID": cluster.cluster_id,
            "Size": cluster.size,
            "Template": cluster.get_template()
        })

    # Créer un DataFrame Pandas pour représenter la matrice des événements
    event_matrix = pd.DataFrame(clusters_data)
    
    # Enregistrer la matrice comme fichier CSV
    event_matrix.to_csv('event_matrix.csv', index=False)

    return event_matrix

# Utilisation de la fonction generate_event_matrix
event_matrix_df = generate_event_matrix(template_miner)

# Assurer que event_counts est une série numérique
event_counts = event_matrix_df.sum(axis=0)  # Calculer les sommes des événements par colonne
event_counts = pd.to_numeric(event_counts, errors='coerce')  # Convertir en numérique, NaN si non convertible

# Identifier les colonnes où le nombre d'événements est supérieur à 0
failure_events = event_counts[event_counts > 0].index
print(f"Les événements avec des occurrences (échec) : {failure_events}")


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

    # Extraire les numéros d'événements uniquement si le format est correct
    event_numbers = []
    for event in failure_events:
        try:
            # Extraire le numéro après "Event_"
            if "_" in event:
                event_numbers.append(int(event.split('_')[1]))
        except (IndexError, ValueError):
            # Ignorer les événements avec un format inattendu
            continue

    # Sélectionner les événements multiples de 5
    event_labels = [f'Event_{number}' for number in event_numbers if number % 5 == 0]
    event_positions = [failure_events[i] for i, number in enumerate(event_numbers) if number % 5 == 0]

    # Définir les étiquettes de l'axe x à des multiples de 5
    plt.xticks(event_positions, event_labels, rotation=45)

    plt.show()

# Afficher les statistiques des pannes
plot_failure_distribution(event_counts)

# Apprentissage avec modele de regression logistique
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, recall_score, roc_auc_score, precision_score
import numpy as np

# Affichage des premières lignes pour inspecter le DataFrame
print("DF headers")
print(event_matrix_df.head())

# Affichage de la structure complète du DataFrame
print("DF info")
print(event_matrix_df.info())


# Vérification des données préparées
print(event_matrix_df)

# Séparation des features et de la cible
X = event_matrix_df.drop(columns=['Event_100'])
y = event_matrix_df['Event_100']
# Suppression des corrélations élevées
corr_matrix = X.corr().abs()
upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
to_drop = [column for column in upper.columns if any(upper[column] > 0.9)]
X = X.drop(columns=to_drop)

# Séparation des jeux de données d'entraînement et de test avec stratification
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Entraînement du modèle de régression logistique
model = LogisticRegression(max_iter=1000)
model.fit(X_train, y_train)

# Prédictions sur les données de test
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

# Évaluation du modèle
precision1 = precision_score(y_test, y_pred)
recall1 = recall_score(y_test, y_pred)
accuracy1 = accuracy_score(y_test, y_pred)

# Calcul de l'AUC uniquement si les deux classes sont présentes dans y_test
if len(np.unique(y_test)) > 1:
    auc1 = roc_auc_score(y_test, y_pred_proba)
else:
    auc1 = None
    print("AUC score cannot be computed as only one class is present in y_test.")

print(f"Précision: {precision}")
print(f"Rappel: {recall}")
if auc is not None:
    print(f"AUC: {auc}")


# Apprentissage avec modele de forêt aléatoire (Random Forest)

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, recall_score, roc_auc_score, precision_score

# Séparation des features et de la cible
X = event_matrix_df.drop(columns=['Event_100'])
y = event_matrix_df['Event_100']

# Suppression des corrélations élevées

corr_matrix = X.corr().abs()
upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
to_drop = [column for column in upper.columns if any(upper[column] > 0.9)]
X = X.drop(columns=to_drop)

# Séparation des jeux de données d'entraînement et de test avec stratification
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Vérification des classes dans y_train et y_test
print("Classes dans y_train:", np.bincount(y_train))
print("Classes dans y_test:", np.bincount(y_test))

# Entraînement du modèle de forêt aléatoire
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Prédictions sur les données de test
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

# Évaluation du modèle
precision2 = precision_score(y_test, y_pred)
recall2 = recall_score(y_test, y_pred)
accuracy2 = accuracy_score(y_test, y_pred)

# Calcul de l'AUC uniquement si les deux classes sont présentes dans y_test
if len(np.unique(y_test)) > 1:
    auc2 = roc_auc_score(y_test, y_pred_proba)
else:
    auc2= None
    print("AUC score cannot be computed as only one class is present in y_test.")

print(f"Précision: {precision}")
print(f"Rappel: {recall}")
print(f"Exactitude: {accuracy}")
if auc is not None:
    print(f"AUC: {auc}")

# Comparaison des performances des deux modèles
print("Comparaison des performances des modèles :")
print(f"Précision de la régression logistique : {precision1}")
print(f"Précision de la forêt aléatoire : {precision2}")

print(f"Rappel de la régression logistique : {recall1}")
print(f"Rappel de la forêt aléatoire : {recall2}")

print(f"Exactitude de la régression logistique : {accuracy1}")
print(f"Exactitude de la forêt aléatoire : {accuracy2}")

if auc1 is not None and auc2 is not None:
    print(f"AUC de la régression logistique : {auc1}")
    print(f"AUC de la forêt aléatoire : {auc2}")
else:
    print("L'AUC ne peut pas être calculé pour l'un des modèles car une seule classe est présente dans y_test.")

# Variables(Événement) importants utilisant la regression logistique

from sklearn.linear_model import LogisticRegression
import numpy as np

# Créer et entraîner le modèle de régression logistique
log_reg = LogisticRegression(max_iter=1000)
log_reg.fit(X, y)

# Récupérer les coefficients
coefficients = log_reg.coef_[0]

# Afficher les coefficients avec les noms des événements
importance = pd.Series(coefficients, index=X.columns)
importance = importance.abs().sort_values(ascending=False)

print("Importance des variables selon la régression logistique :")
print(importance)


# Variables (Événement) importants utilisant foret aleatoire

from sklearn.ensemble import RandomForestClassifier

# Créer et entraîner le modèle de forêt aléatoire
rf = RandomForestClassifier(n_estimators=100)
rf.fit(X, y)

# Récupérer les importances des caractéristiques
importances = rf.feature_importances_

# Créer un DataFrame pour afficher l'importance des variables
importance_rf = pd.Series(importances, index=X.columns)
importance_rf = importance_rf.sort_values(ascending=False)

print("Importance des variables selon la forêt aléatoire :")
print(importance_rf)

#  Méthode de détection d'anomalies basée sur l'Isolation Forest
from sklearn.ensemble import IsolationForest

# Créer et entraîner un modèle Isolation Forest
iso_forest = IsolationForest(contamination=0.1)  # Estimation de la proportion d'anomalies
iso_forest.fit(X)

# Prédire les anomalies
y_pred = iso_forest.predict(X)

# Convertir -1 (anomalie) en 1 et 1 (normal) en 0
y_pred = [1 if label == -1 else 0 for label in y_pred]

# Comparer les prédictions avec les véritables anomalies
print("Prédictions d'anomalies :")
print(y_pred)

# Méthode de détection d'anomalies basée l'analyse en composantes principales (PCA) 
from sklearn.decomposition import PCA

# Appliquer PCA pour réduire la dimensionnalité
pca = PCA(n_components=2)
X_pca = pca.fit_transform(X)

# Visualiser les composants principaux
import matplotlib.pyplot as plt

plt.scatter(X_pca[:, 0], X_pca[:, 1], c=y, cmap='coolwarm')
plt.title("Projection des données sur les deux premiers composants principaux")
plt.show()

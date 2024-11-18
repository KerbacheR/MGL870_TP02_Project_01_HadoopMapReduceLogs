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

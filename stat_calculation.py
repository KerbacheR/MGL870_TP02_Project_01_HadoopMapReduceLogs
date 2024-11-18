# Calculer les statistiques de base
event_counts = event_matrix_df.sum(axis=0)
failure_events = event_matrix_df.columns[event_counts > 0]  # Supposons que les pannes sont identifiées par un certain seuil

print("Événements liés aux pannes:")
print(failure_events)

print("Distribution des pannes:")
print(event_counts[failure_events])

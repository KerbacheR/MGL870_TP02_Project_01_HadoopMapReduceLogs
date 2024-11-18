import logging
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence

# Configuration de Drain3
persistence = FilePersistence("drain3_state.bin")
template_miner = TemplateMiner(persistence)

# Configuration du logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Fonction pour lire les journaux et les parser avec Drain3
def parse_logs(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    for line in lines:
        result = template_miner.add_log_message(line)
        if result["change_type"] != "none":
            logger.info(f"New template: {result['template']}")
            logger.info(f"Clusters: {len(template_miner.drain.clusters)}")

    return template_miner

# Parse les journaux Hadoop
template_miner = parse_logs("hadoop_logs")

# Sauvegarde les résultats
template_miner.persistence.save_state()


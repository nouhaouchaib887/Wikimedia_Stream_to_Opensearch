import time
from kafka import KafkaProducer
from sseclient import SSEClient  # For handling server-sent events (SSE) from the Wikimedia stream

# Configuration du serveur Kafka et du topic
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'wikimedia.recentchange'

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: v.encode('utf-8')  # Sérialisation des valeurs en UTF-8
)

def send_to_kafka_with_retry():
    """
    Envoie les événements du flux Wikimedia RecentChange à un topic Kafka
    avec gestion des erreurs et limite de temps d'exécution.
    """
    # URL du flux Wikimedia
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    client = SSEClient(url)  # Connexion au flux SSE

    # Définir une durée maximale d'exécution (1 minute)
    start_time = time.time()
    max_duration = 60  # Limite de 60 secondes

    print("Commence à écouter le flux Wikimedia...")

    for event in client:
        # Vérifier si la limite de temps est dépassée
        if time.time() - start_time > max_duration:
            print("Limite de temps atteinte. Arrêt du flux.")
            break

        try:
            # Vérifier que l'événement a des données valides
            if event.data:
                # Envoyer les données au topic Kafka
                producer.send(TOPIC, value=event.data)
                print(f"Événement envoyé à Kafka : {event.data[:100]}...")  # Affiche un extrait des données
            else:
                print("Événement ignoré (données vides ou None)")
        except Exception as e:
            print(f"Erreur lors de l'envoi de l'événement : {e}")
            # Retenter l'envoi après un délai en cas d'erreur
            time.sleep(1)

    print("Fin du traitement du flux Wikimedia.")

# Exécution du producteur Kafka avec fermeture propre
if __name__ == "__main__":
    try:
        send_to_kafka_with_retry()
    except KeyboardInterrupt:
        print("Flux interrompu par l'utilisateur.")
    finally:
        producer.close()
        print("Producteur Kafka fermé.")

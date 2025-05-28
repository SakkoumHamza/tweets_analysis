import tweepy # Pour connecter  a lapi X (Twitter)
from kafka import KafkaProducer 
import json # Pour encoder les tweets en JSON
import time 

# Configuration des clés API Twitter
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAItT2AEAAAAArp%2F97zesHukzMBLwkcWO8cJumBo%3Dno69HGlH1o7dSQoqJcx291uE0pQOjIwoAG4hfl1ApTksHolyLy'

# Initialisation du client Twitter (v2)
client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8') #json.dumps convert a subset of Python objects into a json string et encode utf-8 converti en bytes pour que kafa y comprenne
)
# Méthode 1 : Utiliser la classe StreamingClient necessite un plan API v2 developer Payant
    # # Classe pour écouter les tweets en temps réel
    # class TwitterStream(tweepy.StreamingClient):
    #     def on_tweet(self, tweet):
    #         # Filtrer les retweets et les tweets courts
    #         if not tweet.text.startswith("RT") and len(tweet.text) > 10: # On ignore les retweets pour eviter les doublons et le bruit dans les données
    #             data = {
    #                 'id': tweet.id,
    #                 'text': tweet.text,
    #                 'author_id': tweet.author_id,
    #                 'lang': tweet.lang,
    #             }
    #             print(f"Tweet capturé : {data['text']}")
    #             # Envoi vers Kafka topic "tweets"
    #             producer.send('tweets', value=data)

    #     def on_errors(self, errors):
    #         print(f"Erreur : {errors}")

# Méthode 2 :  Fonction de recherche des tweets (polling régulier)
def search_and_send_to_kafka(query, max_results=10):
    response = client.search_recent_tweets(
        query=query,
        max_results=max_results,
        tweet_fields=["author_id", "created_at", "lang"],
    )

    if response.data:
        for tweet in response.data:
            # Ignorer les retweets et les tweets courts
            if not tweet.text.startswith("RT") and len(tweet.text) > 10:
                data = {
                    'id': tweet.id,
                    'text': tweet.text,
                    'author_id': tweet.author_id,
                    'lang': tweet.lang,
                }
                print(f"Tweet capturé : {data['text']}")
                producer.send('tweets', value=data)
# Lancer le stream
if __name__ == "__main__":

# Methode 2
    query = "diabetes OR #santé OR maladie lang:fr"

    print("🔄 Démarrage du polling Twitter (mode gratuit)...")
    while True:
        try:
            search_and_send_to_kafka(query=query, max_results=10)
            time.sleep(120)  # Attendre 30 secondes avant la prochaine requête
        except Exception as e:
            print(f"❌ Erreur : {e}")
            time.sleep(240)  # En cas d'erreur, attendre un peu plus

    # Methode 1 
#     stream = TwitterStream(bearer_token=BEARER_TOKEN)

#     # Supprimer les règles existantes
#     rules = stream.get_rules().data
#     if rules:
#         rule_ids = [rule.id for rule in rules]
#         stream.delete_rules(rule_ids)

#     # Ajouter une règle (par exemple, mots-clés liés à la santé)
#     stream.add_rules(tweepy.StreamRule("diabetes OR #santé OR maladie"))

#     print("Streaming des tweets...")
#     stream.filter(
#     tweet_fields=["author_id", "created_at", "lang", "public_metrics"],
#     expansions=["author_id"],
#     user_fields=["username", "public_metrics"]
# )

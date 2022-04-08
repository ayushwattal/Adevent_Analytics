from google.cloud import pubsub
subscriber = pubsub.SubscriberClient()

topic = "enriched_data_out"
subs = "enriched_data_out-sub"
project = "advertising-analytics-grp5"


subs_path = subscriber.subscription_path(project, subs)
topic_path = pubsub.PublisherClient.topic_path(project, topic)

print(f"Listening to pub sub topic {topic_path}...\n")

def callback(message):
    print(message.data.decode("utf-8"))
    message.ack()


future = subscriber.subscribe(subs_path, callback)
try:
    future.result()
except KeyboardInterrupt:
    future.cancel()

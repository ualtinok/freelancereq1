mvn compile exec:java \
  -Dexec.mainClass=com.examples.pubsub.streaming.PubSubToBigQuery \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.args=" \
    --project=$PROJECT_NAME \
    --inputSubscription=projects/$PROJECT_NAME/subscriptions/testsub \
    --runner=DataflowRunner"

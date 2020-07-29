async function main(topicName = 'YOUR_TOPIC_NAME') {
    // [START pubsub_create_topic]
    /**
     * TODO(developer): Uncomment this variable before running the sample.
     */
        // const topicName = 'YOUR_TOPIC_NAME';

        // Imports the Google Cloud client library
    const {PubSub} = require('@google-cloud/pubsub');

    // Creates a client; cache this for further use
    const pubSubClient = new PubSub();

    async function createTopic() {
        // Creates a new topic
        await pubSubClient.createTopic(topicName);
        console.log(`Topic ${topicName} created.`);
    }

    createTopic();
    // [END pubsub_create_topic]
}

main(...process.argv.slice(2)).catch(e => {
    console.error(e);
    process.exitCode = -1;
});

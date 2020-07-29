#Requirements

* You must have a [gcp](https://console.cloud.google.com/) project.

* Install [pubsub emulator](https://cloud.google.com/pubsub/docs/emulator). Exporting env variables(PUBSUB_EMULATOR_HOST and PUBSUB_PROJECT_ID) is critical.

* Create topic and subscription
```
cd fluentdToBeam/nodejs
npm install
node createtopic.js testtopic
node createsubs.js testtopic testsub
```

* Install [fluentd](https://www.fluentd.org/).

* Clone [Avro Schema Registry](https://github.com/salsify/avro-schema-registry). Copy ``runAvroSchemaRegistry.sh`` to avro-schema-registry folder, run that file. Then run ``node registerSchemas.js``. Avro schema registry should be working now.

* Edit **project**, **buffer_path** and **key** in *./fluentd/fluent.conf*

* Start fluentd 
```
fluentd -c ./fluentd/fluent.conf -p ./fluentd/plugin -vv
```

* Run pipeline
```
cd fluentdToBeam/beam
./start.sh
```

* Emit mock data
```
cd fluentdToBeam/nodejs
node emitter.js
```

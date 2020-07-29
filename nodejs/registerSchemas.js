const axios = require('axios');
const fs = require('fs');

const schema_registry_url = 'http://supersecret:avro@localhost:5000';


const readerSchema = fs.readFileSync('../schemas/org/be/beam/avroSchema/AvroReader.avsc', 'utf8');
const writerSchema = fs.readFileSync('../schemas/org/be/beam/avroSchema/AvroWriter.avsc', 'utf8');


axios.post(`${schema_registry_url}/subjects/avro_reader_schema/versions`,
    {
            schema: readerSchema,
            with_compatibility: "NONE",
            after_compatibility: "BACKWARD"
    },
    {headers: { Accept: "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"}})
.then((res) => {
    console.log(`Reader response status: ${res.status}`)
})
.catch((error) => {
    console.error(error)
});

axios.post(`${schema_registry_url}/subjects/avro_writer_schema/versions`,
    {
        schema: writerSchema,
        with_compatibility: "NONE",
        after_compatibility: "BACKWARD"
    },
    {headers: { Accept: "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json"}})
.then((res) => {
    console.log(`Writer response status: ${res.status}`)
})
.catch((error) => {
    console.error(error)
});

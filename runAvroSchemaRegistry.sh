#!/bin/bash

echo MUST BE COPIED TO avro-schema-registry FOLDER

docker build . -t avro-schema-registry

docker run --name avro-postgres -d \
  -e POSTGRES_PASSWORD=avro \
  -e POSTGRES_USER=avro \
  postgres:9.6

docker run --name avro-schema-registry --link avro-postgres:postgres -p 5000:5000 -d \
  -e DATABASE_URL=postgresql://avro:avro@postgres/avro \
  -e FORCE_SSL=false \
  -e SECRET_KEY_BASE=supersecret \
  -e SCHEMA_REGISTRY_PASSWORD=avro \
  avro-schema-registry


sleep 5s

docker exec avro-schema-registry bundle exec rails db:setup

echo REGISTER NEW SCHEMAS WITH registerSchemas.js NOW

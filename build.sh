#!/bin/bash

echo "Building trino-db2 plugins"
docker build --tag trino-db2-builder .

echo "Copying trino-db2 output"

id=$(docker create trino-db2-builder)
docker cp $id:/trino-db2/target/trino-db2-422.zip ./build >/dev/null
docker rm -v $id

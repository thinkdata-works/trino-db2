FROM docker.io/library/maven:3.6.3-openjdk-17 AS builder
WORKDIR /trino-db2
COPY . /trino-db2
RUN mvn install -DskipTests -Dair.check.skip-all=true -Dmaven.javadoc.skip=true -B -T C1

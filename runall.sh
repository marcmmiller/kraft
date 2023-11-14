#!/bin/bash

./gradlew buildFatJar || exit 1

java -jar app/build/libs/app-all.jar 8080 8081 8082 &
P1=$!

java -jar app/build/libs/app-all.jar 8081 8080 8082 &
P2=$!

java -jar app/build/libs/app-all.jar 8082 8080 8081 &
P3=$!

trap "kill -TERM $P1 $P2 $P3" EXIT

wait



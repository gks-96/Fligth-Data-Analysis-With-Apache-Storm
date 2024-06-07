#!/bin/sh
/Applications/apache-maven-3.9.1/bin/mvn exec:java -Dexec.mainClass="TopologyMain" -Dexec.args="src/main/resources/flights.txt src/main/resources/airports.txt"

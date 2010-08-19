#!/bin/sh
mvn -npu clean assembly:assembly
# eclipse:eclipse
mv ./target/ba2onto-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./target/ba2onto.jar

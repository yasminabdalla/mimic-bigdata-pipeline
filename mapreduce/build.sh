#!/bin/bash

SRC_DIR=src
BUILD_DIR=build
JAR_NAME=AvgAge.jar

rm -rf $BUILD_DIR
mkdir $BUILD_DIR

CLASSPATH="./hadoop-jars/common/*:./hadoop-jars/mapreduce/*"

javac -classpath "$CLASSPATH" -d $BUILD_DIR $SRC_DIR/AvgAge.java

jar -cvf $JAR_NAME -C $BUILD_DIR/ .
echo "✔ Built $JAR_NAME"

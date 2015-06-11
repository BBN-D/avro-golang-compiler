#!/bin/bash

java -cp ".:lib/*:target/*:target/lib/*:target/dependency/*" main.GenerateGolang "$@"

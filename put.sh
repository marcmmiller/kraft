#!/bin/bash

echo curl -L http://localhost:8080/put\?k=$1\&v=$2
curl -L http://localhost:8080/put\?k=$1\&v=$2
echo


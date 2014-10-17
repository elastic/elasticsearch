#!/usr/bin/env bash

# To use this script, copy and paste it in elasticsearch git root dir
# launch
# git bisect start {bad revision} {good revision}
# git bisect run {path to your script}

echo " -> starting a new test"
echo "$(git show --quiet)"

echo " -> remove old release"
rm -r target/releases
echo " -> compile/package"
mvn package -DskipTests > /dev/null 2> /dev/null
rm -r target/it/
echo " -> unzip"
unzip target/releases/elasticsearch-*.zip -d target/it/

echo " -> launch elasticsearch"
./target/it/elasticsearch*/bin/elasticsearch -d -p ./target/it/pid.txt

sleep 10

echo " PID $(cat ./target/it/pid.txt)"

tail -10 ./target/it/elasticsearch-*/logs/elasticsearch.log


# Your scenario goes here:
curl -XPOST "http://localhost:9200/test/test?refresh" -d'
{
  "date":"2014-05-26",
  "type":"press"
}' > /dev/null 2> /dev/null

curl -XGET "http://localhost:9200/_search/template" -d'
{
  "template": {
    "query": { "match_all": {}},
    "size": "{{my_size}}"
  }
}' 2>&1 | grep 'SearchParseException'
# At the end we try to find the error ^^^^

# Retrieve the exit code of the grep.
if [ "$?" -eq "0" ]; then
    echo "### bug is in this version"

    # We kill elasticsearch
    kill -TERM $(cat ./target/it/pid.txt)
    sleep 1
    tail -5 ./target/it/elasticsearch-*/logs/elasticsearch.log
    exit 1
else
    echo "no issue"

    # We kill elasticsearch
    kill -TERM $(cat ./target/it/pid.txt)
    sleep 1
    tail -5 ./target/it/elasticsearch-*/logs/elasticsearch.log
    exit 0
fi

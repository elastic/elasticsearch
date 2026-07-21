#!/bin/sh
set -e
printf 'path.repo: %s\n' "$ES_PATH_REPO" >> /usr/share/elasticsearch/config/elasticsearch.yml
exec bin/elasticsearch

#!/bin/sh
set -e
printf 'path.repo: %s\n' "$ES_PATH_REPO" >> /usr/share/elasticsearch/config/elasticsearch.yml
if [ -n "${ES_EXTRA_CONFIG:-}" ]; then
    printf '%s\n' "$ES_EXTRA_CONFIG" >> /usr/share/elasticsearch/config/elasticsearch.yml
fi
# The entrypoint runs as root, so it can manage the bind-mounted repo directory regardless
# of who owns it on the host. Ensure it exists, clear stale data from previous runs, then
# make it fully accessible to both the old ES process and the host-side new cluster.
mkdir -p "$ES_PATH_REPO"
find "$ES_PATH_REPO" -mindepth 1 -depth -delete 2>/dev/null || true
chmod 777 "$ES_PATH_REPO"
# Ensure files and directories created by old ES inside the repo are world-accessible
# so the host-side new cluster can write to them for repository verification and restore.
umask 0000
# Every other supported version stays in the foreground by default. 0.90.13's launch
# script backgrounds the process (and exits) unless explicitly told to foreground with -f.
if [ "$ES_VERSION" = "0.90.13" ]; then
    exec gosu elasticsearch bin/elasticsearch -f
else
    exec gosu elasticsearch bin/elasticsearch
fi

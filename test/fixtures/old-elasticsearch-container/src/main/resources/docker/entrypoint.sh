#!/bin/sh
set -e
printf 'path.repo: %s\n' "$ES_PATH_REPO" >> /usr/share/elasticsearch/config/elasticsearch.yml
if [ -n "${ES_EXTRA_CONFIG:-}" ]; then
    printf '%s\n' "$ES_EXTRA_CONFIG" >> /usr/share/elasticsearch/config/elasticsearch.yml
fi
# Make the bind-mounted repo directory fully accessible to both the old ES process and the
# host-side new cluster. The entrypoint runs as root so it can chmod regardless of ownership.
chmod 777 "$ES_PATH_REPO"
# Ensure files and directories created by old ES inside the repo are world-accessible
# so the host-side new cluster can write to them for repository verification and restore.
umask 0000
exec gosu elasticsearch bin/elasticsearch

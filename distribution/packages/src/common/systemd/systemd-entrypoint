#!/bin/sh

# This wrapper script allows SystemD to feed a file containing a passphrase into
# the main Elasticsearch startup script

if [ -n "$ES_KEYSTORE_PASSPHRASE_FILE" ] ; then
  exec /usr/share/elasticsearch/bin/elasticsearch "$@" < "$ES_KEYSTORE_PASSPHRASE_FILE"
else
  exec /usr/share/elasticsearch/bin/elasticsearch "$@"
fi

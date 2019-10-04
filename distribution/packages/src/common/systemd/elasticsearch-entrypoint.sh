#!/bin/sh

if [ -n "$ES_KEYSTORE_PASSPHRASE_FILE" ] ; then
  exec /usr/share/elasticsearch/bin/elasticsearch "$@" < "$ES_KEYSTORE_PASSPHRASE_FILE"
else
  exec /usr/share/elasticsearch/bin/elasticsearch "$@"
fi

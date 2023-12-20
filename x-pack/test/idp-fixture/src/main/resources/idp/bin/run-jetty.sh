#!/bin/sh

#set -x

if [ -e "/opt/shibboleth-idp/ext-conf/idp-secrets.properties" ]; then
  export JETTY_BACKCHANNEL_SSL_KEYSTORE_PASSWORD=`gawk 'match($0,/^jetty.backchannel.sslContext.keyStorePassword=\s?(.*)\s?$/, a) {print a[1]}' /opt/shibboleth-idp/ext-conf/idp-secrets.properties`
  export JETTY_BROWSER_SSL_KEYSTORE_PASSWORD=`gawk 'match($0,/^jetty\.sslContext\.keyStorePassword=\s?(.*)\s?$/, a) {print a[1]}' /opt/shibboleth-idp/ext-conf/idp-secrets.properties`
fi

export JETTY_ARGS="jetty.sslContext.keyStorePassword=$JETTY_BROWSER_SSL_KEYSTORE_PASSWORD jetty.backchannel.sslContext.keyStorePassword=$JETTY_BACKCHANNEL_SSL_KEYSTORE_PASSWORD"
sed -i "s/^-Xmx.*$/-Xmx$JETTY_MAX_HEAP/g" /opt/shib-jetty-base/start.ini

# For some reason, this container always immediately (in less than 1 second) exits with code 0 when starting for the first time
# Even with a health check, docker-compose will immediately report the container as unhealthy when using --wait instead of waiting for it to become healthy
# So, let's just start it a second time if it exits quickly
set +e
start_time=$(date +%s)
/opt/jetty-home/bin/jetty.sh run
exit_code=$?
end_time=$(date +%s)

duration=$((end_time - start_time))
if [ $duration -lt 5 ]; then
  /opt/jetty-home/bin/jetty.sh run
  exit_code=$?
fi

exit $exit_code

#!/bin/bash
cd /usr/share/elasticsearch/bin/
# Credentials are provisioned by DockerElasticsearchCluster via ES_TEST_USER / ES_TEST_PASS;
# fall back to the historical defaults so the script still works if invoked outside the JUnit fixture.
./elasticsearch-users useradd "${ES_TEST_USER:-x_pack_rest_user}" -p "${ES_TEST_PASS:-x-pack-test-password}" -r superuser || true
echo "testnode" > /tmp/password
cat /tmp/password  | ./elasticsearch-keystore add -x -f -v 'xpack.security.transport.ssl.secure_key_passphrase'
cat /tmp/password  | ./elasticsearch-keystore add -x -f -v 'xpack.security.http.ssl.secure_key_passphrase'
/usr/local/bin/docker-entrypoint.sh | tee /usr/share/elasticsearch/logs/console.log

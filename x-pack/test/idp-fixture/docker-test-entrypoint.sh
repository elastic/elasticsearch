#!/bin/bash
cd /usr/share/elasticsearch/bin/
./elasticsearch-users useradd x_pack_rest_user -p x-pack-test-password -r superuser || true
echo "testnode" >/tmp/password
echo "b07efb7a1cf6ec9462afe7b6d3ab55c6c7880262aa61ac28dded292aca47c9a2" >/tmp/client_secret
cat /tmp/password | ./elasticsearch-keystore add -x -f -v 'xpack.security.http.ssl.keystore.secure_password'
cat /tmp/client_secret | ./elasticsearch-keystore add -x -f -v 'xpack.security.authc.realms.oidc.c2id.rp.client_secret'
cat /tmp/client_secret | ./elasticsearch-keystore add -x -f -v 'xpack.security.authc.realms.oidc.c2id-implicit.rp.client_secret'
cat /tmp/client_secret | ./elasticsearch-keystore add -x -f -v 'xpack.security.authc.realms.oidc.c2id-proxy.rp.client_secret'
cat /tmp/client_secret | ./elasticsearch-keystore add -x -f -v 'xpack.security.authc.realms.oidc.c2id-post.rp.client_secret'
cat /tmp/client_secret | ./elasticsearch-keystore add -x -f -v 'xpack.security.authc.realms.oidc.c2id-jwt.rp.client_secret'
/usr/local/bin/docker-entrypoint.sh | tee >/usr/share/elasticsearch/logs/console.log

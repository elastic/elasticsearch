FROM osixia/openldap:1.4.0


ENV LDAP_ADMIN_PASSWORD=NickFuryHeartsES \
    LDAP_DOMAIN=oldap.test.elasticsearch.com \
    LDAP_BASE_DN=DC=oldap,DC=test,DC=elasticsearch,DC=com \
    LDAP_TLS=DC=true \
    LDAP_TLS_CRT_FILENAME=ldap_server.pem \
    LDAP_TLS_CA_CRT_FILENAME=ca_server.pem \
    LDAP_TLS_KEY_FILENAME=ldap_server.key \
    LDAP_TLS_VERIFY_CLIENT=never \
    LDAP_TLS_CIPHER_SUITE=NORMAL \
    LDAP_LOG_LEVEL=256

COPY ./ldif/users.ldif /container/service/slapd/assets/config/bootstrap/ldif/custom/20-bootstrap-users.ldif
COPY ./ldif/config.ldif /container/service/slapd/assets/config/bootstrap/ldif/custom/10-bootstrap-config.ldif
COPY ./certs /container/service/slapd/assets/certs

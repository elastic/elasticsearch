/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.CacheableTestFixture;
import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.junit.rules.TestRule;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class OpenLdapTestContainer extends DockerEnvironmentAwareTestContainer implements TestRule, CacheableTestFixture {

    public static final String DOCKER_BASE_IMAGE = "osixia/openldap:1.4.0";

    public OpenLdapTestContainer() {
        super(
            new ImageFromDockerfile("es-openldap-testfixture", false).withDockerfileFromBuilder(
                    builder -> builder.from(DOCKER_BASE_IMAGE)
                        .env("LDAP_ADMIN_PASSWORD", "NickFuryHeartsES")
                        .env("LDAP_DOMAIN", "oldap.test.elasticsearch.com")
                        .env("LDAP_BASE_DN", "DC=oldap,DC=test,DC=elasticsearch,DC=com")
                        .env("LDAP_TLS", "true")
                        .env("LDAP_TLS_CRT_FILENAME", "ldap_server.pem")
                        .env("LDAP_TLS_CA_CRT_FILENAME", "ca_server.pem")
                        .env("LDAP_TLS_KEY_FILENAME", "ldap_server.key")
                        .env("LDAP_TLS_VERIFY_CLIENT", "never")
                        .env("LDAP_TLS_CIPHER_SUITE", "NORMAL")
                        .env("LDAP_LOG_LEVEL", "256")
                        .copy("openldap/ldif/users.ldif", "/container/service/slapd/assets/config/bootstrap/ldif/custom/20-bootstrap-users.ldif")
                        .copy("openldap/ldif/config.ldif", "/container/service/slapd/assets/config/bootstrap/ldif/custom/10-bootstrap-config.ldif")
                        .copy("openldap/certs","/container/service/slapd/assets/certs")
                        .expose(389)
                        .expose(636)
                        .build()
                )
                .withFileFromClasspath("openldap/certs", "/openldap/certs/")
                .withFileFromClasspath("openldap/ldif/users.ldif", "/openldap/ldif/users.ldif")
                .withFileFromClasspath("openldap/ldif/config.ldif", "/openldap/ldif/config.ldif")
        );
    }

    @Override
    public void cache() {
        try {
            start();
            stop();
        } catch (RuntimeException e) {
            logger().warn("Error while caching container images.", e);
        }
    }
}

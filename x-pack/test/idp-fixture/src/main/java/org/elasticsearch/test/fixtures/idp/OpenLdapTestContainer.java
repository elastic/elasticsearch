/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.test.fixtures.ResourceUtils.copyResourceToFile;

public final class OpenLdapTestContainer extends DockerEnvironmentAwareTestContainer {

    public static final String DOCKER_BASE_IMAGE = "osixia/openldap:1.4.0";

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Path certsPath;

    public OpenLdapTestContainer() {
        this(Network.newNetwork());
    }

    public OpenLdapTestContainer(Network network) {
        super(
            new ImageFromDockerfile("es-openldap-testfixture").withDockerfileFromBuilder(
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
                    .copy(
                        "openldap/ldif/users.ldif",
                        "/container/service/slapd/assets/config/bootstrap/ldif/custom/20-bootstrap-users.ldif"
                    )
                    .copy(
                        "openldap/ldif/config.ldif",
                        "/container/service/slapd/assets/config/bootstrap/ldif/custom/10-bootstrap-config.ldif"
                    )
                    .copy("openldap/certs", "/container/service/slapd/assets/certs")

                    .build()
            )
                .withFileFromClasspath("openldap/certs", "/openldap/certs/")
                .withFileFromClasspath("openldap/ldif/users.ldif", "/openldap/ldif/users.ldif")
                .withFileFromClasspath("openldap/ldif/config.ldif", "/openldap/ldif/config.ldif")
        );
        // withLogConsumer(new Slf4jLogConsumer(logger()));
        withNetworkAliases("openldap");
        withNetwork(network);
        withExposedPorts(389, 636);
    }

    public String getLdapUrl() {
        return "ldaps://localhost:" + getMappedPort(636);
    }

    @Override
    public void start() {
        super.start();
        setupCerts();
    }

    @Override
    public void stop() {
        super.stop();
        temporaryFolder.delete();
    }

    private void setupCerts() {
        try {
            temporaryFolder.create();
            certsPath = temporaryFolder.newFolder("certs").toPath();
            copyResourceToFile(getClass(), certsPath, "openldap/certs/ca.jks");
            copyResourceToFile(getClass(), certsPath, "openldap/certs/ca_server.key");
            copyResourceToFile(getClass(), certsPath, "openldap/certs/ca_server.pem");
            copyResourceToFile(getClass(), certsPath, "openldap/certs/dhparam.pem");
            copyResourceToFile(getClass(), certsPath, "openldap/certs/ldap_server.key");
            copyResourceToFile(getClass(), certsPath, "openldap/certs/ldap_server.pem");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path getJavaKeyStorePath() {
        return certsPath.resolve("ca.jks");
    }

    public Path getCaCertPath() {
        return certsPath.resolve("ca_server.pem");
    }

    public Integer getDefaultPort() {
        return getMappedPort(636);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.PullOrBuildImage;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.test.fixtures.ResourceUtils.copyResourceToFile;

public final class OpenLdapTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String DOCKER_BASE_IMAGE = "docker.elastic.co/elasticsearch-dev/openldap-fixture:1.1";
    private static final String LDAP_READY_COMMAND = """
        ldapsearch -LLL -x\
         -H ldaps://localhost:636\
         -D "cn=admin,$LDAP_BASE_DN"\
         -w "$LDAP_ADMIN_PASSWORD"\
         -b "uid=cap,ou=people,$LDAP_BASE_DN"\
         -s base dn""";

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Path certsPath;

    public OpenLdapTestContainer() {
        this(Network.newNetwork());
    }

    public OpenLdapTestContainer(Network network) {
        super(
            new PullOrBuildImage(
                DOCKER_BASE_IMAGE,
                new ImageFromDockerfile("localhost/es-openldap-fixture").withFileFromClasspath("Dockerfile", "/openldap/Dockerfile")
                    .withFileFromClasspath("ldif/users.ldif", "/openldap/ldif/users.ldif")
                    .withFileFromClasspath("ldif/config.ldif", "/openldap/ldif/config.ldif")
                    .withFileFromClasspath("certs", "/openldap/certs")
                    .withFileFromClasspath("wait-for-slapd.sh", "/openldap/wait-for-slapd.sh")
            )
        );
        withNetworkAliases("openldap");
        withNetwork(network);
        withExposedPorts(389, 636);
        withCommand("--loglevel", "debug");
        waitingFor(Wait.forSuccessfulCommand(LDAP_READY_COMMAND));
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

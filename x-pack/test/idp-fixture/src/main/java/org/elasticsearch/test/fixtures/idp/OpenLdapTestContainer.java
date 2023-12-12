/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.CacheableTestFixture;
import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

public final class OpenLdapTestContainer extends DockerEnvironmentAwareTestContainer implements TestRule, CacheableTestFixture {

    public static final String DOCKER_BASE_IMAGE = "osixia/openldap:1.4.0";

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Path certsPath;

    public OpenLdapTestContainer() {
        this(Network.newNetwork());
    }

    public OpenLdapTestContainer(Network network) {
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
    public void cache() {
        try {
            start();
            stop();
        } catch (RuntimeException e) {
            logger().warn("Error while caching container images.", e);
        }
    }

    @Override
    public void start() {
        super.start();
        setupCerts();
    }

    private void setupCerts() {
        try {
            temporaryFolder.create();
            certsPath = temporaryFolder.newFolder("certs").toPath();
            copyResource(certsPath, "openldap/certs/ca.jks");
            copyResource(certsPath, "openldap/certs/ca_server.key");
            copyResource(certsPath, "openldap/certs/ca_server.pem");
            copyResource(certsPath, "openldap/certs/dhparam.pem");
            copyResource(certsPath, "openldap/certs/ldap_server.key");
            copyResource(certsPath, "openldap/certs/ldap_server.pem");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path copyResource(Path targetFolder, String resourcePath) {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            URL resourceUrl = classLoader.getResource(resourcePath);
            if (resourceUrl == null) {
                throw new RuntimeException("Failed to load " + resourcePath + " from classpath");
            }
            InputStream inputStream = resourceUrl.openStream();
            File outputFile = new File(targetFolder.toFile(), resourcePath.substring(resourcePath.lastIndexOf("/")));
            Files.copy(inputStream, outputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return outputFile.toPath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load ca.jks from classpath", e);
        }
    }

    public Path getJavaKeyStorePath() {
        System.out.println("OpenLdapTestContainer.getJavaKeyStorePath");
        System.out.println("certsPath = " + certsPath);
        for (File f : Objects.requireNonNull(certsPath.toFile().listFiles())) {
            System.out.println("f = " + f + " --- " + f.exists());
        }
        certsPath.toFile().listFiles();

        return certsPath.resolve("ca.jks");
    }

    public Path getCaCertPath() {
        System.out.println("OpenLdapTestContainer.getCaCertPath");
        System.out.println("certsPath = " + certsPath);
        for (File f : Objects.requireNonNull(certsPath.toFile().listFiles())) {
            System.out.println("f = " + f + " --- " + f.exists());
        }
        return certsPath.resolve("ca_server.pem");
    }
}

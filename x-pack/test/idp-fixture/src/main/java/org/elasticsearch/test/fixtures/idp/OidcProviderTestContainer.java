/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.PullOrBuildImage;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;

import java.time.Duration;

public final class OidcProviderTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String DOCKER_BASE_IMAGE = "docker.elastic.co/elasticsearch-dev/oidc-fixture:1.1";

    private static final int PORT = 8080;
    private static final int SSL_PORT = 8443;

    /** Time to wait for /c2id/jwks.json to become available after copying override.properties. */
    private static final Duration JWKS_READY_TIMEOUT = Duration.ofSeconds(120);

    /**
     * for packer caching only
     * */
    protected OidcProviderTestContainer() {
        this(Network.newNetwork());
    }

    public OidcProviderTestContainer(Network network) {
        super(
            new PullOrBuildImage(
                DOCKER_BASE_IMAGE,
                new ImageFromDockerfile("localhost/es-oidc-provider-fixture").withFileFromClasspath("build.sh", "/oidc/build.sh")
                    .withFileFromClasspath("entrypoint.sh", "/oidc/entrypoint.sh")
                    .withFileFromClasspath("testnode.jks", "/oidc/testnode.jks")
                    .withFileFromClasspath("Dockerfile", "/oidc/Dockerfile")
            )
        );
        withNetworkAliases("oidc-provider");
        withNetwork(network);
        addExposedPorts(PORT, SSL_PORT);
        // Phase 1: consider container "started" when entrypoint is waiting for override.properties
        setWaitStrategy(Wait.forLogMessage(".*Waiting for properties file.*", 1).withStartupTimeout(JWKS_READY_TIMEOUT));
    }

    @Override
    public void start() {
        super.start();
        // Phase 2: inject override.properties so entrypoint can start Tomcat
        copyFileToContainer(
            Transferable.of(
                "op.issuer=http://127.0.0.1:"
                    + getMappedPort(PORT)
                    + "/c2id\n"
                    + "op.authz.endpoint=http://127.0.0.1:"
                    + getMappedPort(PORT)
                    + "/c2id-login/\n"
                    + "op.reg.apiAccessTokenSHA256=d1c4fa70d9ee708d13cfa01daa0e060a05a2075a53c5cc1ad79e460e96ab5363\n"
                    + "op.authz.alwaysPromptForConsent=true\n"
                    + "op.authz.alwaysPromptForAuth=true"
            ),
            "/config/c2id/override.properties"
        );
        // Phase 3: wait for JWKS endpoint (Tomcat and c2id are up)
        Wait.forHttp("/c2id/jwks.json")
            .forPort(SSL_PORT)
            .usingTls()
            .allowInsecure()
            .withStartupTimeout(JWKS_READY_TIMEOUT)
            .waitUntilReady(this);
    }

    public String getC2OPUrl() {
        return "http://127.0.0.1:" + getMappedPort(PORT);
    }

    public String getC2IssuerUrl() {
        return getC2OPUrl() + "/c2id";
    }

    public String getC2IDSslUrl() {
        return "https://127.0.0.1:" + getMappedPort(SSL_PORT) + "/c2id";
    }
}

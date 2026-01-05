/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;

public final class OidcProviderTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final int PORT = 8080;
    private static final int SSL_PORT = 8443;

    /**
     * for packer caching only
     * */
    protected OidcProviderTestContainer() {
        this(Network.newNetwork());
    }

    public OidcProviderTestContainer(Network network) {
        super(
            new ImageFromDockerfile("es-oidc-provider-fixture").withFileFromClasspath("oidc/setup.sh", "/oidc/setup.sh")
                .withFileFromClasspath("oidc/testnode.jks", "/oidc/testnode.jks")
                // we cannot make use of docker file builder
                // as it does not support multi-stage builds
                .withFileFromClasspath("Dockerfile", "oidc/Dockerfile")
        );
        withNetworkAliases("oidc-provider");
        withNetwork(network);
        addExposedPorts(PORT, SSL_PORT);
    }

    @Override
    public void start() {
        super.start();
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
            "config/c2id/override.properties"
        );
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

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

public final class HttpProxyTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final Integer PORT = 8888;
    private static final Integer TLS_PORT = 8889;

    /**
     * for packer caching only
     * */
    public HttpProxyTestContainer() {
        this(Network.newNetwork());
    }

    public HttpProxyTestContainer(Network network) {
        super(
            new ImageFromDockerfile("es-http-proxy-fixture").withFileFromClasspath("Dockerfile", "nginx/Dockerfile")
                .withFileFromClasspath("nginx/nginx.conf", "/nginx/nginx.conf")
        );
        addExposedPorts(PORT, TLS_PORT);
        withNetwork(network);
    }

    public Integer getProxyPort() {
        return getMappedPort(PORT);
    }

    public Integer getTlsPort() {
        return getMappedPort(TLS_PORT);
    }
}

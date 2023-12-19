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

    public static final String DOCKER_BASE_IMAGE = "nginx:latest";
    private static final Integer PORT = 8888;

    /**
     * for packer caching only
     * */
    public HttpProxyTestContainer() {
        this(Network.newNetwork());
    }

    public HttpProxyTestContainer(Network network) {
        super(
            new ImageFromDockerfile("es-http-proxy-fixture").withDockerfileFromBuilder(
                builder -> builder.from(DOCKER_BASE_IMAGE).copy("oidc/nginx.conf", "/etc/nginx/nginx.conf").build()
            ).withFileFromClasspath("oidc/nginx.conf", "/oidc/nginx.conf")
        );
        addExposedPort(PORT);
        withNetwork(network);

    }

    public Integer getProxyPort() {
        return getMappedPort(PORT);
    }
}

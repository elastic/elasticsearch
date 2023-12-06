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

public class HttpProxyTestContainer extends DockerEnvironmentAwareTestContainer implements TestRule, CacheableTestFixture {

    public static final String DOCKER_BASE_IMAGE = "nginx:latest";

    public HttpProxyTestContainer() {
        super(
            new ImageFromDockerfile("es-http-proxy-fixture", false).withDockerfileFromBuilder(
                    builder -> builder.from(DOCKER_BASE_IMAGE)
                        .copy("oidc/nginx.conf", "/etc/nginx/nginx.conf")
                        .expose(8888)
                        .build()
                )
                .withFileFromClasspath("oidc/nginx.conf", "/oidc/nginx.conf")
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

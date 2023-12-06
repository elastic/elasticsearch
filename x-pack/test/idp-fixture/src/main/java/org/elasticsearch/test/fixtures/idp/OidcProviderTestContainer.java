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
import org.testcontainers.images.builder.dockerfile.statement.RawStatement;
import org.testcontainers.images.builder.dockerfile.statement.Statement;

public class OidcProviderTestContainer extends DockerEnvironmentAwareTestContainer implements TestRule, CacheableTestFixture {

    public OidcProviderTestContainer() {
        super(
            new ImageFromDockerfile("es-oidc-provider-fixture", false).withDockerfileFromBuilder(
                builder -> builder
                    // .from("c2id/c2id-server-demo:12.18")
                    .withStatement(new RawStatement("FROM", "FROM c2id/c2id-server-demo:12.18 as c2id"))
                    .from("openjdk:11.0.16-jre")
                    .run("apt-get update -qqy && apt-get install -qqy python3")
                    .copy("--from=c2id /c2id-server", "/c2id-server")
                    .copy("--from=c2id /etc/c2id", "/etc/c2id")
                    .copy("oidc/setup.sh", "/fixture/")
                    .env("ENV CATALINA_OPTS", "-DsystemPropertiesURL=file:///config/c2id/override.properties")
                    .expose(8888)
                    .cmd("/bin/bash", "/fixture/setup.sh")
                    .build()
            ).withFileFromClasspath("oidc/setup.sh", "/oidc/setup.sh")
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

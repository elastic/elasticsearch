/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.minio;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class MinioFixtureTestContainer implements TestRule {

    private static final int servicePort = 9000;
    private GenericContainer<?> container;

    private GenericContainer<?> createContainer() {
        return new GenericContainer<>(
            new ImageFromDockerfile().withDockerfileFromBuilder(
                builder -> builder.from("minio/minio:RELEASE.2021-03-01T04-20-55Z")
                    .env("MINIO_ACCESS_KEY", "s3_test_access_key")
                    .env("MINIO_SECRET_KEY", "s3_test_secret_key")
                    .run("mkdir -p /minio/data/bucket")
                    .cmd("server", "/minio/data")
                    .build()
            )
        ).withExposedPorts(servicePort);
    }

    public MinioFixtureTestContainer(boolean enabled) {
        if (enabled) {
            this.container = createContainer();
        }
    }

    @Override
    public Statement apply(Statement base, Description description) {
        if (container != null) {
            container.start();
        }
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                base.evaluate();
            }
        };
    }

    private int getServicePort() {
        return container.getMappedPort(servicePort);
    }

    public String getServiceUrl() {
        return "http://127.0.0.1:" + getServicePort();
    }
}

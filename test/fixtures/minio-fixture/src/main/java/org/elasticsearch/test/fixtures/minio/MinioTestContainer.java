/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.minio;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

public final class MinioTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final int servicePort = 9000;
    public static final String DOCKER_BASE_IMAGE = "minio/minio:RELEASE.2021-03-01T04-20-55Z";
    private final boolean enabled;

    public MinioTestContainer() {
        this(true);
    }

    public MinioTestContainer(boolean enabled) {
        super(
            new ImageFromDockerfile("es-minio-testfixture").withDockerfileFromBuilder(
                builder -> builder.from(DOCKER_BASE_IMAGE)
                    .env("MINIO_ACCESS_KEY", "s3_test_access_key")
                    .env("MINIO_SECRET_KEY", "s3_test_secret_key")
                    .run("mkdir -p /minio/data/bucket")
                    .cmd("server", "/minio/data")
                    .build()
            )
        );
        if (enabled) {
            addExposedPort(servicePort);
        }
        this.enabled = enabled;
    }

    @Override
    public void start() {
        if (enabled) {
            super.start();
        }
    }

    public String getAddress() {
        return "http://127.0.0.1:" + getMappedPort(servicePort);
    }
}

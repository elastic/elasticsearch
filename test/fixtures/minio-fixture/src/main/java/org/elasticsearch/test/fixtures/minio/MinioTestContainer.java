/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.minio;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

public final class MinioTestContainer extends DockerEnvironmentAwareTestContainer {

    // NB releases earlier than 2025-05-24 are buggy, see https://github.com/minio/minio/issues/21189, and #127166 for a workaround
    public static final String DOCKER_BASE_IMAGE = "minio/minio:RELEASE.2025-05-24T17-08-30Z";

    private static final int servicePort = 9000;
    private final boolean enabled;

    /**
     * for packer caching only
     * see CacheCacheableTestFixtures.
     * */
    protected MinioTestContainer() {
        this(true, "minio", "minio123", "test-bucket");
    }

    public MinioTestContainer(boolean enabled, String accessKey, String secretKey, String bucketName) {
        super(
            new ImageFromDockerfile("es-minio-testfixture").withDockerfileFromBuilder(
                builder -> builder.from(DOCKER_BASE_IMAGE)
                    .env("MINIO_ACCESS_KEY", accessKey)
                    .env("MINIO_SECRET_KEY", secretKey)
                    .run("mkdir -p /minio/data/" + bucketName)
                    .cmd("server", "/minio/data")
                    .build()
            )
        );
        if (enabled) {
            addExposedPort(servicePort);
            // The following waits for a specific log message as the readiness signal. When the minio docker image
            // gets upgraded in future, we must ensure the log message still exists or update it here accordingly.
            // Otherwise the tests using the minio fixture will fail with timeout on waiting the container to be ready.
            setWaitStrategy(Wait.forLogMessage("API: .*:9000.*", 1));
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

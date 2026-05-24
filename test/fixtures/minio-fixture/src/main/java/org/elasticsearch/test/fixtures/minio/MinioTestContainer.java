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
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.RemoteDockerImage;

import java.io.File;
import java.io.IOException;

public final class MinioTestContainer extends DockerEnvironmentAwareTestContainer {

    /*
     * Known issues broken down by MinIO release date:
     * [< 2025-05-24                ] known issue https://github.com/minio/minio/issues/21189; workaround in #127166
     * [= 2025-05-24                ] known issue https://github.com/minio/minio/issues/21377; no workaround
     * [> 2025-05-24 && < 2025-09-07] known issue https://github.com/minio/minio/issues/21456; workaround in #131815
     * [>= 2025-09-07               ] no known issues (yet)
     */
    public static final String DOCKER_BASE_IMAGE = "minio/minio:RELEASE.2025-09-07T16-13-09Z";

    private static final int servicePort = 9000;
    private final boolean enabled;
    private final String bucketName;

    private final TemporaryFolder dataFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * for packer caching only
     * see CacheCacheableTestFixtures.
     * */
    protected MinioTestContainer() {
        this(true, "minio", "minio123", "test-bucket");
    }

    public MinioTestContainer(boolean enabled, String accessKey, String secretKey, String bucketName) {
        super(new RemoteDockerImage(DOCKER_BASE_IMAGE));
        this.bucketName = bucketName;
        withEnv("MINIO_ROOT_USER", accessKey);
        withEnv("MINIO_ROOT_PASSWORD", secretKey);
        withCommand("server", "/minio/data");
        File bucketFolder = null;
        try {
            dataFolder.create();
            bucketFolder = dataFolder.newFolder("minio", "data", bucketName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        withFileSystemBind(bucketFolder.getParentFile().getAbsolutePath(), "/minio/data/", BindMode.READ_WRITE);

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

    @Override
    public void stop() {
        if (enabled) {
            super.stop();
        }
        // Always cleanup dataFolder since it's always created in constructor
        // Use try-catch to prevent cleanup failures from failing tests on CI where /dev/shm may have permission issues
        // or container still holds references to files as not fully stopped yet.
        try {
            dataFolder.delete();
        } catch (AssertionError e) {
            LOGGER.warn(
                "Failed to clean up temporary folder at {}. This is typically harmless and cleanup will happen via CI agent cleanup.",
                dataFolder.getRoot(),
                e
            );
            // Don't propagate - cleanup failures shouldn't fail tests
        }
    }

    public String getAddress() {
        return "http://127.0.0.1:" + getMappedPort(servicePort);
    }
}

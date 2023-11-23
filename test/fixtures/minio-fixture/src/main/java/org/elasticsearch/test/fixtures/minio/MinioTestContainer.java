/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.minio;

import org.junit.Assume;
import org.junit.rules.TestRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class MinioTestContainer extends GenericContainer<MinioTestContainer> implements TestRule {

    private static final int servicePort = 9000;
    private final boolean enabled;

    public MinioTestContainer() {
        this(true);
    }

    public MinioTestContainer(boolean enabled) {
        super(
            new ImageFromDockerfile().withDockerfileFromBuilder(
                builder -> builder.from("minio/minio:RELEASE.2021-03-01T04-20-55Z")
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
        Assume.assumeFalse(
            "https://github.com/elastic/elasticsearch/issues/102532",
            System.getProperty("os.name").toLowerCase().startsWith("windows")
        );
        if (enabled) {
            super.start();
        }
    }

    public String getAddress() {
        return "http://127.0.0.1:" + getMappedPort(servicePort);
    }
}

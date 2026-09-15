/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import fixture.s3.BlobEntry;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

/**
 * Verifies that {@code addressing_style=virtual_hosted} with a bare-IP endpoint falls back
 * to path-style end-to-end: the SDK cannot use virtual-hosted addressing for bare IPs, so the
 * request must still reach the fixture and produce a path-style URL.
 * <p>
 * URL-shape assertions for the full addressing_style matrix (endpoint absent/present ×
 * auto/path/virtual_hosted) are covered by the no-network
 * {@link S3AddressingStyleInterceptorTests}, which abort before any network I/O.
 */
public class S3AddressingStyleTests extends ESTestCase {

    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";
    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/test.parquet";

    @ClassRule
    public static final HandlerExposingS3HttpFixture s3Fixture = new HandlerExposingS3HttpFixture(BUCKET, ACCESS_KEY);

    @BeforeClass
    public static void seedFixture() {
        s3Fixture.handler().blobs().put("/" + BUCKET + "/" + KEY, new BlobEntry(new BytesArray(new byte[0]), "STANDARD"));
    }

    @AfterClass
    public static void clearFixture() {
        s3Fixture.handler().blobs().clear();
    }

    /**
     * addressing_style=virtual_hosted with a bare-IP endpoint: the SDK falls back to path-style.
     * Asserts that the fallback works end-to-end — no crash, request reaches the fixture, and the
     * recorded path is path-style.
     */
    public void testVirtualHostedWithBareIpFallsBackToPathStyle() throws IOException {
        S3Configuration config = S3Configuration.fromMap(
            Map.of(
                "access_key",
                ACCESS_KEY,
                "secret_key",
                SECRET_KEY,
                "endpoint",
                s3Fixture.getAddress(),
                "addressing_style",
                "virtual_hosted"
            )
        );
        int logSizeBefore = s3Fixture.handler().requestLog().size();
        try (S3StorageProvider provider = new S3StorageProvider(config)) {
            provider.exists(StoragePath.of("s3://" + BUCKET + "/" + KEY));
        }
        var log = s3Fixture.handler().requestLog();
        assertTrue("No request recorded", log.size() > logSizeBefore);
        String path = log.get(log.size() - 1).path();
        assertTrue("Expected path-style request, got: " + path, path.startsWith("/" + BUCKET + "/"));
    }
}

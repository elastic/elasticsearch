/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import fixture.s3.BlobEntry;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;

/**
 * Verifies that the addressing_style setting controls whether requests use path-style
 * or virtual-hosted URL addressing by inspecting the request paths recorded by an
 * in-process S3 fixture.
 * <p>
 * For all combinations tested here, the fixture endpoint is a bare IP (127.0.0.1),
 * so the AWS SDK falls back to path-style for {@code virtual_hosted} too — which is
 * the documented behaviour for bare-IP endpoints. The test asserts that the fallback
 * works and that no addressing-style value breaks routing through the fixture.
 */
public class S3AddressingStyleTests extends ESTestCase {

    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";
    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/test.parquet";

    @ClassRule
    public static final HandlerExposingS3HttpFixture s3Fixture = new HandlerExposingS3HttpFixture();

    @BeforeClass
    public static void seedFixture() {
        s3Fixture.handler().blobs().put("/" + BUCKET + "/" + KEY, new BlobEntry(new BytesArray(new byte[0]), "STANDARD"));
    }

    @AfterClass
    public static void clearFixture() {
        s3Fixture.handler().blobs().clear();
    }

    /**
     * No addressing_style set (auto default) with an endpoint override → path-style (existing behaviour).
     */
    public void testAutoDefaultWithEndpointUsesPathStyle() throws IOException {
        S3Configuration config = S3Configuration.fromFields(ACCESS_KEY, SECRET_KEY, s3Fixture.getAddress(), "us-east-1");
        assertRequestPathStartsWithBucket(config, "absent (auto)");
    }

    /**
     * addressing_style=auto with an endpoint override → path-style.
     */
    public void testAutoWithEndpointUsesPathStyle() throws IOException {
        S3Configuration config = S3Configuration.fromMap(
            Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY, "endpoint", s3Fixture.getAddress(), "addressing_style", "auto")
        );
        assertRequestPathStartsWithBucket(config, "auto");
    }

    /**
     * addressing_style=path with an endpoint override → path-style.
     */
    public void testPathWithEndpointUsesPathStyle() throws IOException {
        S3Configuration config = S3Configuration.fromMap(
            Map.of("access_key", ACCESS_KEY, "secret_key", SECRET_KEY, "endpoint", s3Fixture.getAddress(), "addressing_style", "path")
        );
        assertRequestPathStartsWithBucket(config, "path");
    }

    /**
     * addressing_style=virtual_hosted with a bare-IP endpoint: the SDK cannot use virtual-hosted
     * addressing for bare IPs and falls back to path-style. The assertion verifies that the fallback
     * works end-to-end (no crash, request reaches the fixture) and produces a path-style request.
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
        assertRequestPathStartsWithBucket(config, "virtual_hosted (bare IP fallback)");
    }

    private void assertRequestPathStartsWithBucket(S3Configuration config, String label) throws IOException {
        int logSizeBefore = s3Fixture.handler().requestLog().size();
        try (S3StorageProvider provider = new S3StorageProvider(config)) {
            provider.exists(StoragePath.of("s3://" + BUCKET + "/" + KEY));
        }
        var log = s3Fixture.handler().requestLog();
        assertTrue("No request recorded for addressing_style=" + label, log.size() > logSizeBefore);
        String path = log.get(log.size() - 1).path();
        assertTrue(
            "Expected path-style request (path starting with /" + BUCKET + "/) for addressing_style=" + label + ", got: " + path,
            path.startsWith("/" + BUCKET + "/")
        );
    }

    /**
     * Local S3HttpFixture that exposes the underlying S3HttpHandler so tests can seed blobs
     * and inspect the request log. Mirrors HandlerExposingS3HttpFixture in S3GlobDiscoveryTests.
     */
    @SuppressForbidden(reason = "overrides S3HttpFixture.createHandler which returns com.sun.net.httpserver.HttpHandler")
    public static final class HandlerExposingS3HttpFixture extends S3HttpFixture {

        private S3HttpHandler handler;

        public HandlerExposingS3HttpFixture() {
            super(true, () -> S3ConsistencyModel.STRONG_MPUS);
        }

        @Override
        protected HttpHandler createHandler() {
            handler = new S3HttpHandler(BUCKET, null, S3ConsistencyModel.STRONG_MPUS);
            var auth = fixedAccessKey(ACCESS_KEY, () -> "us-east-1", "s3");
            return exchange -> {
                if (checkAuthorization(auth, exchange)) {
                    handler.handle(exchange);
                }
            };
        }

        S3HttpHandler handler() {
            return handler;
        }
    }
}

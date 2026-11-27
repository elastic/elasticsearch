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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static fixture.aws.AwsFixtureUtils.sendError;

/**
 * A freshly minted access key can 403 {@code InvalidAccessKeyId} until IAM has propagated it.
 * The S3 SDK Standard strategy retries that code; a permission denial ({@code AccessDenied}) still
 * fails on the first attempt. Async range reads use the same strategy outside the SDK client.
 */
@ThreadLeakFilters(filters = { S3StorageObjectAsyncTests.DelaySchedulerThreadFilter.class })
public class S3CredentialPropagationRetryTests extends ESTestCase {

    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";
    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.csv";
    private static final byte[] BODY = "seeded-after-iam-lag".getBytes(StandardCharsets.UTF_8);

    @ClassRule
    public static final FirstDeniedFixture s3Fixture = new FirstDeniedFixture();

    @BeforeClass
    public static void seedFixture() {
        s3Fixture.handler().blobs().put("/" + BUCKET + "/" + KEY, new BlobEntry(new BytesArray(BODY), "STANDARD"));
    }

    @Before
    public void resetFixture() {
        s3Fixture.reset();
    }

    public void testGetRetriesOneInvalidAccessKeyId() throws IOException {
        try (
            S3StorageProvider provider = new S3StorageProvider(config());
            InputStream in = provider.newObject(StoragePath.of("s3://" + BUCKET + "/" + KEY)).newStream()
        ) {
            assertArrayEquals(BODY, in.readAllBytes());
        }
        assertEquals(List.of(getRequest(), getRequest()), s3Fixture.requests());
    }

    public void testListingRetriesOneInvalidAccessKeyId() throws IOException {
        List<String> names = new ArrayList<>();
        try (
            S3StorageProvider provider = new S3StorageProvider(config());
            StorageIterator iterator = provider.listObjects(StoragePath.of("s3://" + BUCKET + "/data"), false)
        ) {
            while (iterator.hasNext()) {
                names.add(iterator.next().path().objectName());
            }
        }
        assertEquals(List.of("file.csv"), names);
        assertEquals(2, s3Fixture.requests().size());
        assertTrue(s3Fixture.requests().stream().allMatch(r -> r.startsWith("LIST ")));
    }

    public void testAsyncRangeReadRetriesOneInvalidAccessKeyId() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(NoopCircuitBreaker.INSTANCE);
        StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
        try (S3StorageProvider provider = new S3StorageProvider(config())) {
            Releasable cancel = provider.newObject(path, BODY.length)
                .startReadBytesAsync(0, BODY.length, factory, Runnable::run, new ActionListener<>() {
                    @Override
                    public void onResponse(DirectReadBuffer buffer) {
                        result.set(buffer);
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        error.set(e);
                        latch.countDown();
                    }
                });
            boolean settled = false;
            try {
                settled = latch.await(30, TimeUnit.SECONDS);
                assertTrue("async range read never settled", settled);
            } finally {
                if (settled == false) {
                    cancel.close();
                }
            }
        }
        if (error.get() != null) {
            throw new AssertionError("async range read failed", error.get());
        }
        try (DirectReadBuffer buf = result.get()) {
            assertNotNull(buf);
            byte[] got = new byte[buf.buffer().remaining()];
            buf.buffer().get(got);
            assertArrayEquals(BODY, got);
        }
        assertEquals(List.of(getRequest(), getRequest()), s3Fixture.requests());
    }

    public void testAccessDeniedIsNotRetried() throws IOException {
        s3Fixture.failFirstWith("AccessDenied");
        try (S3StorageProvider provider = new S3StorageProvider(config())) {
            expectThrows(ExternalClientException.class, () -> provider.newObject(StoragePath.of("s3://" + BUCKET + "/" + KEY)).newStream());
        }
        assertEquals(List.of(getRequest()), s3Fixture.requests());
    }

    private static S3Configuration config() {
        return S3Configuration.fromFields(ACCESS_KEY, SECRET_KEY, s3Fixture.getAddress(), "us-east-1");
    }

    private static String getRequest() {
        return "GET /" + BUCKET + "/" + KEY;
    }

    /**
     * After auth, the first GET of a seeded object or ListObjectsV2 answers the configured 403;
     * later requests go to {@link S3HttpHandler}.
     */
    @SuppressForbidden(reason = "overrides S3HttpFixture.createHandler which returns com.sun.net.httpserver.HttpHandler")
    static final class FirstDeniedFixture extends S3HttpFixture {

        private S3HttpHandler handler;
        private final AtomicReference<String> firstErrorCode = new AtomicReference<>("InvalidAccessKeyId");
        private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        private final List<String> requests = new CopyOnWriteArrayList<>();

        FirstDeniedFixture() {
            super(true, () -> S3ConsistencyModel.STRONG_MPUS);
        }

        @Override
        protected HttpHandler createHandler() {
            handler = new S3HttpHandler(BUCKET, null, S3ConsistencyModel.STRONG_MPUS);
            var auth = fixedAccessKey(ACCESS_KEY, () -> "us-east-1", "s3");
            return exchange -> {
                if (checkAuthorization(auth, exchange) == false) {
                    return;
                }
                S3HttpHandler.S3Request request = handler.parseRequest(exchange);
                boolean list = request.isListObjectsRequest();
                boolean seededGet = request.isGetObjectRequest() && handler.blobs().containsKey(request.path());
                if (list || seededGet) {
                    String recorded = (list ? "LIST " : "GET ") + request.path();
                    requests.add(recorded);
                    if (attempts.computeIfAbsent(request.path(), k -> new AtomicInteger()).getAndIncrement() == 0) {
                        String code = firstErrorCode.get();
                        sendError(exchange, RestStatus.FORBIDDEN, code, code);
                        return;
                    }
                }
                handler.handle(exchange);
            };
        }

        S3HttpHandler handler() {
            return handler;
        }

        List<String> requests() {
            return List.copyOf(requests);
        }

        void failFirstWith(String errorCode) {
            firstErrorCode.set(errorCode);
        }

        void reset() {
            attempts.clear();
            requests.clear();
            firstErrorCode.set("InvalidAccessKeyId");
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static software.amazon.awssdk.http.HttpStatusCode.REQUEST_TIMEOUT;
import static software.amazon.awssdk.http.HttpStatusCode.SERVICE_UNAVAILABLE;
import static software.amazon.awssdk.http.HttpStatusCode.THROTTLING;

public class S3TenaciousRetriesBlobContainerTests extends S3BlobContainerRetriesTests {

    final RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
    final RepositoriesMetrics repositoriesMetrics = new RepositoriesMetrics(recordingMeterRegistry);
    final AtomicBoolean stopped = new AtomicBoolean(false);

    class TestS3TenaciousRetryBlobContainer extends S3TenaciousRetryBlobContainer {

        TestS3TenaciousRetryBlobContainer(BlobContainer delegate, RepositoriesMetrics repositoriesMetrics) {
            super(delegate, repositoriesMetrics);
        }

        @Override
        protected boolean isExceptionRetryable(Exception e) {
            if (stopped.get()) {
                return false;
            }

            return super.isExceptionRetryable(e);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new TestS3TenaciousRetryBlobContainer(child, repositoriesMetrics);
        }

        @Override
        protected long getRetryDelayInMillis(int attempt) {
            return 10;
        }

    }

    @Override
    protected BlobContainer createBlobContainer(
        final @Nullable Integer maxRetries,
        final @Nullable TimeValue readTimeout,
        final @Nullable TimeValue requestTimeout,
        final @Nullable Boolean disableChunkedEncoding,
        final @Nullable Integer maxConnections,
        final @Nullable ByteSizeValue bufferSize,
        final @Nullable Integer maxBulkDeletes,
        final @Nullable BlobPath blobContainerPath
    ) {
        BlobContainer delegate = super.createBlobContainer(
            maxRetries,
            readTimeout,
            requestTimeout,
            disableChunkedEncoding,
            maxConnections,
            bufferSize,
            maxBulkDeletes,
            blobContainerPath
        );

        return new TestS3TenaciousRetryBlobContainer(delegate, repositoriesMetrics);
    }

    public void testShouldTenaciousRetryOnRetryableExceptions() {
        final int maxRetries = between(1, 3);
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(maxRetries).build();

        final AtomicInteger attempts = new AtomicInteger(0);

        @SuppressForbidden(reason = "use a http server")
        class TenaciousRetriesHandler implements HttpHandler {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                Streams.readFully(exchange.getRequestBody());
                attempts.incrementAndGet();
                logger.debug("--> Handler hit, attempt {}", attempts.get());
                try (exchange) {
                    if (randomBoolean()) {
                        exchange.sendResponseHeaders(randomFrom(SERVICE_UNAVAILABLE, THROTTLING, REQUEST_TIMEOUT), -1);
                    } else {
                        final var responseBody = Strings.format("""
                            <?xml version="1.0" encoding="UTF-8"?>
                            <Error>
                              <Code>%s</Code>
                              <Message>InvalidAccessKeyId</Message>
                              <RequestId>%s</RequestId>
                            </Error>""", "InvalidAccessKeyId", randomUUID()).getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(RestStatus.FORBIDDEN.getStatus(), responseBody.length);
                        exchange.getResponseBody().write(responseBody);
                    }
                }
            }
        }
        httpServer.createContext("/", new TenaciousRetriesHandler());

        // No retries attempted for non INDICES purposes.
        expectThrows(
            IOException.class,
            () -> blobContainer.listBlobs(
                randomFrom(Arrays.stream(OperationPurpose.values()).filter(v -> v != OperationPurpose.INDICES).toList())
            )
        );
        expectThrows(
            IOException.class,
            () -> blobContainer.listBlobsByPrefix(
                randomFrom(Arrays.stream(OperationPurpose.values()).filter(v -> v != OperationPurpose.INDICES).toList()),
                randomIdentifier()
            )
        );
        expectThrows(
            IOException.class,
            () -> blobContainer.children(
                randomFrom(Arrays.stream(OperationPurpose.values()).filter(v -> v != OperationPurpose.INDICES).toList())
            )
        );

        attempts.set(0);
        Thread thread = new Thread(() -> expectThrows(IOException.class, () -> blobContainer.children(OperationPurpose.INDICES)));

        thread.start();
        final int targetRetryCount = randomIntBetween(10, 15);

        // Wait until we've observed the target number of retries
        try {
            assertBusy(() -> {
                recordingMeterRegistry.getRecorder().collect();
                assertThat(attempts.get(), greaterThanOrEqualTo(targetRetryCount));
                assertThat(getAttributes(recordingMeterRegistry).size(), equalTo(4));
                assertThat(getAttributes(recordingMeterRegistry).get("repo_type"), equalTo("s3"));
                assertThat(getAttributes(recordingMeterRegistry).get("operation"), equalTo("ListObjects"));
                assertThat(getAttributes(recordingMeterRegistry).get("method"), equalTo("children()"));
            });
        } catch (Exception e) {
            fail(e);
        }
        // Now stop the retries
        stopped.set(true);
        // Wait for thread to finish
        safeJoin(thread);

    }

    @Override
    public void testWriteBlobWithRetries() throws Exception {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testWriteBlobWithReadTimeouts() {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testWriteLargeBlob() throws Exception {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testWriteLargeBlobStreaming() throws Exception {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testMaxConnections() throws InterruptedException, IOException {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testReadRetriesAfterMeaningfulProgress() throws Exception {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testReadDoesNotRetryForRepositoryAnalysis() {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testReadWithIndicesPurposeRetriesForever() throws IOException {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testDoesNotRetryOnNotFound() {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testSnapshotDeletesRetryOnThrottlingError() throws IOException {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testSnapshotDeletesAbortRetriesWhenThreadIsInterrupted() {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testNonSnapshotDeletesAreNotRetried() {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testNonThrottlingErrorsAreNotRetried() {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    private int getMeasurements(RecordingMeterRegistry meterRegistry) {
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, RepositoriesMetrics.METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL)
            .size();
    }

    private Map<String, Object> getAttributes(RecordingMeterRegistry meterRegistry) {
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, RepositoriesMetrics.METRIC_TRANSIENT_ERROR_RETRY_ATTEMPTS_TOTAL)
            .getFirst()
            .attributes();
    }
}

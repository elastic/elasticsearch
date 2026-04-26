/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.cloud.storage.StorageException;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@SuppressForbidden(reason = "use a http server")
public class GoogleCloudStorageTenaciousRetriesBlobContainerTests extends GoogleCloudStorageBlobContainerRetriesTests {

    class TestGcsTenaciousRetryBlobContainer extends GcsTenaciousRetryBlobContainer {
        TestGcsTenaciousRetryBlobContainer(BlobContainer delegate, RepositoriesMetrics repositoriesMetrics) {
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
            return new TestGcsTenaciousRetryBlobContainer(child, repositoriesMetrics);
        }

        @Override
        protected long getRetryDelayInMillis(int attempt) {
            return 1;
        }
    }

    final RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
    final RepositoriesMetrics repositoriesMetrics = new RepositoriesMetrics(recordingMeterRegistry);
    final AtomicBoolean stopped = new AtomicBoolean(false);

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

        return new TestGcsTenaciousRetryBlobContainer(delegate, repositoriesMetrics);
    }

    @Override
    public void testShouldRetryOnUnresolvableHost() {
        endpointUrlOverride = "http://unresolvable.invalid";

        final int maxRetries = randomIntBetween(4, 5);
        final BlobContainer blobContainer = blobContainerBuilder().maxRetries(maxRetries).build();
        // No additional retries for NON INDICES purposes.
        expectThrows(
            StorageException.class,
            () -> blobContainer.listBlobs(
                randomFrom(Arrays.stream(OperationPurpose.values()).filter(v -> v != OperationPurpose.INDICES).toList())
            )
        );
        assertEquals(maxRetries + 1, requestCounters.get("/storage/v1/b/bucket/o").get());

        // Infinite retries for children() with INDICES purposes.
        Thread thread = new Thread(() -> expectThrows(StorageException.class, () -> blobContainer.children(OperationPurpose.INDICES)));

        thread.start();

        // Tenacious retries
        final int targetRetryCount = randomIntBetween(20, 30);

        // Wait until we've observed the target number of retries
        try {
            assertBusy(() -> {
                recordingMeterRegistry.getRecorder().collect();
                assertThat(getMeasurements(recordingMeterRegistry), greaterThanOrEqualTo(targetRetryCount));
                assertThat(getAttributes(recordingMeterRegistry).size(), equalTo(3));
                assertThat(getAttributes(recordingMeterRegistry).get("repo_type"), equalTo("gcs"));
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
    public void testRetriesAreTerminatedWhenClientProviderIsClosed() {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testReadLargeBlobWithRetries() throws Exception {
        assumeTrue("List Operation only, Not currently needed", true);
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
    public void testWriteLargeBlob() throws IOException {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testDeleteBatchesAreSentIncrementally() throws Exception {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testCompareAndExchangeWhenThrottled() throws IOException {
        assumeTrue("List Operation only, Not currently needed", true);
    }

    @Override
    public void testContentsChangeWhileStreaming() throws IOException {
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

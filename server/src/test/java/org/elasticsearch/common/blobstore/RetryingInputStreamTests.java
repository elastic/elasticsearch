/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.blobstore.RetryingInputStream.StreamAction.OPEN;
import static org.elasticsearch.common.blobstore.RetryingInputStream.StreamAction.READ;
import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomFiniteRetryingPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomRetryingPurpose;
import static org.hamcrest.Matchers.equalTo;

public class RetryingInputStreamTests extends ESTestCase {

    public void testRetryableErrorsWhenReadingAreRetried() throws IOException {
        final var retryableFailures = randomIntBetween(1, 5);
        final var failureCounter = new AtomicInteger(retryableFailures);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());
        final var eTag = randomUUID();
        final var purpose = randomRetryingPurpose();

        final var services = new BlobStoreServicesAdapter(purpose, retryableFailures * 2) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                return createSingleAttemptInputStream(resourceBytes, eTag, (int) start, failureCounter.getAndDecrement() > 0);
            }
        };

        byte[] results = copyToBytes(new ShortDelayRetryingInputStream(services, purpose));
        assertEquals(resourceBytes.length(), results.length);
        assertThat(new BytesArray(results), equalBytes(resourceBytes));
        assertEquals(retryableFailures + 1, services.getAttempts());
    }

    public void testReadWillFailWhenRetryableErrorsExceedMaxRetries() {
        final var maxRetries = randomIntBetween(1, 5);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(10, 100)).getBytes());
        final var eTag = randomUUID();
        final var purpose = randomFiniteRetryingPurpose();

        final var services = new BlobStoreServicesAdapter(purpose, maxRetries) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                return createSingleAttemptInputStream(resourceBytes, eTag, (int) start, true);
            }
        };

        final var ioException = assertThrows(IOException.class, () -> copyToBytes(new ShortDelayRetryingInputStream(services, purpose)));
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(maxRetries + 1, services.getAttempts());
    }

    public void testReadWillFailWhenRetryableErrorsOccurDuringRepositoryAnalysis() {
        final var maxRetries = randomIntBetween(2, 5);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());
        final var eTag = randomUUID();
        final var purpose = OperationPurpose.REPOSITORY_ANALYSIS;

        final var services = new BlobStoreServicesAdapter(purpose, maxRetries) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                return createSingleAttemptInputStream(resourceBytes, eTag, (int) start, true);
            }
        };

        final var ioException = assertThrows(IOException.class, () -> copyToBytes(new ShortDelayRetryingInputStream(services, purpose)));
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(1, services.getAttempts());
    }

    public void testReadWillRetryIndefinitelyWhenErrorsOccurDuringIndicesOperation() throws IOException {
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());
        final int numberOfFailures = randomIntBetween(1, 10);
        final AtomicInteger failureCounter = new AtomicInteger(numberOfFailures);
        final var eTag = randomUUID();
        final var indices = OperationPurpose.INDICES;

        final var services = new BlobStoreServicesAdapter(indices, 0) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                return createSingleAttemptInputStream(resourceBytes, eTag, (int) start, failureCounter.getAndDecrement() > 0);
            }
        };

        byte[] result = copyToBytes(new ShortDelayRetryingInputStream(services, indices));
        assertThat(new BytesArray(result), equalBytes(resourceBytes));
        assertEquals(numberOfFailures + 1, services.getAttempts());
    }

    public void testRetriesWillBeExtendedWhenMeaningfulProgressIsMade() {
        final var maxRetries = randomIntBetween(1, 5);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(100, 150)).getBytes());
        final var meaningfulProgressSize = randomIntBetween(1024, 4096);
        final var meaningfulProgressAttempts = randomIntBetween(1, 3);
        final var meaningfulProgressAttemptsCounter = new AtomicInteger(meaningfulProgressAttempts);
        final var eTag = randomUUID();
        final var purpose = randomFiniteRetryingPurpose();

        final var services = new BlobStoreServicesAdapter(purpose, maxRetries) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                final var inputStream = meaningfulProgressAttemptsCounter.decrementAndGet() > 0
                    ? createSingleAttemptInputStream(resourceBytes, eTag, (int) start, true, meaningfulProgressSize, Integer.MAX_VALUE)
                    : createSingleAttemptInputStream(resourceBytes, eTag, (int) start, true, 1, meaningfulProgressSize - 1);
                return inputStream;
            }

            @Override
            public long getMeaningfulProgressSize() {
                return meaningfulProgressSize;
            }
        };

        final var ioException = assertThrows(IOException.class, () -> copyToBytes(new ShortDelayRetryingInputStream(services, purpose)));
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(maxRetries + meaningfulProgressAttempts, services.getAttempts());
    }

    public void testNoSuchFileExceptionAndRangeNotSatisfiedTerminatesWithoutRetry() {
        final var notRetriableException = randomFrom(
            new NoSuchFileException("This is not retry-able"),
            new RequestedRangeNotSatisfiedException("This is not retry-able", randomLong(), randomLong()),
            new IOException("This is not retry-able")
        );
        final var retryableFailures = randomIntBetween(1, 5);
        final var failureCounter = new AtomicInteger(retryableFailures);
        final var purpose = randomRetryingPurpose();

        final var services = new BlobStoreServicesAdapter(purpose, retryableFailures * 2) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                if (failureCounter.getAndDecrement() > 0) {
                    throw new RuntimeException("This is retry-able");
                }
                throw notRetriableException;
            }
        };
        final IOException ioException = assertThrows(
            IOException.class,
            () -> copyToBytes(new ShortDelayRetryingInputStream(services, purpose))
        );
        assertSame(notRetriableException, ioException);
        assertEquals(retryableFailures + 1, services.getAttempts());
    }

    public void testBlobVersionIsRequestedForSecondAndSubsequentAttempts() throws IOException {
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());
        final int numberOfFailures = randomIntBetween(1, 10);
        final AtomicInteger failureCounter = new AtomicInteger(numberOfFailures);
        final var eTag = randomUUID();
        final var purpose = randomRetryingPurpose();

        final var services = new BlobStoreServicesAdapter(purpose, numberOfFailures) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                if (getAttempts() > 1) {
                    assertEquals(eTag, version);
                } else {
                    assertNull(version);
                }
                return createSingleAttemptInputStream(resourceBytes, eTag, (int) start, failureCounter.getAndDecrement() > 0);
            }
        };

        copyToBytes(new ShortDelayRetryingInputStream(services, purpose));
    }

    public void testSkipWillRetry() throws IOException {
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());
        final int numberOfFailures = randomIntBetween(1, 10);
        final AtomicInteger failureCounter = new AtomicInteger(numberOfFailures);
        final var eTag = randomUUID();
        final var purpose = randomRetryingPurpose();

        final var services = new BlobStoreServicesAdapter(purpose, numberOfFailures) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                return createSingleAttemptInputStream(resourceBytes, eTag, (int) start, failureCounter.getAndDecrement() > 0);
            }
        };

        try (var inputStream = new ShortDelayRetryingInputStream(services, purpose)) {
            assertEquals(resourceBytes.length() - 1, inputStream.skip(resourceBytes.length() - 1));
        }
    }

    public void testRetryMetricsArePublished() throws IOException {
        final var recordingMeterRegistry = new RecordingMeterRegistry();
        final int maxRetries = randomIntBetween(8, 10);
        final int getRetries = randomIntBetween(1, maxRetries - 1);
        final var getRetriesRemaining = new AtomicInteger(getRetries);
        final int readRetries = randomIntBetween(1, maxRetries - getRetries);
        final var readRetriesRemaining = new AtomicInteger(readRetries);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());
        final var eTag = randomUUID();
        final var purpose = randomRetryingPurpose();

        final var services = new BlobStoreServicesAdapter(purpose, maxRetries) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(@Nullable String version, long start, long end)
                throws IOException {
                if (getRetriesRemaining.get() > 0) {
                    getRetriesRemaining.decrementAndGet();
                    throw new RuntimeException("This is retry-able");
                }
                if (readRetriesRemaining.get() > 0) {
                    readRetriesRemaining.decrementAndGet();
                    // we need to fail to read any bytes for the read attempts to accumulate
                    // we consider the first successful read as a success
                    return createSingleAttemptInputStream(resourceBytes, eTag, (int) start, true, 0, 0);
                }
                return createSingleAttemptInputStream(resourceBytes, eTag, (int) start, false);
            }
        };

        final byte[] result = copyToBytes(
            new ShortDelayRetryingInputStream(new RepositoriesMetrics(recordingMeterRegistry), services, purpose)
        );
        assertThat(new BytesArray(result), equalBytes(resourceBytes));
        recordingMeterRegistry.getRecorder().collect();
        assertThat(
            getMeasurement(
                recordingMeterRegistry,
                InstrumentType.LONG_COUNTER,
                RepositoriesMetrics.METRIC_INPUT_STREAM_RETRY_EVENT_TOTAL,
                OPEN
            ).getLong(),
            equalTo(1L)
        );
        assertThat(
            getMeasurement(
                recordingMeterRegistry,
                InstrumentType.LONG_COUNTER,
                RepositoriesMetrics.METRIC_INPUT_STREAM_RETRY_SUCCESS_TOTAL,
                OPEN
            ).getLong(),
            equalTo(1L)
        );
        assertThat(
            getMeasurement(
                recordingMeterRegistry,
                InstrumentType.LONG_HISTOGRAM,
                RepositoriesMetrics.METRIC_INPUT_STREAM_RETRY_ATTEMPTS_HISTOGRAM,
                OPEN
            ).getLong(),
            equalTo((long) getRetries)
        );
        assertThat(
            getMeasurement(
                recordingMeterRegistry,
                InstrumentType.LONG_COUNTER,
                RepositoriesMetrics.METRIC_INPUT_STREAM_RETRY_EVENT_TOTAL,
                READ
            ).getLong(),
            equalTo(1L)
        );
        assertThat(
            getMeasurement(
                recordingMeterRegistry,
                InstrumentType.LONG_COUNTER,
                RepositoriesMetrics.METRIC_INPUT_STREAM_RETRY_SUCCESS_TOTAL,
                READ
            ).getLong(),
            equalTo(1L)
        );
        assertThat(
            getMeasurement(
                recordingMeterRegistry,
                InstrumentType.LONG_HISTOGRAM,
                RepositoriesMetrics.METRIC_INPUT_STREAM_RETRY_ATTEMPTS_HISTOGRAM,
                READ
            ).getLong(),
            equalTo((long) readRetries)
        );
    }

    private Measurement getMeasurement(
        RecordingMeterRegistry meterRegistry,
        InstrumentType instrumentType,
        String metricName,
        RetryingInputStream.StreamAction action
    ) {
        return meterRegistry.getRecorder()
            .getMeasurements(instrumentType, metricName)
            .stream()
            .filter(measurement -> Objects.equals(measurement.attributes().get("es_retry_action"), action.getPastTense()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Measurement not found for action " + action));
    }

    /**
     * RetryingInputStream with a short fixed delay so these tests run quickly
     */
    private static final class ShortDelayRetryingInputStream extends RetryingInputStream<String> {

        ShortDelayRetryingInputStream(
            RepositoriesMetrics repositoriesMetrics,
            BlobStoreServices<String> blobStoreServices,
            OperationPurpose operationPurpose
        ) throws IOException {
            super(repositoriesMetrics, blobStoreServices, operationPurpose);
        }

        ShortDelayRetryingInputStream(BlobStoreServices<String> blobStoreServices, OperationPurpose purpose) throws IOException {
            super(RepositoriesMetrics.NOOP, blobStoreServices, purpose);
        }

        @Override
        protected long getRetryDelayInMillis() {
            return 1;
        }
    }

    private static byte[] copyToBytes(InputStream inputStream) throws IOException {
        final var outputStream = new ByteArrayOutputStream();
        if (randomBoolean()) {
            Streams.copy(inputStream, outputStream);
        } else {
            while (true) {
                final int read = inputStream.read();
                if (read == -1) {
                    break;
                }
                outputStream.write(read);
            }
        }
        return outputStream.toByteArray();
    }

    private abstract static class BlobStoreServicesAdapter implements RetryingInputStream.BlobStoreServices<String> {

        private final OperationPurpose purpose;
        private final String repositoryName;
        private final AtomicInteger attemptCounter = new AtomicInteger();
        private final int maxRetries;

        private BlobStoreServicesAdapter(OperationPurpose purpose, int maxRetries) {
            this.purpose = purpose;
            this.maxRetries = maxRetries;
            this.repositoryName = randomIdentifier("repo-");
        }

        @Override
        public final RetryingInputStream.SingleAttemptInputStream<String> getInputStream(@Nullable String version, long start, long end)
            throws IOException {
            attemptCounter.incrementAndGet();
            return doGetInputStream(version, start, end);
        }

        protected abstract RetryingInputStream.SingleAttemptInputStream<String> doGetInputStream(
            @Nullable String version,
            long start,
            long end
        ) throws IOException;

        @Override
        public boolean isRetryableException(RetryingInputStream.StreamAction action, Exception e) {
            return switch (action) {
                case OPEN -> e instanceof RuntimeException;
                case READ -> e instanceof IOException;
            };
        }

        @Override
        public long getMeaningfulProgressSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public final int getMaxRetries() {
            return maxRetries;
        }

        @Override
        public String getBlobDescription() {
            return "";
        }

        @Override
        public Map<String, Object> getMetricsAttributes(RetryingInputStream.StreamAction action) {
            return Map.of(
                "repo_type",
                "test_type",
                "repo_name",
                repositoryName,
                "operation",
                "GET_BLOB",
                "purpose",
                purpose.getKey(),
                "es_retry_action",
                action.getPastTense()
            );
        }

        public int getAttempts() {
            return attemptCounter.get();
        }
    }

    private static RetryingInputStream.SingleAttemptInputStream<String> createSingleAttemptInputStream(
        BytesReference bytesReference,
        String version,
        int startIndex,
        boolean failBeforeEnd
    ) throws IOException {
        return createSingleAttemptInputStream(bytesReference, version, startIndex, failBeforeEnd, 1, Integer.MAX_VALUE);
    }

    private static RetryingInputStream.SingleAttemptInputStream<String> createSingleAttemptInputStream(
        BytesReference bytesReference,
        String version,
        int startIndex,
        boolean failBeforeEnd,
        int minimumSuccess,
        int maximumSuccess
    ) throws IOException {
        if (failBeforeEnd) {
            return new RetryingInputStream.SingleAttemptInputStream<>(
                new FailureAtIndexInputStream(bytesReference, startIndex, minimumSuccess, maximumSuccess),
                startIndex,
                version
            );
        }
        return new RetryingInputStream.SingleAttemptInputStream<>(inputStreamAtIndex(bytesReference, startIndex), startIndex, version);
    }

    private static class FailureAtIndexInputStream extends InputStream {

        private final InputStream inputStream;
        private int readRemaining;

        private FailureAtIndexInputStream(BytesReference bytesReference, int startIndex, int minimumSuccess, int maximumSuccess)
            throws IOException {
            this.inputStream = inputStreamAtIndex(bytesReference, startIndex);
            final int remainingBytes = bytesReference.length() - startIndex;
            this.readRemaining = randomIntBetween(minimumSuccess, Math.min(maximumSuccess, remainingBytes / 2));
        }

        @Override
        public int read() throws IOException {
            if (readRemaining > 0) {
                readRemaining--;
                return inputStream.read();
            } else {
                throw new IOException("This is retry-able");
            }
        }

        @Override
        public String toString() {
            return "Failing after " + readRemaining;
        }
    }

    private static InputStream inputStreamAtIndex(BytesReference bytesReference, int startIndex) throws IOException {
        final int remainingBytes = bytesReference.length() - startIndex;
        return bytesReference.slice(startIndex, remainingBytes).streamInput();
    }
}

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
import org.elasticsearch.core.Streams;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.hamcrest.Matchers.empty;

public class RetryingInputStreamTests extends ESTestCase {

    public void testRetryableErrorsWhenReadingAreRetried() throws IOException {
        final var retryableFailures = randomIntBetween(1, 5);
        final var failureCounter = new AtomicInteger(retryableFailures);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());

        final var services = new BlobStoreServicesAdapter(retryableFailures * 2) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream doGetInputStream(long start, long end) throws IOException {
                return new FailureAtIndexInputStream(resourceBytes, (int) start, failureCounter.getAndDecrement() > 0);
            }
        };

        byte[] results = copyToBytes(new RetryingInputStream(services, randomRetryingPurpose()) {
        });
        assertEquals(resourceBytes.length(), results.length);
        assertThat(new BytesArray(results), equalBytes(resourceBytes));
        assertEquals(retryableFailures + 1, services.getAttempts());
        assertEquals(Stream.generate(() -> "read").limit(retryableFailures).toList(), services.getRetryStarted());
    }

    public void testReadWillFailWhenRetryableErrorsExceedMaxRetries() {
        final var maxRetries = randomIntBetween(1, 5);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(10, 100)).getBytes());

        final var services = new BlobStoreServicesAdapter(maxRetries) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream doGetInputStream(long start, long end) throws IOException {
                return new FailureAtIndexInputStream(resourceBytes, (int) start, true);
            }
        };

        final var ioException = assertThrows(
            IOException.class,
            () -> copyToBytes(new RetryingInputStream(services, randomFiniteRetryingPurpose()) {
            })
        );
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(maxRetries + 1, services.getAttempts());
        assertEquals(Stream.generate(() -> "read").limit(maxRetries + 1).toList(), services.getRetryStarted());
    }

    public void testReadWillFailWhenRetryableErrorsOccurDuringRepositoryAnalysis() {
        final var maxRetries = randomIntBetween(2, 5);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());

        final var services = new BlobStoreServicesAdapter(maxRetries) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream doGetInputStream(long start, long end) throws IOException {
                return new FailureAtIndexInputStream(resourceBytes, (int) start, true);
            }
        };

        final var ioException = assertThrows(
            IOException.class,
            () -> copyToBytes(new RetryingInputStream(services, OperationPurpose.REPOSITORY_ANALYSIS) {
            })
        );
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(1, services.getAttempts());
        assertEquals(List.of("read"), services.getRetryStarted());
    }

    public void testReadWillRetryIndefinitelyWhenErrorsOccurDuringIndicesOperation() throws IOException {
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(5, 200)).getBytes());
        final int numberOfFailures = randomIntBetween(1, 10);
        final AtomicInteger failureCounter = new AtomicInteger(numberOfFailures);

        final var services = new BlobStoreServicesAdapter(0) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream doGetInputStream(long start, long end) throws IOException {
                return new FailureAtIndexInputStream(resourceBytes, (int) start, failureCounter.getAndDecrement() > 0);
            }
        };

        byte[] result = copyToBytes(new RetryingInputStream(services, OperationPurpose.INDICES) {
        });
        assertThat(new BytesArray(result), equalBytes(resourceBytes));
        assertEquals(numberOfFailures + 1, services.getAttempts());
        assertEquals(Stream.generate(() -> "read").limit(numberOfFailures).toList(), services.getRetryStarted());
    }

    public void testRetriesWillBeExtendedWhenMeaningfulProgressIsMade() {
        final var maxRetries = randomIntBetween(1, 5);
        final var resourceBytes = randomBytesReference((int) ByteSizeValue.ofKb(randomIntBetween(100, 150)).getBytes());
        final var meaningfulProgressSize = randomIntBetween(1024, 4096);
        final var meaningfulProgressAttempts = randomIntBetween(1, 3);
        final var meaningfulProgressAttemptsCounter = new AtomicInteger(meaningfulProgressAttempts);

        final var services = new BlobStoreServicesAdapter(maxRetries) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream doGetInputStream(long start, long end) throws IOException {
                final var inputStream = meaningfulProgressAttemptsCounter.decrementAndGet() > 0
                    ? new FailureAtIndexInputStream(resourceBytes, (int) start, true, meaningfulProgressSize, Integer.MAX_VALUE)
                    : new FailureAtIndexInputStream(resourceBytes, (int) start, true, 1, meaningfulProgressSize - 1);
                return inputStream;
            }

            @Override
            public long getMeaningfulProgressSize() {
                return meaningfulProgressSize;
            }
        };

        final var ioException = assertThrows(
            IOException.class,
            () -> copyToBytes(new RetryingInputStream(services, randomFiniteRetryingPurpose()) {
            })
        );
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(maxRetries + meaningfulProgressAttempts, services.getAttempts());
        assertEquals(Stream.generate(() -> "read").limit(maxRetries + meaningfulProgressAttempts).toList(), services.getRetryStarted());
    }

    public void testNoSuchFileExceptionAndRangeNotSatisfiedTerminatesWithoutRetry() {
        final var notRetriableException = randomFrom(
            new NoSuchFileException("This is not retry-able"),
            new RequestedRangeNotSatisfiedException("This is not retry-able", randomLong(), randomLong()),
            new IOException("This is not retry-able")
        );
        final var retryableFailures = randomIntBetween(1, 5);
        final var failureCounter = new AtomicInteger(retryableFailures);

        final var services = new BlobStoreServicesAdapter(retryableFailures * 2) {
            @Override
            public RetryingInputStream.SingleAttemptInputStream doGetInputStream(long start, long end) throws IOException {
                if (failureCounter.getAndDecrement() > 0) {
                    throw new RuntimeException("This is retry-able");
                }
                throw notRetriableException;
            }
        };
        final IOException ioException = assertThrows(
            IOException.class,
            () -> copyToBytes(new RetryingInputStream(services, randomRetryingPurpose()) {
            })
        );
        assertSame(notRetriableException, ioException);
        assertEquals(retryableFailures + 1, services.getAttempts());
        assertEquals(List.of("open"), services.getRetryStarted());
        assertThat(services.getRetrySucceeded(), empty());
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

    private abstract static class BlobStoreServicesAdapter implements RetryingInputStream.BlobStoreServices {

        private final AtomicInteger attemptCounter = new AtomicInteger();
        private final List<String> retryStarted = new ArrayList<>();
        private final List<Success> retrySucceeded = new ArrayList<>();
        private final int maxRetries;

        private BlobStoreServicesAdapter(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public final RetryingInputStream.SingleAttemptInputStream getInputStream(long start, long end) throws IOException {
            attemptCounter.incrementAndGet();
            return doGetInputStream(start, end);
        }

        protected abstract RetryingInputStream.SingleAttemptInputStream doGetInputStream(long start, long end) throws IOException;

        @Override
        public void onRetryStarted(String action) {
            retryStarted.add(action);
        }

        @Override
        public void onRetrySucceeded(String action, long numberOfRetries) {
            retrySucceeded.add(new Success(action, numberOfRetries));
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

        record Success(String action, long numberOfRetries) {};

        public int getAttempts() {
            return attemptCounter.get();
        }

        public List<String> getRetryStarted() {
            return retryStarted;
        }

        public List<Success> getRetrySucceeded() {
            return retrySucceeded;
        }
    }

    private static class FailureAtIndexInputStream extends RetryingInputStream.SingleAttemptInputStream {

        private final long firstOffset;
        private final InputStream delegate;
        private int readRemaining;

        private FailureAtIndexInputStream(BytesReference bytesReference, int startIndex, boolean failBeforeEnd) throws IOException {
            this(bytesReference, startIndex, failBeforeEnd, 1, Integer.MAX_VALUE);
        }

        private FailureAtIndexInputStream(
            BytesReference bytesReference,
            int startIndex,
            boolean failBeforeEnd,
            int minimumSuccess,
            int maximumSuccess
        ) throws IOException {
            final int remainingBytes = bytesReference.length() - startIndex;
            this.delegate = bytesReference.slice(startIndex, remainingBytes).streamInput();
            if (failBeforeEnd) {
                this.readRemaining = randomIntBetween(Math.max(1, minimumSuccess), Math.min(maximumSuccess, remainingBytes / 2));
            } else {
                this.readRemaining = Integer.MAX_VALUE;
            }
            this.firstOffset = startIndex;
        }

        @Override
        public int read() throws IOException {
            if (readRemaining > 0) {
                readRemaining--;
                return delegate.read();
            } else {
                throw new IOException("This is retry-able");
            }
        }

        @Override
        public String toString() {
            return "Failing after " + readRemaining;
        }

        @Override
        protected long getFirstOffset() {
            return firstOffset;
        }
    }

    public static OperationPurpose randomRetryingPurpose() {
        return randomValueOtherThan(OperationPurpose.REPOSITORY_ANALYSIS, BlobStoreTestUtil::randomPurpose);
    }

    public static OperationPurpose randomFiniteRetryingPurpose() {
        return randomValueOtherThanMany(
            purpose -> purpose == OperationPurpose.REPOSITORY_ANALYSIS || purpose == OperationPurpose.INDICES,
            BlobStoreTestUtil::randomPurpose
        );
    }
}

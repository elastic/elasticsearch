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
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.elasticsearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase.randomFiniteRetryingPurpose;
import static org.elasticsearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase.randomRetryingPurpose;
import static org.hamcrest.Matchers.empty;

public class RetryingInputStreamTests extends ESTestCase {

    public void testRetryableErrorsWhenReadingAreRetried() throws IOException {
        final var attemptCounter = new AtomicInteger();
        final var retryableFailures = randomIntBetween(1, 5);
        final var failureCounter = new AtomicInteger(retryableFailures);
        final var retryStarted = new ArrayList<String>();
        final var retrySucceeded = new ArrayList<BlobStoreServicesAdapter.Success>();
        final var resourceSize = ByteSizeValue.ofKb(randomIntBetween(5, 200));
        logger.info("--> resource size: {}", resourceSize);
        final var resourceBytes = randomBytesReference((int) resourceSize.getBytes());
        final var eTag = randomUUID();

        final var stream = new RetryingInputStream<>(new BlobStoreServicesAdapter(retryStarted, retrySucceeded) {
            @Override
            public RetryingInputStream.InputStreamAtVersion<String> getInputStreamAtVersion(String version, long start, long end)
                throws IOException {
                attemptCounter.incrementAndGet();
                final var inputStream = new FailureAtIndexInputStream(resourceBytes, (int) start, failureCounter.getAndDecrement() > 0);
                logger.info("--> reading from stream at version [{} -> {}] {}", start, end, inputStream);
                return new RetryingInputStream.InputStreamAtVersion<>(inputStream, eTag);
            }

            @Override
            public int getMaxRetries() {
                return retryableFailures * 2;
            }
        }, randomRetryingPurpose()) {
        };

        final var out = new ByteArrayOutputStream();
        Streams.copy(stream, out);
        assertEquals(resourceBytes.length(), out.size());
        assertEquals(resourceBytes, new BytesArray(out.toByteArray()));
        assertEquals(retryableFailures + 1, attemptCounter.get());
        assertEquals(Stream.generate(() -> "read").limit(retryableFailures).toList(), retryStarted);
    }

    public void testReadWillFailWhenRetryableErrorsExceedMaxRetries() throws IOException {
        final var attemptCounter = new AtomicInteger();
        final var maxRetries = randomIntBetween(1, 5);
        final var retryStarted = new ArrayList<String>();
        final var retrySucceeded = new ArrayList<BlobStoreServicesAdapter.Success>();
        final var resourceSize = ByteSizeValue.ofKb(randomIntBetween(10, 100));
        logger.info("--> resource size: {}", resourceSize);
        final var resourceBytes = randomBytesReference((int) resourceSize.getBytes());
        final var eTag = randomUUID();

        final var stream = new RetryingInputStream<>(new BlobStoreServicesAdapter(retryStarted, retrySucceeded) {
            @Override
            public RetryingInputStream.InputStreamAtVersion<String> getInputStreamAtVersion(String version, long start, long end)
                throws IOException {
                attemptCounter.incrementAndGet();
                final var inputStream = new FailureAtIndexInputStream(resourceBytes, (int) start, true);
                logger.info("--> reading from stream at version [{} -> {}] {}", start, end, inputStream);
                return new RetryingInputStream.InputStreamAtVersion<>(inputStream, eTag);
            }

            @Override
            public int getMaxRetries() {
                return maxRetries;
            }
        }, randomFiniteRetryingPurpose()) {
        };

        final var out = new ByteArrayOutputStream();
        final var ioException = assertThrows(IOException.class, () -> copy(stream, out));
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(maxRetries + 1, attemptCounter.get());
        assertEquals(Stream.generate(() -> "read").limit(maxRetries + 1).toList(), retryStarted);
    }

    public void testReadWillFailWhenRetryableErrorsOccurDuringRepositoryAnalysis() throws IOException {
        final var attemptCounter = new AtomicInteger();
        final var maxRetries = randomIntBetween(2, 5);
        final var retryStarted = new ArrayList<String>();
        final var retrySucceeded = new ArrayList<BlobStoreServicesAdapter.Success>();
        final var resourceSize = ByteSizeValue.ofKb(randomIntBetween(5, 200));
        logger.info("--> resource size: {}", resourceSize);
        final var resourceBytes = randomBytesReference((int) resourceSize.getBytes());
        final var eTag = randomUUID();

        final var stream = new RetryingInputStream<>(new BlobStoreServicesAdapter(retryStarted, retrySucceeded) {
            @Override
            public RetryingInputStream.InputStreamAtVersion<String> getInputStreamAtVersion(String version, long start, long end)
                throws IOException {
                attemptCounter.incrementAndGet();
                final var inputStream = new FailureAtIndexInputStream(resourceBytes, (int) start, true);
                logger.info("--> reading from stream at version [{} -> {}] {}", start, end, inputStream);
                return new RetryingInputStream.InputStreamAtVersion<>(inputStream, eTag);
            }

            @Override
            public int getMaxRetries() {
                return maxRetries;
            }
        }, OperationPurpose.REPOSITORY_ANALYSIS) {
        };

        final var ioException = assertThrows(IOException.class, () -> copy(stream, OutputStream.nullOutputStream()));
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(1, attemptCounter.get());
        assertEquals(List.of("read"), retryStarted);
    }

    public void testRetriesWillBeExtendedWhenMeaningfulProgressIsMade() throws IOException {
        final var attemptCounter = new AtomicInteger();
        final var meaningfulProgressAttempts = randomIntBetween(1, 3);
        final var maxRetries = randomIntBetween(1, 5);
        final var meaningfulProgressSize = randomIntBetween(1024, 4096);
        final var retryStarted = new ArrayList<String>();
        final var retrySucceeded = new ArrayList<BlobStoreServicesAdapter.Success>();
        final var resourceSize = ByteSizeValue.ofKb(randomIntBetween(100, 150));
        logger.info("--> resource size: {}", resourceSize);
        final var resourceBytes = randomBytesReference((int) resourceSize.getBytes());
        final var eTag = randomUUID();
        final AtomicInteger meaningfulProgressAttemptsCounter = new AtomicInteger(meaningfulProgressAttempts);

        final var stream = new RetryingInputStream<>(new BlobStoreServicesAdapter(retryStarted, retrySucceeded) {
            @Override
            public RetryingInputStream.InputStreamAtVersion<String> getInputStreamAtVersion(String version, long start, long end)
                throws IOException {
                attemptCounter.incrementAndGet();
                final var inputStream = meaningfulProgressAttemptsCounter.decrementAndGet() > 0
                    ? new FailureAtIndexInputStream(resourceBytes, (int) start, true, meaningfulProgressSize, Integer.MAX_VALUE)
                    : new FailureAtIndexInputStream(resourceBytes, (int) start, true, 1, meaningfulProgressSize - 1);
                logger.info("--> reading from stream at version [{} -> {}] {}", start, end, inputStream);
                return new RetryingInputStream.InputStreamAtVersion<>(inputStream, eTag);
            }

            @Override
            public int getMaxRetries() {
                return maxRetries;
            }

            @Override
            public long getMeaningfulProgressSize() {
                return meaningfulProgressSize;
            }
        }, randomFiniteRetryingPurpose()) {
        };

        final var out = new ByteArrayOutputStream();
        final var ioException = assertThrows(IOException.class, () -> copy(stream, out));
        assertEquals("This is retry-able", ioException.getMessage());
        assertEquals(maxRetries + meaningfulProgressAttempts, attemptCounter.get());
        assertEquals(Stream.generate(() -> "read").limit(maxRetries + meaningfulProgressAttempts).toList(), retryStarted);
    }

    public void testNoSuchFileExceptionAndRangeNotSatisfiedTerminatesWithoutRetry() {
        final var notRetriableException = randomBoolean()
            ? new NoSuchFileException("This is not retry-able")
            : new RequestedRangeNotSatisfiedException("This is not retry-able", randomLong(), randomLong());
        final var attemptCounter = new AtomicInteger();
        final var retryableFailures = randomIntBetween(1, 5);
        final var failureCounter = new AtomicInteger(retryableFailures);
        final var retryStarted = new ArrayList<String>();
        final var retrySucceeded = new ArrayList<BlobStoreServicesAdapter.Success>();

        final IOException ioException = assertThrows(IOException.class, () -> {
            final var stream = new RetryingInputStream<>(new BlobStoreServicesAdapter(retryStarted, retrySucceeded) {
                @Override
                public RetryingInputStream.InputStreamAtVersion<String> getInputStreamAtVersion(String version, long start, long end)
                    throws IOException {
                    attemptCounter.incrementAndGet();
                    if (failureCounter.getAndDecrement() > 0) {
                        if (randomBoolean()) {
                            throw new RuntimeException("This is retry-able");
                        } else {
                            throw new IOException("This is retry-able");
                        }
                    }
                    throw notRetriableException;
                }

                @Override
                public int getMaxRetries() {
                    return retryableFailures * 2;
                }
            }, randomRetryingPurpose()) {
            };
            copy(stream, OutputStream.nullOutputStream());
        });
        assertSame(notRetriableException, ioException);
        assertEquals(retryableFailures + 1, attemptCounter.get());
        assertEquals(List.of("open"), retryStarted);
        assertThat(retrySucceeded, empty());
    }

    private static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
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
    }

    private abstract static class BlobStoreServicesAdapter implements RetryingInputStream.BlobStoreServices<String> {

        private final List<String> retryStarted;
        private final List<Success> retrySucceeded;

        BlobStoreServicesAdapter(List<String> retryStarted, List<Success> retrySucceeded) {
            this.retryStarted = retryStarted;
            this.retrySucceeded = retrySucceeded;
        }

        BlobStoreServicesAdapter() {
            this(new ArrayList<>(), new ArrayList<>());
        }

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
        public int getMaxRetries() {
            return 0;
        }

        @Override
        public String getBlobDescription() {
            return "";
        }

        record Success(String action, long numberOfRetries) {};
    }

    private static class FailureAtIndexInputStream extends InputStream {

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
    }
}

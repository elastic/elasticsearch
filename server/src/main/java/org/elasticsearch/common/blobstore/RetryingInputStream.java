/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

public abstract class RetryingInputStream<V> extends InputStream {

    private static final Logger logger = LogManager.getLogger(RetryingInputStream.class);

    static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    private final BlobStoreServices<V> blobStoreServices;
    private final OperationPurpose purpose;
    private final long start;
    private final long end;
    private final List<Exception> failures;

    protected InputStreamAtVersion<V> currentStream;
    private long currentStreamFirstOffset;
    private int attempt = 1;
    private int failuresAfterMeaningfulProgress = 0;
    private int currentOffset = 0;
    private boolean closed = false;

    protected RetryingInputStream(BlobStoreServices<V> blobStoreServices, OperationPurpose purpose) throws IOException {
        this(blobStoreServices, purpose, 0L, Long.MAX_VALUE - 1L);
    }

    @SuppressWarnings("this-escape") // TODO: We can do better than this but I don't want to touch the tests for the first implementation
    protected RetryingInputStream(BlobStoreServices<V> blobStoreServices, OperationPurpose purpose, long start, long end)
        throws IOException {
        if (start < 0L) {
            throw new IllegalArgumentException("start must be non-negative");
        }
        if (end < start || end == Long.MAX_VALUE) {
            throw new IllegalArgumentException("end must be >= start and not Long.MAX_VALUE");
        }
        this.blobStoreServices = blobStoreServices;
        this.purpose = purpose;
        this.failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
        this.start = start;
        this.end = end;
        final int initialAttempt = attempt;
        openStreamWithRetry();
        maybeLogAndRecordMetricsForSuccess(initialAttempt, "open");
    }

    private void openStreamWithRetry() throws IOException {
        while (true) {
            if (currentOffset > 0 || start > 0 || end < Long.MAX_VALUE - 1) {
                assert start + currentOffset <= end
                    : "requesting beyond end, start = " + start + " offset=" + currentOffset + " end=" + end;
            }
            try {
                currentStream = blobStoreServices.getInputStreamAtVersion(
                    currentStream != null ? currentStream.version : null,
                    start + currentOffset,
                    end
                );
                this.currentStreamFirstOffset = Math.addExact(start, currentOffset);
                return;
            } catch (NoSuchFileException | RequestedRangeNotSatisfiedException e) {
                throw e;
            } catch (RuntimeException | IOException e) {
                if (attempt == 1) {
                    blobStoreServices.onRetryStarted("open");
                }
                final long delayInMillis = maybeLogAndComputeRetryDelay("opening", e);
                delayBeforeRetry(delayInMillis);
            }
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        final int initialAttempt = attempt;
        while (true) {
            try {
                final int result = currentStream.inputStream.read();
                if (result != -1) {
                    currentOffset += 1;
                }
                maybeLogAndRecordMetricsForSuccess(initialAttempt, "read");
                return result;
            } catch (IOException e) {
                if (attempt == initialAttempt) {
                    blobStoreServices.onRetryStarted("read");
                }
                reopenStreamOrFail(e);
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        final int initialAttempt = attempt;
        while (true) {
            try {
                final int bytesRead = currentStream.inputStream.read(b, off, len);
                if (bytesRead != -1) {
                    currentOffset += bytesRead;
                }
                maybeLogAndRecordMetricsForSuccess(initialAttempt, "read");
                return bytesRead;
            } catch (IOException e) {
                if (attempt == initialAttempt) {
                    blobStoreServices.onRetryStarted("read");
                }
                reopenStreamOrFail(e);
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "using RetryingInputStream after close";
            throw new IllegalStateException("Stream is closed");
        }
    }

    private void reopenStreamOrFail(IOException e) throws IOException {
        final long meaningfulProgressSize = blobStoreServices.getMeaningfulProgressSize();
        if (currentStreamProgress() >= meaningfulProgressSize) {
            failuresAfterMeaningfulProgress += 1;
        }
        final long delayInMillis = maybeLogAndComputeRetryDelay("reading", e);
        IOUtils.closeWhileHandlingException(currentStream.inputStream);

        delayBeforeRetry(delayInMillis);
        openStreamWithRetry();
    }

    // The method throws if the operation should *not* be retried. Otherwise, it keeps a record for the attempt and associated failure
    // and compute the delay before retry.
    private long maybeLogAndComputeRetryDelay(String action, Exception e) throws IOException {
        if (shouldRetry(attempt) == false) {
            final var finalException = addSuppressedExceptions(e);
            logForFailure(action, finalException);
            switch (finalException) {
                case RuntimeException runtimeException:
                    throw runtimeException;
                case IOException ioException:
                    throw ioException;
                default:
                    throw new IOException("Error " + action + "blob", finalException);
            }
        }

        // Log at info level for the 1st retry and then exponentially less
        logForRetry(Integer.bitCount(attempt) == 1 ? Level.INFO : Level.DEBUG, action, e);
        if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
            failures.add(e);
        }
        final long delayInMillis = getRetryDelayInMillis();
        attempt += 1; // increment after computing delay because attempt affects the result
        return delayInMillis;
    }

    private void logForFailure(String action, Exception e) {
        logger.warn(
            () -> format(
                "failed %s [%s] at offset [%s] with purpose [%s]",
                action,
                blobStoreServices.getBlobDescription(),
                start + currentOffset,
                purpose.getKey()
            ),
            e
        );
    }

    private void logForRetry(Level level, String action, Exception e) {
        logger.log(
            level,
            () -> format(
                """
                    failed %s [%s] at offset [%s] with purpose [%s]; \
                    this was attempt [%s] to read this blob which yielded [%s] bytes; in total \
                    [%s] of the attempts to read this blob have made meaningful progress and do not count towards the maximum number of \
                    retries; the maximum number of read attempts which do not make meaningful progress is [%s]""",
                action,
                blobStoreServices.getBlobDescription(),
                start + currentOffset,
                purpose.getKey(),
                attempt,
                currentStreamProgress(),
                failuresAfterMeaningfulProgress,
                maxRetriesForNoMeaningfulProgress()
            ),
            e
        );
    }

    private void maybeLogAndRecordMetricsForSuccess(int initialAttempt, String action) {
        if (attempt > initialAttempt) {
            final int numberOfRetries = attempt - initialAttempt;
            logger.info(
                "successfully {} input stream for [{}] with purpose [{}] after [{}] retries",
                action,
                blobStoreServices.getBlobDescription(),
                purpose.getKey(),
                numberOfRetries
            );
            blobStoreServices.onRetrySucceeded(action, numberOfRetries);
        }
    }

    private long currentStreamProgress() {
        return Math.subtractExact(Math.addExact(start, currentOffset), currentStreamFirstOffset);
    }

    private boolean shouldRetry(int attempt) {
        if (purpose == OperationPurpose.REPOSITORY_ANALYSIS) {
            return false;
        }
        if (purpose == OperationPurpose.INDICES) {
            return true;
        }
        final int maxAttempts = blobStoreServices.getMaxRetries() + 1;
        return attempt < maxAttempts + failuresAfterMeaningfulProgress;
    }

    private int maxRetriesForNoMeaningfulProgress() {
        return purpose == OperationPurpose.INDICES ? Integer.MAX_VALUE : (blobStoreServices.getMaxRetries() + 1);
    }

    private void delayBeforeRetry(long delayInMillis) {
        try {
            assert shouldRetry(attempt - 1) : "should not have retried";
            Thread.sleep(delayInMillis);
        } catch (InterruptedException e) {
            logger.info("s3 input stream delay interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    // protected access for testing
    protected long getRetryDelayInMillis() {
        // Initial delay is 10 ms and cap max delay at 10 * 1024 millis, i.e. it retries every ~10 seconds at a minimum
        return 10L << (Math.min(attempt - 1, 10));
    }

    @Override
    public void close() throws IOException {
        try {
            currentStream.inputStream.close();
        } finally {
            closed = true;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        return currentStream.inputStream.skip(n);
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("RetryingInputStream does not support seeking");
    }

    private <T extends Exception> T addSuppressedExceptions(T e) {
        for (Exception failure : failures) {
            e.addSuppressed(failure);
        }
        return e;
    }

    /**
     * This implements all the behavior that is blob-store-specific
     *
     * @param <V> The type of the version used
     */
    protected interface BlobStoreServices<V> {

        /**
         * Get an input stream for the given version
         *
         * @param version The version to request, or null if the latest version should be used
         * @param start   The start of the range to read, inclusive
         * @param end     The end of the range to read, exclusive, or {@code Long.MAX_VALUE - 1} if the end of the blob should be used
         * @return An input stream for the given version
         * @throws IOException                         if a retryable error occurs while opening the stream
         * @throws NoSuchFileException                 if the blob does not exist, this is not retry-able
         * @throws RequestedRangeNotSatisfiedException if the requested range is not valid, this is not retry-able
         */
        InputStreamAtVersion<V> getInputStreamAtVersion(@Nullable V version, long start, long end) throws IOException;

        void onRetryStarted(String action);

        void onRetrySucceeded(String action, long numberOfRetries);

        long getMeaningfulProgressSize();

        int getMaxRetries();

        String getBlobDescription();
    }

    protected record InputStreamAtVersion<V>(InputStream inputStream, V version) {
        // Make the default constructor public
        public InputStreamAtVersion {}

    }
}

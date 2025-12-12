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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.blobstore.RetryingInputStream.StreamAction.OPEN;
import static org.elasticsearch.common.blobstore.RetryingInputStream.StreamAction.READ;
import static org.elasticsearch.core.Strings.format;

/**
 * An {@link InputStream} that resumes downloads from the point at which they failed when a retry-able error occurs.
 * <p>
 * This class implements some Elasticsearch-specific retry behavior, including:
 * <ul>
 *     <li>Retrying indefinitely for {@link OperationPurpose#INDICES} operations</li>
 *     <li>Not retrying at all for {@link OperationPurpose#REPOSITORY_ANALYSIS} operations</li>
 *     <li>Extending retries if "meaningful" progress was made on the last attempt</li>
 *     <li>More detailed logging about the status of operations being retried</li>
 * </ul>
 *
 * @param <V> The type used to represent the version of a blob
 */
public abstract class RetryingInputStream<V> extends InputStream {

    private static final Logger logger = LogManager.getLogger(RetryingInputStream.class);

    public static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    public enum StreamAction {
        OPEN("open", "opening"),
        READ("read", "reading");

        private final String pastTense;
        private final String presentTense;

        StreamAction(String pastTense, String presentTense) {
            this.pastTense = pastTense;
            this.presentTense = presentTense;
        }

        public String getPastTense() {
            return pastTense;
        }

        public String getPresentTense() {
            return presentTense;
        }
    }

    private final BlobStoreServices<V> blobStoreServices;
    private final OperationPurpose purpose;
    private final long start;
    private final long end;
    private final List<Exception> failures;

    protected SingleAttemptInputStream<V> currentStream;
    private long offset = 0;
    private int attempt = 1;
    private int failuresAfterMeaningfulProgress = 0;
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
        maybeLogAndRecordMetricsForSuccess(initialAttempt, OPEN);
    }

    private void openStreamWithRetry() throws IOException {
        while (true) {
            if (offset > 0 || start > 0 || end < Long.MAX_VALUE - 1) {
                assert start + offset <= end : "requesting beyond end, start = " + start + " offset=" + offset + " end=" + end;
            }
            // noinspection TryWithIdenticalCatches
            try {
                currentStream = blobStoreServices.getInputStream(
                    currentStream != null ? currentStream.getVersion() : null,
                    Math.addExact(start, offset),
                    end
                );
                return;
            } catch (NoSuchFileException | RequestedRangeNotSatisfiedException e) {
                throw addSuppressedExceptions(e);
            } catch (RuntimeException e) {
                retryOrAbortOnOpen(e);
            } catch (IOException e) {
                retryOrAbortOnOpen(e);
            }
        }
    }

    private <T extends Exception> void retryOrAbortOnOpen(T exception) throws T {
        if (attempt == 1) {
            blobStoreServices.onRetryStarted(StreamAction.OPEN);
        }
        final long delayInMillis = maybeLogAndComputeRetryDelay(StreamAction.OPEN, exception);
        delayBeforeRetry(StreamAction.OPEN, exception, delayInMillis);
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        final int initialAttempt = attempt;
        while (true) {
            // noinspection TryWithIdenticalCatches
            try {
                final int result = currentStream.read();
                if (result != -1) {
                    offset += 1;
                }
                maybeLogAndRecordMetricsForSuccess(initialAttempt, READ);
                return result;
            } catch (IOException e) {
                retryOrAbortOnRead(initialAttempt, e);
            } catch (RuntimeException e) {
                retryOrAbortOnRead(initialAttempt, e);
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        final int initialAttempt = attempt;
        while (true) {
            // noinspection TryWithIdenticalCatches
            try {
                final int bytesRead = currentStream.read(b, off, len);
                if (bytesRead != -1) {
                    offset += bytesRead;
                }
                maybeLogAndRecordMetricsForSuccess(initialAttempt, READ);
                return bytesRead;
            } catch (IOException e) {
                retryOrAbortOnRead(initialAttempt, e);
            } catch (RuntimeException e) {
                retryOrAbortOnRead(initialAttempt, e);
            }
        }
    }

    private <T extends Exception> void retryOrAbortOnRead(int initialAttempt, T exception) throws T, IOException {
        if (attempt == initialAttempt) {
            blobStoreServices.onRetryStarted(READ);
        }
        reopenStreamOrFail(exception);
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "using RetryingInputStream after close";
            throw new IllegalStateException("Stream is closed");
        }
    }

    private <T extends Exception> void reopenStreamOrFail(T e) throws T, IOException {
        final long meaningfulProgressSize = blobStoreServices.getMeaningfulProgressSize();
        if (currentStreamProgress() >= meaningfulProgressSize) {
            failuresAfterMeaningfulProgress += 1;
        }
        final long delayInMillis = maybeLogAndComputeRetryDelay(READ, e);
        IOUtils.closeWhileHandlingException(currentStream);

        delayBeforeRetry(READ, e, delayInMillis);
        openStreamWithRetry();
    }

    // The method throws if the operation should *not* be retried. Otherwise, it keeps a record for the attempt and associated failure
    // and compute the delay before retry.
    private <T extends Exception> long maybeLogAndComputeRetryDelay(StreamAction action, T e) throws T {
        if (shouldRetry(action, e, attempt) == false) {
            final var finalException = addSuppressedExceptions(e);
            logForFailure(action, finalException);
            throw finalException;
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

    private void logForFailure(StreamAction action, Exception e) {
        logger.warn(
            () -> format(
                "failed %s [%s] at offset [%s] with purpose [%s]",
                action.getPresentTense(),
                blobStoreServices.getBlobDescription(),
                start + offset,
                purpose.getKey()
            ),
            e
        );
    }

    private void logForRetry(Level level, StreamAction action, Exception e) {
        logger.log(
            level,
            () -> format(
                """
                    failed %s [%s] at offset [%s] with purpose [%s]; \
                    this was attempt [%s] to read this blob which yielded [%s] bytes; in total \
                    [%s] of the attempts to read this blob have made meaningful progress and do not count towards the maximum number of \
                    retries; the maximum number of read attempts which do not make meaningful progress is [%s]""",
                action.getPresentTense(),
                blobStoreServices.getBlobDescription(),
                start + offset,
                purpose.getKey(),
                attempt,
                currentStreamProgress(),
                failuresAfterMeaningfulProgress,
                maxRetriesForNoMeaningfulProgress()
            ),
            e
        );
    }

    private void maybeLogAndRecordMetricsForSuccess(int initialAttempt, StreamAction action) {
        if (attempt > initialAttempt) {
            final int numberOfRetries = attempt - initialAttempt;
            logger.info(
                "successfully {} input stream for [{}] with purpose [{}] after [{}] retries",
                action.getPastTense(),
                blobStoreServices.getBlobDescription(),
                purpose.getKey(),
                numberOfRetries
            );
            blobStoreServices.onRetrySucceeded(action, numberOfRetries);
        }
    }

    private long currentStreamProgress() {
        if (currentStream == null) {
            return 0L;
        }
        return Math.subtractExact(Math.addExact(start, offset), currentStream.getFirstOffset());
    }

    private boolean shouldRetry(StreamAction action, Exception exception, int attempt) {
        if (blobStoreServices.isRetryableException(action, exception) == false) {
            return false;
        }
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

    private void delayBeforeRetry(StreamAction action, Exception exception, long delayInMillis) {
        try {
            assert shouldRetry(action, exception, attempt - 1) : "should not have retried";
            Thread.sleep(delayInMillis);
        } catch (InterruptedException e) {
            logger.info("retrying input stream delay interrupted", e);
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
            currentStream.close();
        } finally {
            closed = true;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        final int initialAttempt = attempt;
        while (true) {
            // noinspection TryWithIdenticalCatches
            try {
                return currentStream.skip(n);
            } catch (IOException e) {
                retryOrAbortOnRead(initialAttempt, e);
            } catch (RuntimeException e) {
                retryOrAbortOnRead(initialAttempt, e);
            }
        }
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
        SingleAttemptInputStream<V> getInputStream(@Nullable V version, long start, long end) throws IOException;

        void onRetryStarted(StreamAction action);

        void onRetrySucceeded(StreamAction action, long numberOfRetries);

        long getMeaningfulProgressSize();

        int getMaxRetries();

        String getBlobDescription();

        boolean isRetryableException(StreamAction action, Exception e);
    }

    /**
     * Represents an {@link InputStream} for a single attempt to read a blob. Each retry
     * will attempt to create a new one of these. If reading from it fails, it should not retry.
     */
    protected static final class SingleAttemptInputStream<V> extends FilterInputStream {

        private final long firstOffset;
        private final V version;

        public SingleAttemptInputStream(InputStream in, long firstOffset, V version) {
            super(in);
            this.firstOffset = firstOffset;
            this.version = version;
        }

        /**
         * @return the offset of the first byte returned by this input stream
         */
        public long getFirstOffset() {
            return firstOffset;
        }

        /**
         * Get the version of this input stream
         */
        public V getVersion() {
            return version;
        }

        /**
         * Unwrap the underlying input stream
         *
         * @param clazz The expected class of the underlying input stream
         * @return The underlying input stream
         * @param <T> The type of the underlying input stream
         */
        public <T> T unwrap(Class<T> clazz) {
            return clazz.cast(in);
        }
    }
}

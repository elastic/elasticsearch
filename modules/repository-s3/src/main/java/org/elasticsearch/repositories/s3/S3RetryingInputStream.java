/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.repositories.s3.S3BlobStore.Operation;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.repositories.s3.S3BlobStore.configureRequestForMetrics;

/**
 * Wrapper around an S3 object that will retry the {@link GetObjectRequest} if the download fails part-way through, resuming from where
 * the failure occurred. This should be handled by the SDK but it isn't today. This should be revisited in the future (e.g. before removing
 * the {@link Version#V_7_0_0} version constant) and removed when the SDK handles retries itself.
 *
 * See https://github.com/aws/aws-sdk-java/issues/856 for the related SDK issue
 */
class S3RetryingInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(S3RetryingInputStream.class);

    static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    private final OperationPurpose purpose;
    private final S3BlobStore blobStore;
    private final String blobKey;
    private final long start;
    private final long end;
    private final List<Exception> failures;

    private S3ObjectInputStream currentStream;
    private long currentStreamFirstOffset;
    private long currentStreamLastOffset;
    private int attempt = 1;
    private int failuresAfterMeaningfulProgress = 0;
    private long currentOffset;
    private boolean closed;
    private boolean eof;

    S3RetryingInputStream(OperationPurpose purpose, S3BlobStore blobStore, String blobKey) throws IOException {
        this(purpose, blobStore, blobKey, 0, Long.MAX_VALUE - 1);
    }

    // both start and end are inclusive bounds, following the definition in GetObjectRequest.setRange
    S3RetryingInputStream(OperationPurpose purpose, S3BlobStore blobStore, String blobKey, long start, long end) throws IOException {
        if (start < 0L) {
            throw new IllegalArgumentException("start must be non-negative");
        }
        if (end < start || end == Long.MAX_VALUE) {
            throw new IllegalArgumentException("end must be >= start and not Long.MAX_VALUE");
        }
        this.purpose = purpose;
        this.blobStore = blobStore;
        this.blobKey = blobKey;
        this.failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
        this.start = start;
        this.end = end;
        final int initialAttempt = attempt;
        openStreamWithRetry();
        maybeLogAndRecordMetricsForSuccess(initialAttempt, "open");
    }

    private void openStreamWithRetry() throws IOException {
        while (true) {
            try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                final GetObjectRequest getObjectRequest = new GetObjectRequest(blobStore.bucket(), blobKey);
                configureRequestForMetrics(getObjectRequest, blobStore, Operation.GET_OBJECT, purpose);
                if (currentOffset > 0 || start > 0 || end < Long.MAX_VALUE - 1) {
                    assert start + currentOffset <= end
                        : "requesting beyond end, start = " + start + " offset=" + currentOffset + " end=" + end;
                    getObjectRequest.setRange(Math.addExact(start, currentOffset), end);
                }
                this.currentStreamFirstOffset = Math.addExact(start, currentOffset);
                final S3Object s3Object = SocketAccess.doPrivileged(() -> clientReference.client().getObject(getObjectRequest));
                this.currentStreamLastOffset = Math.addExact(currentStreamFirstOffset, getStreamLength(s3Object));
                this.currentStream = s3Object.getObjectContent();
                return;
            } catch (AmazonClientException e) {
                if (e instanceof AmazonS3Exception amazonS3Exception) {
                    if (amazonS3Exception.getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
                        throw addSuppressedExceptions(
                            new NoSuchFileException("Blob object [" + blobKey + "] not found: " + amazonS3Exception.getMessage())
                        );
                    }
                    if (amazonS3Exception.getStatusCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()) {
                        throw addSuppressedExceptions(
                            new RequestedRangeNotSatisfiedException(
                                blobKey,
                                currentStreamFirstOffset,
                                (end < Long.MAX_VALUE - 1) ? end - currentStreamFirstOffset + 1 : end,
                                amazonS3Exception
                            )
                        );
                    }
                }

                if (attempt == 1) {
                    blobStore.getS3RepositoriesMetrics().retryStartedCounter().incrementBy(1, metricAttributes("open"));
                }
                final long delayInMillis = maybeLogAndComputeRetryDelay("opening", e);
                delayBeforeRetry(delayInMillis);
            }
        }
    }

    private long getStreamLength(final S3Object object) {
        final ObjectMetadata metadata = object.getObjectMetadata();
        try {
            // Returns the content range of the object if response contains the Content-Range header.
            final Long[] range = metadata.getContentRange();
            if (range != null) {
                assert range[1] >= range[0] : range[1] + " vs " + range[0];
                assert range[0] == start + currentOffset
                    : "Content-Range start value [" + range[0] + "] exceeds start [" + start + "] + current offset [" + currentOffset + ']';
                assert range[1] <= end : "Content-Range end value [" + range[1] + "] exceeds end [" + end + ']';
                return range[1] - range[0] + 1L;
            }
            return metadata.getContentLength();
        } catch (Exception e) {
            assert false : e;
            return Long.MAX_VALUE - 1L; // assume a large stream so that the underlying stream is aborted on closing, unless eof is reached
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        final int initialAttempt = attempt;
        while (true) {
            try {
                final int result = currentStream.read();
                if (result == -1) {
                    eof = true;
                } else {
                    currentOffset += 1;
                }
                maybeLogAndRecordMetricsForSuccess(initialAttempt, "read");
                return result;
            } catch (IOException e) {
                if (attempt == initialAttempt) {
                    blobStore.getS3RepositoriesMetrics().retryStartedCounter().incrementBy(1, metricAttributes("read"));
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
                final int bytesRead = currentStream.read(b, off, len);
                if (bytesRead == -1) {
                    eof = true;
                } else {
                    currentOffset += bytesRead;
                }
                maybeLogAndRecordMetricsForSuccess(initialAttempt, "read");
                return bytesRead;
            } catch (IOException e) {
                if (attempt == initialAttempt) {
                    blobStore.getS3RepositoriesMetrics().retryStartedCounter().incrementBy(1, metricAttributes("read"));
                }
                reopenStreamOrFail(e);
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "using S3RetryingInputStream after close";
            throw new IllegalStateException("using S3RetryingInputStream after close");
        }
    }

    private void reopenStreamOrFail(IOException e) throws IOException {
        final long meaningfulProgressSize = Math.max(1L, blobStore.bufferSizeInBytes() / 100L);
        if (currentStreamProgress() >= meaningfulProgressSize) {
            failuresAfterMeaningfulProgress += 1;
        }
        final long delayInMillis = maybeLogAndComputeRetryDelay("reading", e);
        maybeAbort(currentStream);
        IOUtils.closeWhileHandlingException(currentStream);

        delayBeforeRetry(delayInMillis);
        openStreamWithRetry();
    }

    // The method throws if the operation should *not* be retried. Otherwise, it keeps a record for the attempt and associated failure
    // and compute the delay before retry.
    private <T extends Exception> long maybeLogAndComputeRetryDelay(String action, T e) throws T {
        if (shouldRetry(attempt) == false) {
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

    private void logForFailure(String action, Exception e) {
        logger.warn(
            () -> format(
                "failed %s [%s/%s] at offset [%s] with purpose [%s]",
                action,
                blobStore.bucket(),
                blobKey,
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
                    failed %s [%s/%s] at offset [%s] with purpose [%s]; \
                    this was attempt [%s] to read this blob which yielded [%s] bytes; in total \
                    [%s] of the attempts to read this blob have made meaningful progress and do not count towards the maximum number of \
                    retries; the maximum number of read attempts which do not make meaningful progress is [%s]""",
                action,
                blobStore.bucket(),
                blobKey,
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
                "successfully {} input stream for [{}/{}] with purpose [{}] after [{}] retries",
                action,
                blobStore.bucket(),
                blobKey,
                purpose.getKey(),
                numberOfRetries
            );
            final Map<String, Object> attributes = metricAttributes(action);
            blobStore.getS3RepositoriesMetrics().retryCompletedCounter().incrementBy(1, attributes);
            blobStore.getS3RepositoriesMetrics().retryHistogram().record(numberOfRetries, attributes);
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
        final int maxAttempts = blobStore.getMaxRetries() + 1;
        return attempt < maxAttempts + failuresAfterMeaningfulProgress;
    }

    private int maxRetriesForNoMeaningfulProgress() {
        return purpose == OperationPurpose.INDICES ? Integer.MAX_VALUE : (blobStore.getMaxRetries() + 1);
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

    private Map<String, Object> metricAttributes(String action) {
        return Map.of(
            "repo_type",
            S3Repository.TYPE,
            "repo_name",
            blobStore.getRepositoryMetadata().name(),
            "operation",
            Operation.GET_OBJECT.getKey(),
            "purpose",
            purpose.getKey(),
            "action",
            action
        );
    }

    @Override
    public void close() throws IOException {
        maybeAbort(currentStream);
        try {
            currentStream.close();
        } finally {
            closed = true;
        }
    }

    /**
     * Abort the {@link S3ObjectInputStream} if it wasn't read completely at the time this method is called,
     * suppressing all thrown exceptions.
     */
    private void maybeAbort(S3ObjectInputStream stream) {
        if (isEof()) {
            return;
        }
        try {
            if (start + currentOffset < currentStreamLastOffset) {
                stream.abort();
            }
        } catch (Exception e) {
            logger.warn("Failed to abort stream before closing", e);
        }
    }

    @Override
    public long skip(long n) throws IOException {
        // This could be optimized on a failure by re-opening stream directly to the preferred location. However, it is rarely called,
        // so for now we will rely on the default implementation which just discards bytes by reading.
        return super.skip(n);
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }

    private <T extends Exception> T addSuppressedExceptions(T e) {
        for (Exception failure : failures) {
            e.addSuppressed(failure);
        }
        return e;
    }

    // package-private for tests
    boolean isEof() {
        return eof || start + currentOffset == currentStreamLastOffset;
    }

    // package-private for tests
    boolean isAborted() {
        if (currentStream == null || currentStream.getHttpRequest() == null) {
            return false;
        }
        return currentStream.getHttpRequest().isAborted();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.RetryingInputStream;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.repositories.s3.S3BlobStore.Operation;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Map;

import static org.elasticsearch.repositories.s3.S3BlobStore.configureRequestForMetrics;

/**
 * Wrapper around an S3 object that will retry the {@link GetObjectRequest} if the download fails part-way through, resuming from where
 * the failure occurred. This should be handled by the SDK but it isn't today. This should be revisited in the future (e.g. before removing
 * the {@link Version#V_7_0_0} version constant) and removed when the SDK handles retries itself.
 *
 * See https://github.com/aws/aws-sdk-java/issues/856 for the related SDK issue
 */
class S3RetryingInputStream extends RetryingInputStream<String> {

    private static final Logger logger = LogManager.getLogger(S3RetryingInputStream.class);

    S3RetryingInputStream(OperationPurpose purpose, S3BlobStore blobStore, String blobKey) throws IOException {
        this(purpose, blobStore, blobKey, 0, Long.MAX_VALUE - 1);
    }

    // both start and end are inclusive bounds, following the definition in GetObjectRequest.setRange
    S3RetryingInputStream(OperationPurpose purpose, S3BlobStore blobStore, String blobKey, long start, long end) throws IOException {
        super(new S3BlobStoreServices(blobStore, blobKey, purpose), purpose, start, end);
    }

    private record S3BlobStoreServices(S3BlobStore blobStore, String blobKey, OperationPurpose purpose)
        implements
            BlobStoreServices<String> {

        @Override
        public SingleAttemptInputStream<String> getInputStream(@Nullable String version, long start, long end) throws IOException {
            try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                final var getObjectRequestBuilder = GetObjectRequest.builder().bucket(blobStore.bucket()).key(blobKey);
                configureRequestForMetrics(getObjectRequestBuilder, blobStore, Operation.GET_OBJECT, purpose);
                if (start > 0 || end < Long.MAX_VALUE - 1) {
                    assert start <= end : "requesting beyond end, start = " + start + " end=" + end;
                    getObjectRequestBuilder.range("bytes=" + start + "-" + end);
                }
                if (version != null) {
                    // This is a second or subsequent request, ensure the object hasn't changed since the first request
                    getObjectRequestBuilder.ifMatch(version);
                }
                final var getObjectRequest = getObjectRequestBuilder.build();
                final var getObjectResponse = clientReference.client().getObject(getObjectRequest);
                return new SingleAttemptInputStream<>(
                    new S3ResponseWrapperInputStream(getObjectResponse, start, end),
                    start,
                    getObjectResponse.response().eTag()
                );
            } catch (SdkException e) {
                if (e instanceof SdkServiceException sdkServiceException) {
                    if (sdkServiceException.statusCode() == RestStatus.NOT_FOUND.getStatus()) {
                        throw new NoSuchFileException("Blob object [" + blobKey + "] not found: " + sdkServiceException.getMessage());
                    }
                    if (sdkServiceException.statusCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()) {
                        throw new RequestedRangeNotSatisfiedException(
                            blobKey,
                            start,
                            (end < Long.MAX_VALUE - 1) ? end - start + 1 : end,
                            sdkServiceException
                        );
                    }
                    if (version != null && sdkServiceException.statusCode() == RestStatus.PRECONDITION_FAILED.getStatus()) {
                        throw new NoSuchFileException(
                            "Blob object ["
                                + blobKey
                                + "] with ETag ["
                                + version
                                + "] unavailable on resume: "
                                + sdkServiceException.getMessage()
                        );
                    }
                }
                throw e;
            }
        }

        @Override
        public void onRetryStarted(StreamAction action) {
            blobStore.getS3RepositoriesMetrics().retryStartedCounter().incrementBy(1, metricAttributes(action));
        }

        @Override
        public void onRetrySucceeded(StreamAction action, long numberOfRetries) {
            final Map<String, Object> attributes = metricAttributes(action);
            blobStore.getS3RepositoriesMetrics().retryCompletedCounter().incrementBy(1, attributes);
            blobStore.getS3RepositoriesMetrics().retryHistogram().record(numberOfRetries, attributes);
        }

        @Override
        public long getMeaningfulProgressSize() {
            return Math.max(1L, blobStore.bufferSizeInBytes() / 100L);
        }

        @Override
        public int getMaxRetries() {
            return blobStore.getMaxRetries();
        }

        @Override
        public String getBlobDescription() {
            return blobStore.bucket() + "/" + blobKey;
        }

        @Override
        public boolean isRetryableException(StreamAction action, Exception e) {
            return switch (action) {
                case OPEN -> e instanceof SdkException;
                case READ -> e instanceof IOException;
            };
        }

        private Map<String, Object> metricAttributes(StreamAction action) {
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
                action.getPastTense()
            );
        }
    }

    private static long getStreamLength(final GetObjectResponse getObjectResponse, long expectedStart, long expectedEnd) {
        try {
            return tryGetStreamLength(getObjectResponse, expectedStart, expectedEnd);
        } catch (Exception e) {
            assert false : e;
            return Long.MAX_VALUE - 1L; // assume a large stream so that the underlying stream is aborted on closing, unless eof is reached
        }
    }

    private static long tryGetStreamLength(GetObjectResponse getObjectResponse, long expectedStart, long expectedEnd) {
        // Returns the content range of the object if response contains the Content-Range header.
        final var rangeString = getObjectResponse.contentRange();
        if (rangeString != null) {
            if (rangeString.startsWith("bytes ") == false) {
                throw new IllegalArgumentException(
                    "unexpected Content-range header [" + rangeString + "], should have started with [bytes ]"
                );
            }
            final var hyphenPos = rangeString.indexOf('-');
            if (hyphenPos == -1) {
                throw new IllegalArgumentException("could not parse Content-range header [" + rangeString + "], missing hyphen");
            }
            final var slashPos = rangeString.indexOf('/');
            if (slashPos == -1) {
                throw new IllegalArgumentException("could not parse Content-range header [" + rangeString + "], missing slash");
            }

            final var rangeStart = Long.parseLong(rangeString, "bytes ".length(), hyphenPos, 10);
            final var rangeEnd = Long.parseLong(rangeString, hyphenPos + 1, slashPos, 10);
            if (rangeEnd < rangeStart) {
                throw new IllegalArgumentException("invalid Content-range header [" + rangeString + "]");
            }
            if (rangeStart != expectedStart) {
                throw new IllegalArgumentException(
                    "unexpected Content-range header [" + rangeString + "], should have started at " + expectedStart
                );
            }
            if (rangeEnd > expectedEnd) {
                throw new IllegalArgumentException(
                    "unexpected Content-range header [" + rangeString + "], should have ended no later than " + expectedEnd
                );
            }
            return rangeEnd - rangeStart + 1L;
        }
        return getObjectResponse.contentLength();
    }

    /**
     * A wrapper around the {@link ResponseInputStream} that aborts the stream if it wasn't fully read before closing.
     */
    private static class S3ResponseWrapperInputStream extends InputStream {

        private final ResponseInputStream<GetObjectResponse> responseStream;
        private final long start;
        private final long end;
        private final long lastOffset;
        private long offset = 0;
        private boolean closed;
        private boolean eof;
        private boolean aborted;

        private S3ResponseWrapperInputStream(ResponseInputStream<GetObjectResponse> responseStream, long start, long end) {
            this.responseStream = responseStream;
            this.start = start;
            this.end = end;
            lastOffset = getStreamLength(responseStream.response(), start, end);
        }

        @Override
        public int read() throws IOException {
            ensureOpen();
            int result = responseStream.read();
            if (result == -1) {
                eof = true;
            } else {
                offset++;
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ensureOpen();
            final int bytesRead = responseStream.read(b, off, len);
            if (bytesRead == -1) {
                eof = true;
            } else {
                offset += bytesRead;
            }
            return bytesRead;
        }

        private void ensureOpen() {
            if (closed) {
                final var message = "using " + getClass().getSimpleName() + " after close";
                assert false : message;
                throw new IllegalStateException(message);
            }
        }

        @Override
        public void close() throws IOException {
            maybeAbort(responseStream);
            try {
                responseStream.close();
            } finally {
                closed = true;
            }
        }

        /**
         * Abort the {@link ResponseInputStream} if it wasn't read completely at the time this method is called,
         * suppressing all thrown exceptions.
         */
        private void maybeAbort(ResponseInputStream<?> stream) {
            if (isEof()) {
                return;
            }
            try {
                if (offset < lastOffset) {
                    stream.abort();
                    aborted = true;
                }
            } catch (Exception e) {
                logger.warn("Failed to abort stream before closing", e);
            }
        }

        @Override
        public long skip(long n) {
            throw new UnsupportedOperationException("Skip should be implemented by RetryingInputStream#skip");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("S3InputStream does not support seeking");
        }

        // exposed for testing
        private boolean isEof() {
            return eof || offset == lastOffset;
        }

        // exposed for testing
        private boolean isAborted() {
            // just expose whether abort() was called, we cannot tell if the stream is really aborted
            return aborted;
        }

        // exposed for testing
        private long tryGetStreamLength(GetObjectResponse response) {
            return S3RetryingInputStream.tryGetStreamLength(response, start, end);
        }
    }

    // exposed for testing
    boolean isEof() {
        return currentStream.unwrap(S3ResponseWrapperInputStream.class).isEof();
    }

    // exposed for testing
    boolean isAborted() {
        return currentStream.unwrap(S3ResponseWrapperInputStream.class).isAborted();
    }

    // exposed for testing
    long tryGetStreamLength(GetObjectResponse getObjectResponse) {
        return currentStream.unwrap(S3ResponseWrapperInputStream.class).tryGetStreamLength(getObjectResponse);
    }
}

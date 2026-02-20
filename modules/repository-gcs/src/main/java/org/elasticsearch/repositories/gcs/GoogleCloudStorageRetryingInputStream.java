/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpResponse;
import com.google.cloud.BaseService;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.RetryingInputStream;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.rest.RestStatus;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;

/**
 * Wrapper around reads from GCS that will retry blob downloads that fail part-way through, resuming from where the failure occurred.
 * <p>
 * We wrap the default GCS SDK retry logic with the retry logic from {@link RetryingInputStream}, which is slightly more sophisticated
 * and tailored to our needs.
 */
class GoogleCloudStorageRetryingInputStream extends RetryingInputStream<Long> {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageRetryingInputStream.class);

    GoogleCloudStorageRetryingInputStream(GoogleCloudStorageBlobStore blobStore, OperationPurpose purpose, BlobId blobId)
        throws IOException {
        this(blobStore, purpose, blobId, 0, Long.MAX_VALUE - 1);
    }

    GoogleCloudStorageRetryingInputStream(
        GoogleCloudStorageBlobStore blobStore,
        OperationPurpose purpose,
        BlobId blobId,
        long start,
        long end
    ) throws IOException {
        super(new GoogleCloudStorageBlobStoreServices(blobStore, purpose, blobId), purpose, start, end);
    }

    private static class GoogleCloudStorageBlobStoreServices implements BlobStoreServices<Long> {

        private final GoogleCloudStorageBlobStore blobStore;
        private final OperationPurpose purpose;
        private final BlobId blobId;

        private GoogleCloudStorageBlobStoreServices(GoogleCloudStorageBlobStore blobStore, OperationPurpose purpose, BlobId blobId) {
            this.blobStore = blobStore;
            this.purpose = purpose;
            this.blobId = blobId;
        }

        @Override
        public SingleAttemptInputStream<Long> getInputStream(@Nullable Long lastGeneration, long start, long end) throws IOException {
            final MeteredStorage client = blobStore.client();
            try {
                try {
                    final var meteredGet = client.meteredObjectsGet(purpose, blobId.getBucket(), blobId.getName());
                    meteredGet.setReturnRawInputStream(true);
                    if (lastGeneration != null) {
                        meteredGet.setGeneration(lastGeneration);
                    }

                    if (start > 0 || end < Long.MAX_VALUE - 1) {
                        if (meteredGet.getRequestHeaders() != null) {
                            meteredGet.getRequestHeaders().setRange("bytes=" + start + "-" + end);
                        }
                    }
                    final HttpResponse resp = meteredGet.executeMedia();
                    // Store the generation of the first response we received, so we can detect
                    // if the file has changed if we need to resume
                    if (lastGeneration == null) {
                        lastGeneration = parseGenerationHeader(resp);
                    }

                    final Long contentLength = resp.getHeaders().getContentLength();
                    InputStream content = resp.getContent();
                    if (contentLength != null) {
                        content = new ContentLengthValidatingInputStream(content, contentLength);
                    }
                    return new SingleAttemptInputStream<>(content, start, lastGeneration);
                } catch (IOException e) {
                    throw StorageException.translate(e);
                }
            } catch (StorageException storageException) {
                if (storageException.getCode() == RestStatus.NOT_FOUND.getStatus()) {
                    if (lastGeneration != null) {
                        throw new NoSuchFileException(
                            "Blob object ["
                                + blobId.getName()
                                + "] generation ["
                                + lastGeneration
                                + "] unavailable on resume (contents changed, or object deleted): "
                                + storageException.getMessage()
                        );
                    } else {
                        throw new NoSuchFileException("Blob object [" + blobId.getName() + "] not found: " + storageException.getMessage());
                    }
                }
                if (storageException.getCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()) {
                    throw new RequestedRangeNotSatisfiedException(
                        blobId.getName(),
                        start,
                        (end < Long.MAX_VALUE - 1) ? end - start + 1 : end,
                        storageException
                    );
                }
                throw storageException;
            }
        }

        @Override
        public void onRetryStarted(StreamAction action) {
            // No retry metrics for GCS
        }

        @Override
        public void onRetrySucceeded(StreamAction action, long numberOfRetries) {
            // No retry metrics for GCS
        }

        @Override
        public long getMeaningfulProgressSize() {
            return Math.max(1L, GoogleCloudStorageBlobStore.SDK_DEFAULT_CHUNK_SIZE / 100L);
        }

        @Override
        public int getMaxRetries() {
            return blobStore.getMaxRetries();
        }

        @Override
        public String getBlobDescription() {
            return blobId.toString();
        }

        @Override
        public boolean isRetryableException(StreamAction action, Exception e) {
            if (e instanceof AlreadyClosedException) {
                return false;
            }
            return switch (action) {
                case OPEN -> BaseService.EXCEPTION_HANDLER.shouldRetry(e, null);
                case READ -> e instanceof StorageException;
            };
        }
    }

    private static Long parseGenerationHeader(HttpResponse response) {
        final String generationHeader = response.getHeaders().getFirstHeaderStringValue("x-goog-generation");
        if (generationHeader != null) {
            try {
                return Long.parseLong(generationHeader);
            } catch (NumberFormatException e) {
                final String message = "Unexpected value for x-goog-generation header: " + generationHeader;
                logger.warn(message);
                assert false : message;
            }
        } else {
            String message = "Missing x-goog-generation header";
            logger.warn(message);
            assert false : message;
        }
        return null;
    }

    // Google's SDK ignores the Content-Length header when no bytes are sent, see NetHttpResponse.SizeValidatingInputStream
    // We have to implement our own validation logic here
    static final class ContentLengthValidatingInputStream extends FilterInputStream {
        private final long contentLength;

        private long read = 0L;

        ContentLengthValidatingInputStream(InputStream in, long contentLength) {
            super(in);
            this.contentLength = contentLength;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            try {
                final int n = in.read(b, off, len);
                if (n == -1) {
                    checkContentLengthOnEOF();
                } else {
                    read += n;
                }
                return n;
            } catch (IOException e) {
                throw StorageException.translate(e);
            }
        }

        @Override
        public int read() {
            try {
                final int n = in.read();
                if (n == -1) {
                    checkContentLengthOnEOF();
                } else {
                    read++;
                }
                return n;
            } catch (IOException e) {
                throw StorageException.translate(e);
            }
        }

        @Override
        public long skip(long len) {
            throw new UnsupportedOperationException("Skip should be implemented by RetryingInputStream#skip");
        }

        private void checkContentLengthOnEOF() throws IOException {
            if (read < contentLength) {
                throw new IOException("Connection closed prematurely: read = " + read + ", Content-Length = " + contentLength);
            }
        }
    }

    /**
     * Close the current stream, used to test resume
     */
    // @VisibleForTesting
    void closeCurrentStream() throws IOException {
        currentStream.close();
    }
}

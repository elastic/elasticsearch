/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import com.azure.core.exception.HttpResponseException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.RetryingInputStream;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

public class AzureRetryingInputStream extends RetryingInputStream<String> {

    protected AzureRetryingInputStream(AzureBlobStore azureBlobStore, OperationPurpose purpose, String blob, long position, Long length)
        throws IOException {
        super(
            new AzureBlobStoreServices(azureBlobStore, purpose, blob),
            purpose,
            position,
            length == null ? Long.MAX_VALUE - 1 : position + length
        );
    }

    private record AzureBlobStoreServices(AzureBlobStore blobStore, OperationPurpose purpose, String blob)
        implements
            RetryingInputStream.BlobStoreServices<String> {

        @Override
        public SingleAttemptInputStream<String> getInputStream(@Nullable String version, long start, long end) throws IOException {
            try {
                final Long length = end < Long.MAX_VALUE - 1 ? end - start : null;
                final AzureBlobStore.AzureInputStream inputStream = blobStore.getInputStream(purpose, blob, start, length, version);
                return new SingleAttemptInputStream<>(inputStream, start, inputStream.getETag());
            } catch (Exception e) {
                if (ExceptionsHelper.unwrap(e, HttpResponseException.class) instanceof HttpResponseException httpResponseException) {
                    final var httpStatusCode = httpResponseException.getResponse().getStatusCode();
                    if (httpStatusCode == RestStatus.NOT_FOUND.getStatus()) {
                        throw new NoSuchFileException("Blob [" + blob + "] not found");
                    }
                    if (httpStatusCode == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()) {
                        throw new RequestedRangeNotSatisfiedException(blob, start, end == Long.MAX_VALUE - 1 ? -1 : end - start, e);
                    }
                }
                switch (e) {
                    case RuntimeException runtimeException -> throw runtimeException;
                    case IOException ioException -> throw ioException;
                    default -> throw new IOException("Unable to get input stream for blob [" + blob + "]", e);
                }
            }
        }

        @Override
        public void onRetryStarted(StreamAction action) {
            // No metrics for Azure
        }

        @Override
        public void onRetrySucceeded(StreamAction action, long numberOfRetries) {
            // No metrics for Azure
        }

        @Override
        public long getMeaningfulProgressSize() {
            return Math.max(1L, blobStore.getReadChunkSize() / 100L);
        }

        @Override
        public int getMaxRetries() {
            return blobStore.getMaxReadRetries();
        }

        @Override
        public String getBlobDescription() {
            return blob;
        }

        @Override
        public boolean isRetryableException(StreamAction action, Exception e) {
            // TODO: work out what is and is not retry-able
            return true;
        }
    }
}

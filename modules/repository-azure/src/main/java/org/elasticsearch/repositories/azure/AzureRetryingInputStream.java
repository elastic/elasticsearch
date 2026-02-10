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
import com.azure.core.http.policy.RetryStrategy;

import org.apache.lucene.store.AlreadyClosedException;
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
            } catch (RuntimeException e) {
                if (ExceptionsHelper.unwrap(e, HttpResponseException.class) instanceof HttpResponseException httpResponseException) {
                    final var httpStatusCode = httpResponseException.getResponse().getStatusCode();
                    if (httpStatusCode == RestStatus.NOT_FOUND.getStatus()) {
                        throw new NoSuchFileException("blob object [" + blob + "] not found");
                    }
                    if (httpStatusCode == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()) {
                        throw new RequestedRangeNotSatisfiedException(blob, start, end == Long.MAX_VALUE - 1 ? -1 : end - start, e);
                    }
                }
                throw e;
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

        /**
         * The range of errors that are retried in Azure is quite liberal by default. This
         * is overridden in {@link com.azure.core.http.policy.ExponentialBackoff}, but in
         * our configuration it ends up using the default.
         *
         * @see RetryStrategy#shouldRetryException(Throwable)
         */
        @Override
        public boolean isRetryableException(StreamAction action, Exception e) {
            return e instanceof AlreadyClosedException == false;
        }
    }
}

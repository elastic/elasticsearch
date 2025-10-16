/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.RetryingInputStream;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

public class AzureRetryingInputStream extends RetryingInputStream<String> {

    protected AzureRetryingInputStream(AzureBlobStore azureBlobStore, OperationPurpose purpose, String blob) throws IOException {
        this(azureBlobStore, purpose, blob, 0L, null);
    }

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
        public InputStreamAtVersion<String> getInputStreamAtVersion(@Nullable String version, long start, long end) throws IOException {
            Long length = end < Long.MAX_VALUE - 1 ? end - start : null;
            AzureBlobStore.AzureInputStream inputStream = blobStore.getInputStream(purpose, blob, start, length, version);
            return new InputStreamAtVersion<>(inputStream, inputStream.getETag());
        }

        @Override
        public void onRetryStarted(String action) {
            // No metrics for Azure
        }

        @Override
        public void onRetrySucceeded(String action, long numberOfRetries) {
            // No metrics for Azure
        }

        @Override
        public long getMeaningfulProgressSize() {
            // Any progress is meaningful for Azure
            return 1;
        }

        @Override
        public int getMaxRetries() {
            return 3; // TODO
        }

        @Override
        public String getBlobDescription() {
            return blob;
        }
    }
}

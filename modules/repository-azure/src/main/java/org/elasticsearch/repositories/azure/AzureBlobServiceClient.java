/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import io.netty.buffer.ByteBufAllocator;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;

class AzureBlobServiceClient {
    private final BlobServiceClient blobServiceClient;
    private final BlobServiceAsyncClient blobAsyncClient;
    private final int maxRetries;
    private final ByteBufAllocator allocator;

    AzureBlobServiceClient(
        BlobServiceClient blobServiceClient,
        BlobServiceAsyncClient blobAsyncClient,
        int maxRetries,
        ByteBufAllocator allocator
    ) {
        this.blobServiceClient = blobServiceClient;
        this.blobAsyncClient = blobAsyncClient;
        this.maxRetries = maxRetries;
        this.allocator = allocator;
    }

    BlobServiceClient getSyncClient() {
        return blobServiceClient;
    }

    BlobServiceAsyncClient getAsyncClient() {
        return blobAsyncClient;
    }

    public ByteBufAllocator getAllocator() {
        return allocator;
    }

    int getMaxRetries() {
        return maxRetries;
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.azure;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import io.netty.buffer.ByteBufAllocator;

class AzureBlobServiceClient {
    private final BlobServiceClient blobServiceClient;
    private final BlobServiceAsyncClient blobAsyncClient;
    private final int maxRetries;
    private final ByteBufAllocator allocator;

    AzureBlobServiceClient(BlobServiceClient blobServiceClient,
                           BlobServiceAsyncClient blobAsyncClient,
                           int maxRetries,
                           ByteBufAllocator allocator) {
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

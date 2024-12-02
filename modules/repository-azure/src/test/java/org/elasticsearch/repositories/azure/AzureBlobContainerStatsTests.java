/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.SuppressForbidden;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class AzureBlobContainerStatsTests extends AbstractAzureServerTestCase {

    @SuppressForbidden(reason = "use a http server")
    @Before
    public void configureAzureHandler() {
        httpServer.createContext("/", new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE));
    }

    public void testOperationPurposeIsReflectedInBlobStoreStats() throws IOException {
        serverlessMode = true;
        AzureBlobContainer blobContainer = asInstanceOf(AzureBlobContainer.class, createBlobContainer(between(1, 3)));
        AzureBlobStore blobStore = blobContainer.getBlobStore();
        OperationPurpose purpose = randomFrom(OperationPurpose.values());

        String blobName = randomIdentifier();
        // PUT_BLOB
        blobStore.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);
        // LIST_BLOBS
        blobStore.listBlobsByPrefix(purpose, randomIdentifier(), randomIdentifier());
        // GET_BLOB_PROPERTIES
        blobStore.blobExists(purpose, blobName);
        // PUT_BLOCK & PUT_BLOCK_LIST
        byte[] blobContent = randomByteArrayOfLength((int) blobStore.getUploadBlockSize());
        blobStore.writeBlob(purpose, randomIdentifier(), false, os -> {
            os.write(blobContent);
            os.flush();
        });
        // BLOB_BATCH
        blobStore.deleteBlobsIgnoringIfNotExists(purpose, List.of(randomIdentifier(), randomIdentifier(), randomIdentifier()).iterator());

        Map<String, BlobStoreActionStats> stats = blobStore.stats();
        String statsMapString = stats.toString();
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.PUT_BLOB)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.LIST_BLOBS)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.GET_BLOB_PROPERTIES)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.PUT_BLOCK)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.PUT_BLOCK_LIST)).operations());
        assertEquals(statsMapString, 1L, stats.get(statsKey(purpose, AzureBlobStore.Operation.BLOB_BATCH)).operations());
    }

    public void testOperationPurposeIsNotReflectedInBlobStoreStatsWhenNotServerless() throws IOException {
        serverlessMode = false;
        AzureBlobContainer blobContainer = asInstanceOf(AzureBlobContainer.class, createBlobContainer(between(1, 3)));
        AzureBlobStore blobStore = blobContainer.getBlobStore();

        int repeatTimes = randomIntBetween(1, 3);
        for (int i = 0; i < repeatTimes; i++) {
            OperationPurpose purpose = randomFrom(OperationPurpose.values());

            String blobName = randomIdentifier();
            // PUT_BLOB
            blobStore.writeBlob(purpose, blobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(randomBlobContent())), false);
            // LIST_BLOBS
            blobStore.listBlobsByPrefix(purpose, randomIdentifier(), randomIdentifier());
            // GET_BLOB_PROPERTIES
            blobStore.blobExists(purpose, blobName);
            // PUT_BLOCK & PUT_BLOCK_LIST
            byte[] blobContent = randomByteArrayOfLength((int) blobStore.getUploadBlockSize());
            blobStore.writeBlob(purpose, randomIdentifier(), false, os -> {
                os.write(blobContent);
                os.flush();
            });
            // BLOB_BATCH
            blobStore.deleteBlobsIgnoringIfNotExists(
                purpose,
                List.of(randomIdentifier(), randomIdentifier(), randomIdentifier()).iterator()
            );
        }

        Map<String, BlobStoreActionStats> stats = blobStore.stats();
        String statsMapString = stats.toString();
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.PUT_BLOB.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.LIST_BLOBS.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.GET_BLOB_PROPERTIES.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.PUT_BLOCK.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.PUT_BLOCK_LIST.getKey()).operations());
        assertEquals(statsMapString, repeatTimes, stats.get(AzureBlobStore.Operation.BLOB_BATCH.getKey()).operations());
    }

    private static String statsKey(OperationPurpose purpose, AzureBlobStore.Operation operation) {
        return purpose.getKey() + "_" + operation.getKey();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import com.google.cloud.BatchResult;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleCloudStorageBlobStoreContainerTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDeleteBlobsIgnoringIfNotExistsThrowsIOException() throws Exception {
        final List<String> blobs = Arrays.asList("blobA", "blobB");

        final StorageBatch batch = mock(StorageBatch.class);
        if (randomBoolean()) {
            StorageBatchResult<Boolean> result = mock(StorageBatchResult.class);
            when(batch.delete(any(BlobId.class))).thenReturn(result);
            doThrow(new StorageException(new IOException("Batch submit throws a storage exception"))).when(batch).submit();
        } else {
            StorageBatchResult<Boolean> resultA = mock(StorageBatchResult.class);
            doReturn(resultA).when(batch).delete(eq(BlobId.of("bucket", "blobA")));
            doAnswer(invocation -> {
                StorageException storageException = new StorageException(new IOException("Batched delete throws a storage exception"));
                ((BatchResult.Callback) invocation.getArguments()[0]).error(storageException);
                return null;
            }).when(resultA).notify(any(StorageBatchResult.Callback.class));

            StorageBatchResult<Boolean> resultB = mock(StorageBatchResult.class);
            doReturn(resultB).when(batch).delete(eq(BlobId.of("bucket", "blobB")));
            doAnswer(invocation -> {
                if (randomBoolean()) {
                    StorageException storageException = new StorageException(new IOException("Batched delete throws a storage exception"));
                    ((BatchResult.Callback) invocation.getArguments()[0]).error(storageException);
                } else {
                    ((BatchResult.Callback) invocation.getArguments()[0]).success(randomBoolean());
                }
                return null;
            }).when(resultB).notify(any(StorageBatchResult.Callback.class));

            doNothing().when(batch).submit();
        }

        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        when(storage.batch()).thenReturn(batch);

        final GoogleCloudStorageService storageService = mock(GoogleCloudStorageService.class);
        when(storageService.client(any(String.class), any(String.class), any(GoogleCloudStorageOperationsStats.class))).thenReturn(storage);

        try (BlobStore store = new GoogleCloudStorageBlobStore("bucket", "test", "repo", storageService,
            BigArrays.NON_RECYCLING_INSTANCE, randomIntBetween(1, 8) * 1024)) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);

            IOException e = expectThrows(IOException.class, () -> container.deleteBlobsIgnoringIfNotExists(blobs.iterator()));
            assertThat(e.getCause(), instanceOf(StorageException.class));
        }
    }
}

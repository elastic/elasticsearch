/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.cloud.BatchResult;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
            doThrow(new StorageException(new IOException("Batch item delete throws exception"))).when(resultA).get();

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
        final com.google.api.services.storage.Storage storageRpc = mock(com.google.api.services.storage.Storage.class);
        final MeteredStorage meteredStorage = new MeteredStorage(storage, storageRpc, new GcsRepositoryStatsCollector());

        final GoogleCloudStorageService storageService = mock(GoogleCloudStorageService.class);
        when(storageService.client(eq(ProjectId.DEFAULT), any(String.class), any(String.class), any(GcsRepositoryStatsCollector.class)))
            .thenReturn(meteredStorage);

        try (
            BlobStore store = new GoogleCloudStorageBlobStore(
                ProjectId.DEFAULT,
                "bucket",
                "test",
                "repo",
                storageService,
                BigArrays.NON_RECYCLING_INSTANCE,
                randomIntBetween(1, 8) * 1024,
                BackoffPolicy.noBackoff(),
                new GcsRepositoryStatsCollector()
            )
        ) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);

            IOException e = expectThrows(
                IOException.class,
                () -> container.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobs.iterator())
            );
            assertThat(e.getCause(), instanceOf(StorageException.class));
        }
    }

    public void testCopy() throws Exception {
        final var sourceBucketName = randomAlphaOfLengthBetween(1, 10);
        final var sourceBlobName = randomAlphaOfLengthBetween(1, 10);
        final var blobName = randomAlphaOfLengthBetween(1, 10);
        final var projectId = ProjectId.DEFAULT;
        final String clientName = "test";
        final String repositoryName = "repo";
        final var storageService = mock(GoogleCloudStorageService.class);
        final var statsCollector = new GcsRepositoryStatsCollector();
        final var blobStore = new GoogleCloudStorageBlobStore(
            ProjectId.DEFAULT,
            sourceBucketName,
            clientName,
            repositoryName,
            storageService,
            BigArrays.NON_RECYCLING_INSTANCE,
            1024,
            BackoffPolicy.noBackoff(),
            statsCollector
        );
        final var storage = mock(Storage.class);
        final var storageRpc = mock(com.google.api.services.storage.Storage.class);
        final var meteredStorage = new MeteredStorage(storage, storageRpc, statsCollector);
        when(storageService.client(projectId, clientName, repositoryName, statsCollector)).thenReturn(meteredStorage);
        final long megabytesCopiedPerChunk = randomIntBetween(5, 1000);
        final var clientSettings = mock(GoogleCloudStorageClientSettings.class);
        when(clientSettings.getMegabytesCopiedPerChunk()).thenReturn(megabytesCopiedPerChunk);
        when(storageService.clientSettings(projectId, clientName)).thenReturn(clientSettings);

        final var srcBlobPath = BlobPath.EMPTY.add(randomAlphaOfLengthBetween(1, 10));
        final var srcBlobContainer = new GoogleCloudStorageBlobContainer(srcBlobPath, blobStore);
        final var dstBlobPath = BlobPath.EMPTY.add(randomAlphaOfLengthBetween(1, 10));
        final var dstBlobContainer = new GoogleCloudStorageBlobContainer(dstBlobPath, blobStore);

        final ArgumentCaptor<Storage.CopyRequest> captor = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        final CopyWriter copyWriter = mock(com.google.cloud.storage.CopyWriter.class);
        when(copyWriter.isDone()).thenReturn(true);
        when(storage.copy(captor.capture())).thenReturn(copyWriter);

        dstBlobContainer.copyBlob(randomPurpose(), srcBlobContainer, sourceBlobName, blobName, randomLongBetween(1, 10_000));

        final Storage.CopyRequest request = captor.getValue();
        assertEquals(sourceBucketName, request.getSource().getBucket());
        assertEquals(srcBlobPath.buildAsString() + sourceBlobName, request.getSource().getName());
        assertEquals(sourceBucketName, request.getTarget().getBucket());
        assertEquals(dstBlobPath.buildAsString() + blobName, request.getTarget().getName());
        assertEquals(megabytesCopiedPerChunk, request.getMegabytesCopiedPerChunk().longValue());
    }
}

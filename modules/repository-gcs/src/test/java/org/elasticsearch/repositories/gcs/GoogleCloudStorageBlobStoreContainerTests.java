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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.multipartupload.model.AbortMultipartUploadRequest;
import com.google.cloud.storage.multipartupload.model.AbortMultipartUploadResponse;
import com.google.cloud.storage.multipartupload.model.CreateMultipartUploadResponse;
import com.google.cloud.storage.multipartupload.model.UploadPartResponse;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

        GoogleCloudStorageClientSettings mockClientSettings = mock(GoogleCloudStorageClientSettings.class);
        when(mockClientSettings.getTenaciousRetriesEnabled()).thenReturn(randomBoolean());
        when(storageService.clientSettings(any(), any())).thenReturn(mockClientSettings);

        try (
            BlobStore store = new GoogleCloudStorageBlobStore(
                ProjectId.DEFAULT,
                "bucket",
                "test",
                "repo",
                storageService,
                BigArrays.NON_RECYCLING_INSTANCE,
                randomIntBetween(1, 8) * 1024,
                GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE,
                BackoffPolicy.noBackoff(),
                new GcsRepositoryStatsCollector(),
                null,
                null
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

    public void testConcurrentWriteBlobAtomicAborted() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final long partSize = GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE;
        // 3 parts: ceil((2*partSize+1) / partSize) = 3
        final long blobSize = partSize * 2 + 1;
        final String uploadId = randomAlphaOfLength(25);

        // stages: 0 = uploadPart throws, 1 = completeMultipartUpload throws, 2 = provider throws IOException
        final int stage = randomInt(2);
        final IOException providerException = (stage == 2) ? new IOException("provider failure") : null;

        final MeteredStorage meteredStorage = mock(MeteredStorage.class);
        when(meteredStorage.meteredCreateMultipartUpload(any(), any())).thenReturn(
            CreateMultipartUploadResponse.builder().uploadId(uploadId).build()
        );

        if (stage == 0) {
            when(meteredStorage.meteredUploadPart(any(), any(), any())).thenThrow(new IOException("upload part failed"));
        } else if (stage == 1) {
            when(meteredStorage.meteredUploadPart(any(), any(), any())).thenAnswer(
                inv -> UploadPartResponse.builder().eTag(randomAlphaOfLength(20)).build()
            );
            doThrow(new IOException("complete failed")).when(meteredStorage).meteredCompleteMultipartUpload(any(), any());
        }
        // stage == 2: provider throws before uploadPart is ever called

        when(meteredStorage.meteredAbortMultipartUpload(any(), any())).thenReturn(new AbortMultipartUploadResponse());

        final BlobContainer.BlobMultiPartInputStreamProvider provider = (stage == 2)
            ? (offset, length) -> { throw providerException; }
            : (offset, length) -> new ByteArrayInputStream(new byte[0]);

        try (GoogleCloudStorageBlobStore blobStore = buildBlobStore(bucketName, meteredStorage)) {
            final IOException e = expectThrows(
                IOException.class,
                () -> blobStore.blobContainer(BlobPath.EMPTY)
                    .writeBlobAtomic(randomPurpose(), blobName, blobSize, provider, false, Runnable::run)
            );

            if (stage == 0 || stage == 2) {
                assertEquals("Failed to upload parts", e.getMessage());
            }
            if (stage == 2) {
                assertSame(providerException, e.getCause());
            }

            verify(meteredStorage, times(1)).meteredCreateMultipartUpload(any(), any());

            final ArgumentCaptor<AbortMultipartUploadRequest> abortCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
            verify(meteredStorage, times(1)).meteredAbortMultipartUpload(any(), abortCaptor.capture());

            final AbortMultipartUploadRequest abortRequest = abortCaptor.getValue();
            assertEquals(bucketName, abortRequest.bucket());
            assertEquals(blobName, abortRequest.key());
            assertEquals(uploadId, abortRequest.uploadId());
        }
    }

    public void testConcurrentWriteBlobAtomicSingleThread() throws Exception {
        testConcurrentWriteBlobAtomic(true);
    }

    public void testConcurrentWriteBlobAtomicMultipleThreads() throws Exception {
        testConcurrentWriteBlobAtomic(false);
    }

    private void testConcurrentWriteBlobAtomic(boolean singleThread) throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final int nbParts = randomIntBetween(2, 5);
        final long partSize = GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE;
        // nbParts = ceil(blobSize / partSize)
        final long blobSize = randomLongBetween((nbParts - 1) * partSize + 1, nbParts * partSize);
        assert nbParts == (blobSize + partSize - 1) / partSize;

        final MeteredStorage meteredStorage = mock(MeteredStorage.class);
        when(meteredStorage.meteredCreateMultipartUpload(any(), any())).thenReturn(
            CreateMultipartUploadResponse.builder().uploadId(randomAlphaOfLength(25)).build()
        );

        final int numThreads = singleThread ? 1 : nbParts;
        final CyclicBarrier barrier = new CyclicBarrier(numThreads);
        when(meteredStorage.meteredUploadPart(any(), any(), any())).thenAnswer(inv -> {
            safeAwait(barrier);
            return UploadPartResponse.builder().eTag("test-etag").build();
        });

        final OperationPurpose purpose = randomPurpose();
        try (GoogleCloudStorageBlobStore blobStore = buildBlobStore(bucketName, meteredStorage)) {
            final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            try {
                executorService.submit(() -> {
                    blobStore.blobContainer(BlobPath.EMPTY)
                        .writeBlobAtomic(
                            purpose,
                            blobName,
                            blobSize,
                            (offset, length) -> new ByteArrayInputStream(new byte[0]),
                            false,
                            executorService
                        );
                    return null;
                }).get();
            } finally {
                terminate(executorService);
            }

            verify(meteredStorage, times(1)).meteredCreateMultipartUpload(any(), any());
            verify(meteredStorage, times(nbParts)).meteredUploadPart(any(), any(), any());
            verify(meteredStorage, times(1)).meteredCompleteMultipartUpload(any(), any());
        }
    }

    private static GoogleCloudStorageBlobStore buildBlobStore(String bucketName, MeteredStorage meteredStorage) throws IOException {
        final GoogleCloudStorageService storageService = mock(GoogleCloudStorageService.class);
        when(storageService.client(any(), any(), any(), any())).thenReturn(meteredStorage);
        final GoogleCloudStorageClientSettings clientSettings = mock(GoogleCloudStorageClientSettings.class);
        when(clientSettings.getTenaciousRetriesEnabled()).thenReturn(false);
        when(storageService.clientSettings(any(), any())).thenReturn(clientSettings);
        return new GoogleCloudStorageBlobStore(
            ProjectId.DEFAULT,
            bucketName,
            "test",
            "repo",
            storageService,
            BigArrays.NON_RECYCLING_INSTANCE,
            randomIntBetween(1, 8) * 1024,
            GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE,
            BackoffPolicy.noBackoff(),
            new GcsRepositoryStatsCollector(),
            null,
            null
        );
    }
}

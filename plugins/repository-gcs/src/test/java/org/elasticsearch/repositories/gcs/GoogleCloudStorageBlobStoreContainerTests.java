/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.gcs;

import com.google.cloud.BatchResult;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.repositories.ESBlobStoreContainerTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.repositories.ESBlobStoreTestCase.randomBytes;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleCloudStorageBlobStoreContainerTests extends ESBlobStoreContainerTestCase {

    @Override
    protected BlobStore newBlobStore() {
        final String bucketName = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        final String clientName = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        final GoogleCloudStorageService storageService = mock(GoogleCloudStorageService.class);
        try {
            when(storageService.client(any(String.class))).thenReturn(new MockStorage(bucketName, new ConcurrentHashMap<>(), random()));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return new GoogleCloudStorageBlobStore(bucketName, clientName, storageService);
    }

    public void testWriteReadLarge() throws IOException {
        try(BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath());
            byte[] data = randomBytes(GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE + 1);
            writeBlob(container, "foobar", new BytesArray(data), randomBoolean());
            if (randomBoolean()) {
                // override file, to check if we get latest contents
                random().nextBytes(data);
                writeBlob(container, "foobar", new BytesArray(data), false);
            }
            try (InputStream stream = container.readBlob("foobar")) {
                BytesRefBuilder target = new BytesRefBuilder();
                while (target.length() < data.length) {
                    byte[] buffer = new byte[scaledRandomIntBetween(1, data.length - target.length())];
                    int offset = scaledRandomIntBetween(0, buffer.length - 1);
                    int read = stream.read(buffer, offset, buffer.length - offset);
                    target.append(new BytesRef(buffer, offset, read));
                }
                assertEquals(data.length, target.length());
                assertArrayEquals(data, Arrays.copyOfRange(target.bytes(), 0, target.length()));
            }
        }
    }

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
        when(storageService.client(any(String.class))).thenReturn(storage);

        try (BlobStore store = new GoogleCloudStorageBlobStore("bucket", "test", storageService)) {
            final BlobContainer container = store.blobContainer(new BlobPath());

            IOException e = expectThrows(IOException.class, () -> container.deleteBlobsIgnoringIfNotExists(blobs));
            assertThat(e.getCause(), instanceOf(StorageException.class));
        }
    }
}

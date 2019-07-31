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

import com.google.api.gax.paging.Page;
import com.google.cloud.BatchResult;
import com.google.cloud.Policy;
import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.ServiceAccount;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRpcOptionUtils;
import com.google.cloud.storage.StorageTestUtils;
import org.elasticsearch.core.internal.io.IOUtils;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * {@link MockStorage} mocks a {@link Storage} client by storing all the blobs
 * in a given concurrent map.
 */
class MockStorage implements Storage {

    private final String bucketName;
    private final ConcurrentMap<String, byte[]> blobs;

    MockStorage(final String bucket, final ConcurrentMap<String, byte[]> blobs) {
        this.bucketName = Objects.requireNonNull(bucket);
        this.blobs = Objects.requireNonNull(blobs);
    }

    @Override
    public Bucket get(String bucket, BucketGetOption... options) {
        if (bucketName.equals(bucket)) {
            return StorageTestUtils.createBucket(this, bucketName);
        } else {
            return null;
        }
    }

    @Override
    public Bucket lockRetentionPolicy(final BucketInfo bucket, final BucketTargetOption... options) {
        return null;
    }

    @Override
    public Blob get(BlobId blob) {
        if (bucketName.equals(blob.getBucket())) {
            final byte[] bytes = blobs.get(blob.getName());
            if (bytes != null) {
                return StorageTestUtils.createBlob(this, bucketName, blob.getName(), bytes.length);
            }
        }
        return null;
    }

    @Override
    public boolean delete(BlobId blob) {
        if (bucketName.equals(blob.getBucket()) && blobs.containsKey(blob.getName())) {
            return blobs.remove(blob.getName()) != null;
        }
        return false;
    }

    @Override
    public List<Boolean> delete(Iterable<BlobId> blobIds) {
        final List<Boolean> ans = new ArrayList<>();
        for (final BlobId blobId : blobIds) {
            ans.add(delete(blobId));
        }
        return ans;
    }

    @Override
    public Blob create(BlobInfo blobInfo, byte[] content, BlobTargetOption... options) {
        if (bucketName.equals(blobInfo.getBucket()) == false) {
            throw new StorageException(404, "Bucket not found");
        }
        if (Stream.of(options).anyMatch(option -> option.equals(BlobTargetOption.doesNotExist()))) {
            byte[] existingBytes = blobs.putIfAbsent(blobInfo.getName(), content);
            if (existingBytes != null) {
                throw new StorageException(412, "Blob already exists");
            }
        } else {
            blobs.put(blobInfo.getName(), content);
        }
        return get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));
    }

    @Override
    public Page<Blob> list(String bucket, BlobListOption... options) {
        if (bucketName.equals(bucket) == false) {
            throw new StorageException(404, "Bucket not found");
        }
        final Storage storage = this;
        final String prefix = StorageRpcOptionUtils.getPrefix(options);

        return new Page<Blob>() {
            @Override
            public boolean hasNextPage() {
                return false;
            }

            @Override
            public String getNextPageToken() {
                return null;
            }

            @Override
            public Page<Blob> getNextPage() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterable<Blob> iterateAll() {
                return blobs.entrySet().stream()
                    .filter(blob -> ((prefix == null) || blob.getKey().startsWith(prefix)))
                    .map(blob -> StorageTestUtils.createBlob(storage, bucketName, blob.getKey(), blob.getValue().length))
                    .collect(Collectors.toList());
            }

            @Override
            public Iterable<Blob> getValues() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public ReadChannel reader(BlobId blob, BlobSourceOption... options) {
        if (bucketName.equals(blob.getBucket())) {
            final byte[] bytes = blobs.get(blob.getName());

            final ReadableByteChannel readableByteChannel;
            if (bytes != null) {
                readableByteChannel = Channels.newChannel(new ByteArrayInputStream(bytes));
            } else {
                readableByteChannel = new ReadableByteChannel() {
                    @Override
                    public int read(ByteBuffer dst) throws IOException {
                        throw new StorageException(404, "Object not found");
                    }

                    @Override
                    public boolean isOpen() {
                        return false;
                    }

                    @Override
                    public void close() throws IOException {
                    }
                };
            }
            return new ReadChannel() {
                @Override
                public void close() {
                    IOUtils.closeWhileHandlingException(readableByteChannel);
                }

                @Override
                public void seek(long position) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void setChunkSize(int chunkSize) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public RestorableState<ReadChannel> capture() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int read(ByteBuffer dst) throws IOException {
                    return readableByteChannel.read(dst);
                }

                @Override
                public boolean isOpen() {
                    return readableByteChannel.isOpen();
                }
            };
        }
        return null;
    }

    @Override
    public WriteChannel writer(BlobInfo blobInfo, BlobWriteOption... options) {
        if (bucketName.equals(blobInfo.getBucket())) {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            return new WriteChannel() {

                final WritableByteChannel writableByteChannel = Channels.newChannel(output);

                @Override
                public void setChunkSize(int chunkSize) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public RestorableState<WriteChannel> capture() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int write(ByteBuffer src) throws IOException {
                    return writableByteChannel.write(src);
                }

                @Override
                public boolean isOpen() {
                    return writableByteChannel.isOpen();
                }

                @Override
                public void close() {
                    IOUtils.closeWhileHandlingException(writableByteChannel);
                    if (Stream.of(options).anyMatch(option -> option.equals(BlobWriteOption.doesNotExist()))) {
                        byte[] existingBytes = blobs.putIfAbsent(blobInfo.getName(), output.toByteArray());
                        if (existingBytes != null) {
                            throw new StorageException(412, "Blob already exists");
                        }
                    } else {
                        blobs.put(blobInfo.getName(), output.toByteArray());
                    }
                }
            };
        }
        return null;
    }

    @Override
    public WriteChannel writer(URL signedURL) {
        return null;
    }

    // Everything below this line is not implemented.

    @Override
    public CopyWriter copy(CopyRequest copyRequest) {
        return null;
    }

    @Override
    public Blob create(BlobInfo blobInfo, byte[] content, int offset, int length, BlobTargetOption... options) {
        return null;
    }

    @Override
    public Bucket create(BucketInfo bucketInfo, BucketTargetOption... options) {
        return null;
    }

    @Override
    public Blob create(BlobInfo blobInfo, BlobTargetOption... options) {
        return null;
    }

    @Override
    public Blob create(BlobInfo blobInfo, InputStream content, BlobWriteOption... options) {
        return null;
    }

    @Override
    public Blob get(String bucket, String blob, BlobGetOption... options) {
        return null;
    }

    @Override
    public Blob get(BlobId blob, BlobGetOption... options) {
        return null;
    }

    @Override
    public Page<Bucket> list(BucketListOption... options) {
        return null;
    }

    @Override
    public Bucket update(BucketInfo bucketInfo, BucketTargetOption... options) {
        return null;
    }

    @Override
    public Blob update(BlobInfo blobInfo, BlobTargetOption... options) {
        return null;
    }

    @Override
    public Blob update(BlobInfo blobInfo) {
        return null;
    }

    @Override
    public boolean delete(String bucket, BucketSourceOption... options) {
        return false;
    }

    @Override
    public boolean delete(String bucket, String blob, BlobSourceOption... options) {
        return false;
    }

    @Override
    public boolean delete(BlobId blob, BlobSourceOption... options) {
        return false;
    }

    @Override
    public Blob compose(ComposeRequest composeRequest) {
        return null;
    }

    @Override
    public byte[] readAllBytes(String bucket, String blob, BlobSourceOption... options) {
        return new byte[0];
    }

    @Override
    public byte[] readAllBytes(BlobId blob, BlobSourceOption... options) {
        return new byte[0];
    }

    @Override
    @SuppressWarnings("unchecked")
    public StorageBatch batch() {
        final Answer<?> throwOnMissingMock = invocationOnMock -> {
            throw new AssertionError("Did not expect call to method [" + invocationOnMock.getMethod().getName() + ']');
        };
        final StorageBatch batch = mock(StorageBatch.class, throwOnMissingMock);
        StorageBatchResult<Boolean> result = mock(StorageBatchResult.class, throwOnMissingMock);
        doAnswer(answer -> {
            BatchResult.Callback<Boolean, Exception> callback = (BatchResult.Callback<Boolean, Exception>) answer.getArguments()[0];
            callback.success(true);
            return null;
        }).when(result).notify(any(BatchResult.Callback.class));
        doAnswer(invocation -> {
            final BlobId blobId = (BlobId) invocation.getArguments()[0];
            delete(blobId);
            return result;
        }).when(batch).delete(any(BlobId.class), anyVararg());
        doAnswer(invocation -> null).when(batch).submit();
        return batch;
    }

    @Override
    public ReadChannel reader(String bucket, String blob, BlobSourceOption... options) {
        return null;
    }

    @Override
    public URL signUrl(BlobInfo blobInfo, long duration, TimeUnit unit, SignUrlOption... options) {
        return null;
    }

    @Override
    public List<Blob> get(BlobId... blobIds) {
        return null;
    }

    @Override
    public List<Blob> get(Iterable<BlobId> blobIds) {
        return null;
    }

    @Override
    public List<Blob> update(BlobInfo... blobInfos) {
        return null;
    }

    @Override
    public List<Blob> update(Iterable<BlobInfo> blobInfos) {
        return null;
    }

    @Override
    public List<Boolean> delete(BlobId... blobIds) {
        return null;
    }

    @Override
    public Acl getAcl(String bucket, Acl.Entity entity, BucketSourceOption... options) {
        return null;
    }

    @Override
    public Acl getAcl(String bucket, Acl.Entity entity) {
        return null;
    }

    @Override
    public boolean deleteAcl(String bucket, Acl.Entity entity, BucketSourceOption... options) {
        return false;
    }

    @Override
    public boolean deleteAcl(String bucket, Acl.Entity entity) {
        return false;
    }

    @Override
    public Acl createAcl(String bucket, Acl acl, BucketSourceOption... options) {
        return null;
    }

    @Override
    public Acl createAcl(String bucket, Acl acl) {
        return null;
    }

    @Override
    public Acl updateAcl(String bucket, Acl acl, BucketSourceOption... options) {
        return null;
    }

    @Override
    public Acl updateAcl(String bucket, Acl acl) {
        return null;
    }

    @Override
    public List<Acl> listAcls(String bucket, BucketSourceOption... options) {
        return null;
    }

    @Override
    public List<Acl> listAcls(String bucket) {
        return null;
    }

    @Override
    public Acl getDefaultAcl(String bucket, Acl.Entity entity) {
        return null;
    }

    @Override
    public boolean deleteDefaultAcl(String bucket, Acl.Entity entity) {
        return false;
    }

    @Override
    public Acl createDefaultAcl(String bucket, Acl acl) {
        return null;
    }

    @Override
    public Acl updateDefaultAcl(String bucket, Acl acl) {
        return null;
    }

    @Override
    public List<Acl> listDefaultAcls(String bucket) {
        return null;
    }

    @Override
    public Acl getAcl(BlobId blob, Acl.Entity entity) {
        return null;
    }

    @Override
    public boolean deleteAcl(BlobId blob, Acl.Entity entity) {
        return false;
    }

    @Override
    public Acl createAcl(BlobId blob, Acl acl) {
        return null;
    }

    @Override
    public Acl updateAcl(BlobId blob, Acl acl) {
        return null;
    }

    @Override
    public List<Acl> listAcls(BlobId blob) {
        return null;
    }

    @Override
    public Policy getIamPolicy(String bucket, BucketSourceOption... options) {
        return null;
    }

    @Override
    public Policy setIamPolicy(String bucket, Policy policy, BucketSourceOption... options) {
        return null;
    }

    @Override
    public List<Boolean> testIamPermissions(String bucket, List<String> permissions, BucketSourceOption... options) {
        return null;
    }

    @Override
    public ServiceAccount getServiceAccount(String projectId) {
        return null;
    }

    @Override
    public StorageOptions getOptions() {
        return null;
    }
}

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

import org.elasticsearch.common.SuppressForbidden;
import org.mockito.Matchers;

import com.google.api.gax.paging.Page;
import com.google.cloud.Policy;
import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Acl.Entity;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.ServiceAccount;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.spi.v1.StorageRpc;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageOptions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link MockStorage} mocks a {@link Storage} client by storing all the blobs
 * in a given concurrent map.
 */
class MockStorage implements Storage {

    private final Bucket theBucket;
    private final ConcurrentMap<String, Blob> blobsMap;

    @SuppressForbidden(reason = "mocking here requires reflection that trespasses the access system")
    MockStorage(final String bucketName, final ConcurrentMap<String, Blob> blobs) {
        this.blobsMap = blobs;
        // mock bucket
        this.theBucket = mock(Bucket.class);
        when(this.theBucket.getName()).thenReturn(bucketName);
        doAnswer(invocation -> {
            assert invocation.getArguments().length == 1 : "Only a single filter is mocked";
            final BlobListOption prefixFilter = (BlobListOption) invocation.getArguments()[0];
            final Method optionMethod = BlobListOption.class.getSuperclass().getDeclaredMethod("getRpcOption");
            optionMethod.setAccessible(true);
            assert StorageRpc.Option.PREFIX.equals(optionMethod.invoke(prefixFilter)) : "Only the prefix filter is mocked";
            final Method valueMethod = BlobListOption.class.getSuperclass().getDeclaredMethod("getValue");
            valueMethod.setAccessible(true);
            final String prefixValue = (String) valueMethod.invoke(prefixFilter);
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
                    return null;
                }

                @Override
                public Iterable<Blob> iterateAll() {
                    return getValues();
                }

                @Override
                public Iterable<Blob> getValues() {
                    return () -> blobs.entrySet()
                            .stream()
                            .filter(entry1 -> entry1.getKey().startsWith(prefixValue))
                            .map(entry2 -> entry2.getValue())
                            .iterator();
                }
            };
        }).when(this.theBucket).list(Matchers.anyVararg());
    }

    @Override
    public StorageOptions getOptions() {
        return StorageOptions.getDefaultInstance();
    }

    @Override
    public Bucket create(BucketInfo bucketInfo, BucketTargetOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Blob create(BlobInfo blobInfo, BlobTargetOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Blob create(BlobInfo blobInfo, byte[] content, BlobTargetOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Blob create(BlobInfo blobInfo, InputStream content, BlobWriteOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Bucket get(String bucketName, BucketGetOption... options) {
        assert bucketName.equals(this.theBucket.getName()) : "Only a single bucket is mocked";
        return theBucket;
    }

    @Override
    public Blob get(String bucketName, String blobName, BlobGetOption... options) {
        assert bucketName.equals(this.theBucket.getName()) : "Only a single bucket is mocked";
        return blobsMap.get(blobName);
    }

    @Override
    public Blob get(BlobId blob, BlobGetOption... options) {
        return get(blob.getBucket(), blob.getName());
    }

    @Override
    public Blob get(BlobId blob) {
        return get(blob.getBucket(), blob.getName());
    }

    @Override
    public Page<Bucket> list(BucketListOption... options) {
        return new Page<Bucket>() {
            @Override
            public boolean hasNextPage() {
                return false;
            }
            @Override
            public String getNextPageToken() {
                return null;
            }
            @Override
            public Page<Bucket> getNextPage() {
                return null;
            }
            @Override
            public Iterable<Bucket> iterateAll() {
                return getValues();
            }
            @Override
            public Iterable<Bucket> getValues() {
                return Arrays.asList(theBucket);
            }
        };
    }

    @Override
    public Page<Blob> list(String bucketName, BlobListOption... options) {
        assert bucketName.equals(this.theBucket.getName()) : "Only a single bucket is mocked";
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
                return null;
            }

            @Override
            public Iterable<Blob> iterateAll() {
                return getValues();
            }

            @Override
            public Iterable<Blob> getValues() {
                return blobsMap.values();
            }
        };
    }

    @Override
    public Bucket update(BucketInfo bucketInfo, BucketTargetOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Blob update(BlobInfo blobInfo, BlobTargetOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Blob update(BlobInfo blobInfo) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public boolean delete(String bucket, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public boolean delete(String bucketName, String blobName, BlobSourceOption... options) {
        assert bucketName.equals(this.theBucket.getName()) : "Only a single bucket is mocked";
        return blobsMap.remove(blobName) != null;
    }

    @Override
    public boolean delete(BlobId blob, BlobSourceOption... options) {
        return delete(blob.getBucket(), blob.getName());
    }

    @Override
    public boolean delete(BlobId blob) {
        return delete(blob.getBucket(), blob.getName());
    }

    @Override
    public Blob compose(ComposeRequest composeRequest) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public CopyWriter copy(CopyRequest copyRequest) {
        assert copyRequest.getSource().getBucket().equals(this.theBucket.getName()) : "Only a single bucket is mocked";
        assert copyRequest.getTarget().getBucket().equals(this.theBucket.getName()) : "Only a single bucket is mocked";
        final Blob sourceBlob = blobsMap.get(copyRequest.getSource().getName());
        return sourceBlob.copyTo(copyRequest.getTarget().getBucket(), copyRequest.getTarget().getName());
    }

    @Override
    public byte[] readAllBytes(String bucketName, String blobName, BlobSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public byte[] readAllBytes(BlobId blob, BlobSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public StorageBatch batch() {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public ReadChannel reader(String bucket, String blob, BlobSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public ReadChannel reader(BlobId blob, BlobSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public WriteChannel writer(BlobInfo blobInfo, BlobWriteOption... options) {
        assert blobInfo.getBucket().equals(this.theBucket.getName()) : "Only a single bucket is mocked";
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        return new WriteChannel() {
            private boolean isOpenFlag = true;

            @Override
            public boolean isOpen() {
                return isOpenFlag;
            }

            @Override
            public void close() throws IOException {
                constructMockBlob(blobInfo.getName(), baos.toByteArray(), blobsMap);
                isOpenFlag = false;
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                final int size1 = baos.size();
                while (src.hasRemaining()) {
                    baos.write(src.get());
                }
                final int size2 = baos.size();
                return size2 - size1;
            }

            @Override
            public void setChunkSize(int chunkSize) {
            }

            @Override
            public RestorableState<WriteChannel> capture() {
                return null;
            }
        };
    }

    @Override
    public URL signUrl(BlobInfo blobInfo, long duration, TimeUnit unit, SignUrlOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public List<Blob> get(BlobId... blobIds) {
        final List<Blob> ans = new ArrayList<>();
        for (final BlobId blobId : blobIds) {
            ans.add(get(blobId));
        }
        return ans;
    }

    @Override
    public List<Blob> get(Iterable<BlobId> blobIds) {
        final List<Blob> ans = new ArrayList<>();
        for (final BlobId blobId : blobIds) {
            ans.add(get(blobId));
        }
        return ans;
    }

    @Override
    public List<Blob> update(BlobInfo... blobInfos) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public List<Blob> update(Iterable<BlobInfo> blobInfos) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public List<Boolean> delete(BlobId... blobIds) {
        final List<Boolean> ans = new ArrayList<>();
        for (final BlobId blobId : blobIds) {
            ans.add(delete(blobId));
        }
        return ans;
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
    public Acl getAcl(String bucket, Entity entity, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl getAcl(String bucket, Entity entity) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public boolean deleteAcl(String bucket, Entity entity, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public boolean deleteAcl(String bucket, Entity entity) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl createAcl(String bucket, Acl acl, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl createAcl(String bucket, Acl acl) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl updateAcl(String bucket, Acl acl, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl updateAcl(String bucket, Acl acl) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public List<Acl> listAcls(String bucket, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public List<Acl> listAcls(String bucket) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl getDefaultAcl(String bucket, Entity entity) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public boolean deleteDefaultAcl(String bucket, Entity entity) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl createDefaultAcl(String bucket, Acl acl) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl updateDefaultAcl(String bucket, Acl acl) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public List<Acl> listDefaultAcls(String bucket) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl getAcl(BlobId blob, Entity entity) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public boolean deleteAcl(BlobId blob, Entity entity) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl createAcl(BlobId blob, Acl acl) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Acl updateAcl(BlobId blob, Acl acl) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public List<Acl> listAcls(BlobId blob) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Policy getIamPolicy(String bucket, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public Policy setIamPolicy(String bucket, Policy policy, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public List<Boolean> testIamPermissions(String bucket, List<String> permissions, BucketSourceOption... options) {
        throw new RuntimeException("Mock not implemented");
    }

    @Override
    public ServiceAccount getServiceAccount(String projectId) {
        throw new RuntimeException("Mock not implemented");
    }

    private static class ReadChannelFromByteArray implements ReadChannel {
        private boolean isOpenFlag;
        private final ByteBuffer byteBuffer;

        ReadChannelFromByteArray(byte[] srcArray) {
            final byte[] clonedArray = Arrays.copyOf(srcArray, srcArray.length);
            byteBuffer = ByteBuffer.wrap(clonedArray);
            isOpenFlag = byteBuffer.hasRemaining();
        }

        @Override
        public boolean isOpen() {
            return isOpenFlag;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (byteBuffer.hasRemaining() == false) {
                return -1;
            }
            final int size1 = dst.remaining();
            while (dst.hasRemaining() && byteBuffer.hasRemaining()) {
                dst.put(byteBuffer.get());
            }
            final int size2 = dst.remaining();
            return size1 - size2;
        }

        @Override
        public void setChunkSize(int chunkSize) {
        }

        @Override
        public void seek(long position) throws IOException {
            byteBuffer.position(Math.toIntExact(position));
        }

        @Override
        public void close() {
            isOpenFlag = false;
        }

        @Override
        public RestorableState<ReadChannel> capture() {
            return null;
        }
    }

    private static Blob constructMockBlob(String blobName, byte[] data, ConcurrentMap<String, Blob> blobsMap) {
        final Blob blobMock = mock(Blob.class);
        when(blobMock.getName()).thenReturn(blobName);
        when(blobMock.getSize()).thenReturn((long) data.length);
        when(blobMock.reload(Matchers.anyVararg())).thenReturn(blobMock);
        when(blobMock.reader(Matchers.anyVararg())).thenReturn(new ReadChannelFromByteArray(data));
        when(blobMock.copyTo(Matchers.anyString(), Matchers.anyVararg()))
                .thenThrow(new RuntimeException("Mock not implemented. Only a single bucket is mocked."));
        doAnswer(invocation -> {
            final String copiedBlobName = (String) invocation.getArguments()[1];
            final Blob copiedMockBlob = constructMockBlob(copiedBlobName, data, blobsMap);
            final CopyWriter ans = mock(CopyWriter.class);
            when(ans.getResult()).thenReturn(copiedMockBlob);
            when(ans.isDone()).thenReturn(true);
            return ans;
        }).when(blobMock).copyTo(Matchers.anyString(), Matchers.anyString(), Matchers.anyVararg());
        doAnswer(invocation -> {
            final BlobId blobId = (BlobId) invocation.getArguments()[0];
            final Blob copiedMockBlob = constructMockBlob(blobId.getName(), data, blobsMap);
            final CopyWriter ans = mock(CopyWriter.class);
            when(ans.getResult()).thenReturn(copiedMockBlob);
            when(ans.isDone()).thenReturn(true);
            return ans;
        }).when(blobMock).copyTo(Matchers.any(BlobId.class), Matchers.anyVararg());
        blobsMap.put(blobName, blobMock);
        return blobMock;
    }

}

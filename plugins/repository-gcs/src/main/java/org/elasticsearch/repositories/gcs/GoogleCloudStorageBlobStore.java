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

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.Streams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

class GoogleCloudStorageBlobStore extends AbstractComponent implements BlobStore {

    // The recommended maximum size of a blob that should be uploaded in a single
    // request. Larger files should be uploaded over multiple requests (this is
    // called "resumable upload")
    // https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
    private static final int LARGE_BLOB_THRESHOLD_BYTE_SIZE = 5 * 1024 * 1024;

    private final Storage storage;
    private final String bucket;

    GoogleCloudStorageBlobStore(Settings settings, String bucket, Storage storage) {
        super(settings);
        this.bucket = bucket;
        this.storage = storage;
        if (doesBucketExist(bucket) == false) {
            throw new BlobStoreException("Bucket [" + bucket + "] does not exist");
        }
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new GoogleCloudStorageBlobContainer(path, this);
    }

    @Override
    public void delete(BlobPath path) throws IOException {
        deleteBlobsByPrefix(path.buildAsString());
    }

    @Override
    public void close() {
    }

    /**
     * Return true if the given bucket exists
     *
     * @param bucketName name of the bucket
     * @return true if the bucket exists, false otherwise
     */
    boolean doesBucketExist(String bucketName) {
        try {
            final Bucket bucket = SocketAccess.doPrivilegedIOException(() -> storage.get(bucketName));
            return bucket != null;
        } catch (final Exception e) {
            throw new BlobStoreException("Unable to check if bucket [" + bucketName + "] exists", e);
        }
    }

    /**
     * List blobs in the bucket under the specified path. The path root is removed.
     *
     * @param path
     *            base path of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetaData> listBlobs(String path) throws IOException {
        return listBlobsByPrefix(path, "");
    }

    /**
     * List all blobs in the bucket which have a prefix
     *
     * @param path
     *            base path of the blobs to list. This path is removed from the
     *            names of the blobs returned.
     * @param prefix
     *            prefix of the blobs to list.
     * @return a map of blob names and their metadata.
     */
    Map<String, BlobMetaData> listBlobsByPrefix(String path, String prefix) throws IOException {
        final String pathPrefix = buildKey(path, prefix);
        final MapBuilder<String, BlobMetaData> mapBuilder = MapBuilder.newMapBuilder();
        SocketAccess.doPrivilegedVoidIOException(() -> {
            storage.get(bucket).list(BlobListOption.prefix(pathPrefix)).iterateAll().forEach(blob -> {
                assert blob.getName().startsWith(path);
                final String suffixName = blob.getName().substring(path.length());
                mapBuilder.put(suffixName, new PlainBlobMetaData(suffixName, blob.getSize()));
            });
        });
        return mapBuilder.immutableMap();
    }

    /**
     * Returns true if the blob exists in the bucket
     *
     * @param blobName name of the blob
     * @return true if the blob exists, false otherwise
     */
    boolean blobExists(String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucket, blobName);
        final Blob blob = SocketAccess.doPrivilegedIOException(() -> storage.get(blobId));
        return blob != null;
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @return an InputStream
     */
    InputStream readBlob(String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucket, blobName);
        final Blob blob = SocketAccess.doPrivilegedIOException(() -> storage.get(blobId));
        if (blob == null) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exit");
        }
        final ReadChannel readChannel = SocketAccess.doPrivilegedIOException(blob::reader);
        return Channels.newInputStream(new ReadableByteChannel() {
            @SuppressForbidden(reason = "Channel is based of a socket not a file")
            @Override
            public int read(ByteBuffer dst) throws IOException {
                return SocketAccess.doPrivilegedIOException(() -> readChannel.read(dst));
            }

            @Override
            public boolean isOpen() {
                return readChannel.isOpen();
            }

            @Override
            public void close() throws IOException {
                SocketAccess.doPrivilegedVoidIOException(readChannel::close);
            }
        });
    }

    /**
     * Writes a blob in the bucket.
     *
     * @param inputStream content of the blob to be written
     * @param blobSize    expected size of the blob to be written
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucket, blobName).build();
        if (blobSize > LARGE_BLOB_THRESHOLD_BYTE_SIZE) {
            writeBlobResumable(blobInfo, inputStream);
        } else {
            writeBlobMultipart(blobInfo, inputStream, blobSize);
        }
    }

    /**
     * Uploads a blob using the "resumable upload" method (multiple requests, which
     * can be independently retried in case of failure, see
     * https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
     *
     * @param blobInfo the info for the blob to be uploaded
     * @param inputStream the stream containing the blob data
     */
    private void writeBlobResumable(BlobInfo blobInfo, InputStream inputStream) throws IOException {
        try {
            final WriteChannel writeChannel = SocketAccess.doPrivilegedIOException(
                () -> storage.writer(blobInfo, Storage.BlobWriteOption.doesNotExist()));
            Streams.copy(inputStream, Channels.newOutputStream(new WritableByteChannel() {
                @Override
                public boolean isOpen() {
                    return writeChannel.isOpen();
                }

                @Override
                public void close() throws IOException {
                    SocketAccess.doPrivilegedVoidIOException(writeChannel::close);
                }

                @SuppressForbidden(reason = "Channel is based of a socket not a file")
                @Override
                public int write(ByteBuffer src) throws IOException {
                    return SocketAccess.doPrivilegedIOException(() -> writeChannel.write(src));
                }
            }));
        } catch (StorageException se) {
            if (se.getCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
            }
            throw se;
        }
    }

    /**
     * Uploads a blob using the "multipart upload" method (a single
     * 'multipart/related' request containing both data and metadata. The request is
     * gziped), see:
     * https://cloud.google.com/storage/docs/json_api/v1/how-tos/multipart-upload
     *
     * @param blobInfo the info for the blob to be uploaded
     * @param inputStream the stream containing the blob data
     * @param blobSize the size
     */
    private void writeBlobMultipart(BlobInfo blobInfo, InputStream inputStream, long blobSize) throws IOException {
        assert blobSize <= LARGE_BLOB_THRESHOLD_BYTE_SIZE : "large blob uploads should use the resumable upload method";
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(Math.toIntExact(blobSize));
        Streams.copy(inputStream, baos);
        SocketAccess.doPrivilegedVoidIOException(
            () -> {
                try {
                    storage.create(blobInfo, baos.toByteArray(), Storage.BlobTargetOption.doesNotExist());
                } catch (StorageException se) {
                    if (se.getCode() == HTTP_PRECON_FAILED) {
                        throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
                    }
                    throw se;
                }
            });
    }

    /**
     * Deletes a blob in the bucket
     *
     * @param blobName name of the blob
     */
    void deleteBlob(String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucket, blobName);
        final boolean deleted = SocketAccess.doPrivilegedIOException(() -> storage.delete(blobId));
        if (deleted == false) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
    }

    /**
     * Deletes multiple blobs in the bucket that have a given prefix
     *
     * @param prefix prefix of the buckets to delete
     */
    void deleteBlobsByPrefix(String prefix) throws IOException {
        deleteBlobs(listBlobsByPrefix("", prefix).keySet());
    }

    /**
     * Deletes multiple blobs in the given bucket (uses a batch request to perform this)
     *
     * @param blobNames names of the bucket to delete
     */
    void deleteBlobs(Collection<String> blobNames) throws IOException {
        if (blobNames.isEmpty()) {
            return;
        }
        // for a single op submit a simple delete instead of a batch of size 1
        if (blobNames.size() == 1) {
            deleteBlob(blobNames.iterator().next());
            return;
        }
        final List<BlobId> blobIdsToDelete = blobNames.stream().map(blobName -> BlobId.of(bucket, blobName)).collect(Collectors.toList());
        final List<Boolean> deletedStatuses = SocketAccess.doPrivilegedIOException(() -> storage.delete(blobIdsToDelete));
        assert blobIdsToDelete.size() == deletedStatuses.size();
        boolean failed = false;
        for (int i = 0; i < blobIdsToDelete.size(); i++) {
            if (deletedStatuses.get(i) == false) {
                logger.error("Failed to delete blob [{}] in bucket [{}]", blobIdsToDelete.get(i).getName(), bucket);
                failed = true;
            }
        }
        if (failed) {
            throw new IOException("Failed to delete all [" + blobIdsToDelete.size() + "] blobs");
        }
    }

    private static String buildKey(String keyPath, String s) {
        assert s != null;
        return keyPath + s;
    }

}

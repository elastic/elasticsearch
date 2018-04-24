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
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.settings.Settings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class GoogleCloudStorageBlobStore extends AbstractComponent implements BlobStore {

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
            if (bucket != null) {
                return Strings.hasText(bucket.getName());
            }
            return false;
        } catch (final Exception e) {
            throw new BlobStoreException("Unable to check if bucket [" + bucketName + "] exists", e);
        }
    }

    /**
     * List all blobs in the bucket
     *
     * @param path base path of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetaData> listBlobs(String prefix) throws IOException {
        return listBlobsByPrefix(prefix, "");
    }

    /**
     * List all blobs in the bucket which have a prefix.
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
        if (blob != null) {
            return Strings.hasText(blob.getName());
        }
        return false;
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @return an InputStream
     */
    InputStream readBlob(String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucket, blobName);
        final Tuple<ReadChannel, Long> readerAndSize = SocketAccess.doPrivilegedIOException(() ->
            {
            final Blob blob = storage.get(blobId);
                if (blob == null) {
                    return null;
                }
            return new Tuple<>(blob.reader(), blob.getSize());
            });
        if (readerAndSize == null) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exit.");
        }
        final ByteBuffer buffer = ByteBuffer.allocate(8 * 1024);
        // first read pull data
        buffer.flip();
        return new InputStream() {
            long bytesRemaining = readerAndSize.v2();

            @SuppressForbidden(reason = "the reader channel is backed by a socket instead of a file,"
                    + "ie. it is not seekable and reading should advance the readers's internal position")
            @Override
            public int read() throws IOException {
                while (true) {
                    try {
                        return (0xFF & buffer.get());
                    } catch (final BufferUnderflowException e) {
                        // pull another chunck
                        buffer.clear();
                        final long bytesRead = SocketAccess.doPrivilegedIOException(() -> readerAndSize.v1().read(buffer));
                        buffer.flip();
                        if (bytesRead < 0) {
                            return -1;
                        } else if ((bytesRead == 0) && (bytesRemaining == 0)) {
                            return -1;
                        }
                        bytesRemaining -= bytesRead;
                        // retry in case of non-blocking socket
                    }
                }
            }

            @Override
            public void close() throws IOException {
                readerAndSize.v1().close();
            }
        };
    }

    /**
     * Writes a blob in the bucket.
     *
     * @param inputStream content of the blob to be written
     * @param blobSize    expected size of the blob to be written
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucket, blobName).build();
        final byte[] buffer = new byte[64 * 1024];
        SocketAccess.doPrivilegedVoidIOException(() -> {
            long bytesWritten = 0;
            try (WriteChannel writer = storage.writer(blobInfo)) {
                int limit;
                while ((limit = inputStream.read(buffer)) >= 0) {
                    try {
                        Channels.writeToChannel(buffer, 0, limit, writer);
                        bytesWritten += limit;
                    } catch (final Exception e) {
                        throw new IOException("Failed to write blob [" + blobName + "] into bucket [" + bucket + "].", e);
                    }
                }
            }
            assert blobSize == bytesWritten : "InputStream unexpected size, expected [" + blobSize + "] got [" + bytesWritten + "]";
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
        if ((blobNames == null) || blobNames.isEmpty()) {
            return;
        }
        final List<BlobId> blobIdsToDelete = blobNames.stream().map(blobName -> BlobId.of(bucket, blobName)).collect(Collectors.toList());
        final List<Boolean> deletedStatuses = SocketAccess.doPrivilegedIOException(() -> storage.delete(blobIdsToDelete));
        assert blobIdsToDelete.size() == deletedStatuses.size();
        boolean failed = false;
        for (int i = 0; i < blobIdsToDelete.size(); i++) {
            if (deletedStatuses.get(i) == false) {
                logger.error("Failed to delete blob [{}] in bucket [{}].", blobIdsToDelete.get(i).getName(), bucket);
                failed = true;
            }
        }
        if (failed) {
            throw new IOException("Failed to delete all [" + blobIdsToDelete.size() + "] blobs.");
        }
    }

    /**
     * Moves a blob within the same bucket
     *
     * @param sourceBlob name of the blob to move
     * @param targetBlob new name of the blob in the target bucket
     */
    void moveBlob(String sourceBlobName, String targetBlobName) throws IOException {
        final BlobId sourceBlobId = BlobId.of(bucket, sourceBlobName);
        final BlobId targetBlobId = BlobId.of(bucket, targetBlobName);
        final CopyRequest request = CopyRequest.newBuilder()
                .setSource(sourceBlobId)
                .setTarget(targetBlobId)
                .build();
        SocketAccess.doPrivilegedVoidIOException(() -> {
            // There's no atomic "move" in GCS so we need to copy and delete
            final CopyWriter copyWriter = storage.copy(request);
            final Blob destBlob = copyWriter.getResult();
            final boolean deleted = storage.delete(sourceBlobId);
            if ((deleted == false) || (destBlob.reload() == null)) {
                throw new IOException("Failed to move source [" + sourceBlobName + "] to target [" + targetBlobName + "].");
            }
        });
    }

    private static String buildKey(String keyPath, String s) {
        assert s != null;
        return keyPath + s;
    }

}

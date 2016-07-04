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

package org.elasticsearch.common.blobstore.gcs;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class GoogleCloudStorageBlobStore extends AbstractComponent implements BlobStore {

    /**
     * Google Cloud Storage batch requests are limited to 1000 operations
     **/
    private static final int MAX_BATCHING_REQUESTS = 999;

    private final Storage client;
    private final String bucket;

    public GoogleCloudStorageBlobStore(Settings settings, String bucket, Storage storageClient) {
        super(settings);
        this.bucket = bucket;
        this.client = storageClient;

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
            return doPrivileged(() -> {
                try {
                    Bucket bucket = client.buckets().get(bucketName).execute();
                    if (bucket != null) {
                        return Strings.hasText(bucket.getId());
                    }
                } catch (GoogleJsonResponseException e) {
                    GoogleJsonError error = e.getDetails();
                    if ((e.getStatusCode() == HTTP_NOT_FOUND) || ((error != null) && (error.getCode() == HTTP_NOT_FOUND))) {
                        return false;
                    }
                    throw e;
                }
                return false;
            });
        } catch (IOException e) {
            throw new BlobStoreException("Unable to check if bucket [" + bucketName + "] exists", e);
        }
    }

    /**
     * List all blobs in the bucket
     *
     * @param path base path of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetaData> listBlobs(String path) throws IOException {
        return doPrivileged(() -> listBlobsByPath(bucket, path, path));
    }

    /**
     * List all blobs in the bucket which have a prefix
     *
     * @param path   base path of the blobs to list
     * @param prefix prefix of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetaData> listBlobsByPrefix(String path, String prefix) throws IOException {
        return doPrivileged(() -> listBlobsByPath(bucket, buildKey(path, prefix), path));
    }

    /**
     * Lists all blobs in a given bucket
     *
     * @param bucketName   name of the bucket
     * @param path         base path of the blobs to list
     * @param pathToRemove if true, this path part is removed from blob name
     * @return a map of blob names and their metadata
     */
    private Map<String, BlobMetaData> listBlobsByPath(String bucketName, String path, String pathToRemove) throws IOException {
        return blobsStream(client, bucketName, path, MAX_BATCHING_REQUESTS)
                .map(new BlobMetaDataConverter(pathToRemove))
                .collect(Collectors.toMap(PlainBlobMetaData::name, Function.identity()));
    }

    /**
     * Returns true if the blob exists in the bucket
     *
     * @param blobName name of the blob
     * @return true if the blob exists, false otherwise
     */
    boolean blobExists(String blobName) throws IOException {
        return doPrivileged(() -> {
            try {
                StorageObject blob = client.objects().get(bucket, blobName).execute();
                if (blob != null) {
                    return Strings.hasText(blob.getId());
                }
            } catch (GoogleJsonResponseException e) {
                GoogleJsonError error = e.getDetails();
                if ((e.getStatusCode() == HTTP_NOT_FOUND) || ((error != null) && (error.getCode() == HTTP_NOT_FOUND))) {
                    return false;
                }
                throw e;
            }
            return false;
        });
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @return an InputStream
     */
    InputStream readBlob(String blobName) throws IOException {
        return doPrivileged(() -> {
            try {
                Storage.Objects.Get object = client.objects().get(bucket, blobName);
                return object.executeMediaAsInputStream();
            } catch (GoogleJsonResponseException e) {
                GoogleJsonError error = e.getDetails();
                if ((e.getStatusCode() == HTTP_NOT_FOUND) || ((error != null) && (error.getCode() == HTTP_NOT_FOUND))) {
                    throw new FileNotFoundException(e.getMessage());
                }
                throw e;
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
        doPrivileged(() -> {
            InputStreamContent stream = new InputStreamContent(null, inputStream);
            stream.setLength(blobSize);

            Storage.Objects.Insert insert = client.objects().insert(bucket, null, stream);
            insert.setName(blobName);
            insert.execute();
            return null;
        });
    }

    /**
     * Deletes a blob in the bucket
     *
     * @param blobName name of the blob
     */
    void deleteBlob(String blobName) throws IOException {
        doPrivileged(() -> client.objects().delete(bucket, blobName).execute());
    }

    /**
     * Deletes multiple blobs in the bucket that have a given prefix
     *
     * @param prefix prefix of the buckets to delete
     */
    void deleteBlobsByPrefix(String prefix) throws IOException {
        doPrivileged(() -> {
            deleteBlobs(listBlobsByPath(bucket, prefix, null).keySet());
            return null;
        });
    }

    /**
     * Deletes multiple blobs in the given bucket (uses a batch request to perform this)
     *
     * @param blobNames names of the bucket to delete
     */
    void deleteBlobs(Collection<String> blobNames) throws IOException {
        if (blobNames == null || blobNames.isEmpty()) {
            return;
        }

        if (blobNames.size() == 1) {
            deleteBlob(blobNames.iterator().next());
            return;
        }

        doPrivileged(() -> {
            final List<Storage.Objects.Delete> deletions = new ArrayList<>();
            final Iterator<String> blobs = blobNames.iterator();

            while (blobs.hasNext()) {
                // Create a delete request for each blob to delete
                deletions.add(client.objects().delete(bucket, blobs.next()));

                if (blobs.hasNext() == false || deletions.size() == MAX_BATCHING_REQUESTS) {
                    try {
                        // Deletions are executed using a batch request
                        BatchRequest batch = client.batch();

                        // Used to track successful deletions
                        CountDown countDown = new CountDown(deletions.size());

                        for (Storage.Objects.Delete delete : deletions) {
                            // Queue the delete request in batch
                            delete.queue(batch, new JsonBatchCallback<Void>() {
                                @Override
                                public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
                                    logger.error("failed to delete blob [{}] in bucket [{}]: {}", delete.getObject(), delete.getBucket(), e
                                            .getMessage());
                                }

                                @Override
                                public void onSuccess(Void aVoid, HttpHeaders responseHeaders) throws IOException {
                                    countDown.countDown();
                                }
                            });
                        }

                        batch.execute();

                        if (countDown.isCountedDown() == false) {
                            throw new IOException("Failed to delete all [" + deletions.size() + "] blobs");
                        }
                    } finally {
                        deletions.clear();
                    }
                }
            }
            return null;
        });
    }

    /**
     * Moves a blob within the same bucket
     *
     * @param sourceBlob name of the blob to move
     * @param targetBlob new name of the blob in the target bucket
     */
    void moveBlob(String sourceBlob, String targetBlob) throws IOException {
        doPrivileged(() -> {
            // There's no atomic "move" in GCS so we need to copy and delete
            client.objects().copy(bucket, sourceBlob, bucket, targetBlob, null).execute();
            client.objects().delete(bucket, sourceBlob).execute();
            return null;
        });
    }

    /**
     * Executes a {@link PrivilegedExceptionAction} with privileges enabled.
     */
    <T> T doPrivileged(PrivilegedExceptionAction<T> operation) throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) operation::run);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }
    }

    private String buildKey(String keyPath, String s) {
        assert s != null;
        return keyPath + s;
    }

    /**
     * Converts a {@link StorageObject} to a {@link PlainBlobMetaData}
     */
    class BlobMetaDataConverter implements Function<StorageObject, PlainBlobMetaData> {

        private final String pathToRemove;

        BlobMetaDataConverter(String pathToRemove) {
            this.pathToRemove = pathToRemove;
        }

        @Override
        public PlainBlobMetaData apply(StorageObject storageObject) {
            String blobName = storageObject.getName();
            if (Strings.hasLength(pathToRemove)) {
                blobName = blobName.substring(pathToRemove.length());
            }
            return new PlainBlobMetaData(blobName, storageObject.getSize().longValue());
        }
    }

    /**
     * Spliterator can be used to list storage objects stored in a bucket.
     */
    static class StorageObjectsSpliterator implements Spliterator<StorageObject> {

        private final Storage.Objects.List list;

        StorageObjectsSpliterator(Storage client, String bucketName, String prefix, long pageSize) throws IOException {
            list = client.objects().list(bucketName);
            list.setMaxResults(pageSize);
            if (prefix != null) {
                list.setPrefix(prefix);
            }
        }

        @Override
        public boolean tryAdvance(Consumer<? super StorageObject> action) {
            try {
                // Retrieves the next page of items
                Objects objects = list.execute();

                if ((objects == null) || (objects.getItems() == null) || (objects.getItems().isEmpty())) {
                    return false;
                }

                // Consumes all the items
                objects.getItems().forEach(action::accept);

                // Sets the page token of the next page,
                // null indicates that all items have been consumed
                String next = objects.getNextPageToken();
                if (next != null) {
                    list.setPageToken(next);
                    return true;
                }

                return false;
            } catch (Exception e) {
                throw new BlobStoreException("Exception while listing objects", e);
            }
        }

        @Override
        public Spliterator<StorageObject> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return 0;
        }
    }

    /**
     * Returns a {@link Stream} of {@link StorageObject}s that are stored in a given bucket.
     */
    static Stream<StorageObject> blobsStream(Storage client, String bucketName, String prefix, long pageSize) throws IOException {
        return StreamSupport.stream(new StorageObjectsSpliterator(client, bucketName, prefix, pageSize), false);
    }

}

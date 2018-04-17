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

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.rest.RestStatus;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.CopyRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.mock;

/**
 * {@link MockStorage} mocks a {@link Storage} client by storing all the blobs
 * in a given concurrent map.
 */
class MockStorage extends Storage {

    /* A custom HTTP header name used to propagate the name of the blobs to delete in batch requests */
    private static final String DELETION_HEADER = "x-blob-to-delete";

    private final String bucketName;
    private final ConcurrentMap<String, byte[]> blobs;

    MockStorage(final String bucket, final ConcurrentMap<String, byte[]> blobs) {
        super(new MockedHttpTransport(blobs), mock(JsonFactory.class), mock(HttpRequestInitializer.class));
        this.bucketName = bucket;
        this.blobs = blobs;
    }

    @Override
    public Buckets buckets() {
        return new MockBuckets();
    }

    @Override
    public Objects objects() {
        return new MockObjects();
    }

    class MockBuckets extends Buckets {

        @Override
        public Get get(String getBucket) {
            return new Get(getBucket) {
                @Override
                public Bucket execute() {
                    if (bucketName.equals(getBucket())) {
                        final Bucket bucket = new Bucket();
                        bucket.setId(bucketName);
                        return bucket;
                    } else {
                        return null;
                    }
                }
            };
        }
    }

    class MockObjects extends Objects {

        @Override
        public Get get(String getBucket, String getObject) {
            return new Get(getBucket, getObject) {
                @Override
                public StorageObject execute() throws IOException {
                    if (bucketName.equals(getBucket()) == false) {
                        throw newBucketNotFoundException(getBucket());
                    }
                    if (blobs.containsKey(getObject()) == false) {
                        throw newObjectNotFoundException(getObject());
                    }

                    final StorageObject storageObject = new StorageObject();
                    storageObject.setId(getObject());
                    return storageObject;
                }

                @Override
                public InputStream executeMediaAsInputStream() throws IOException {
                    if (bucketName.equals(getBucket()) == false) {
                        throw newBucketNotFoundException(getBucket());
                    }
                    if (blobs.containsKey(getObject()) == false) {
                        throw newObjectNotFoundException(getObject());
                    }
                    return new ByteArrayInputStream(blobs.get(getObject()));
                }
            };
        }

        @Override
        public Insert insert(String insertBucket, StorageObject insertObject, AbstractInputStreamContent insertStream) {
            return new Insert(insertBucket, insertObject) {
                @Override
                public StorageObject execute() throws IOException {
                    if (bucketName.equals(getBucket()) == false) {
                        throw newBucketNotFoundException(getBucket());
                    }

                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    Streams.copy(insertStream.getInputStream(), out);
                    blobs.put(getName(), out.toByteArray());
                    return null;
                }
            };
        }

        @Override
        public List list(String listBucket) {
            return new List(listBucket) {
                @Override
                public com.google.api.services.storage.model.Objects execute() throws IOException {
                    if (bucketName.equals(getBucket()) == false) {
                        throw newBucketNotFoundException(getBucket());
                    }

                    final com.google.api.services.storage.model.Objects objects = new com.google.api.services.storage.model.Objects();

                    final java.util.List<StorageObject> storageObjects = new ArrayList<>();
                    for (final Entry<String, byte[]> blob : blobs.entrySet()) {
                        if ((getPrefix() == null) || blob.getKey().startsWith(getPrefix())) {
                            final StorageObject storageObject = new StorageObject();
                            storageObject.setId(blob.getKey());
                            storageObject.setName(blob.getKey());
                            storageObject.setSize(BigInteger.valueOf((long) blob.getValue().length));
                            storageObjects.add(storageObject);
                        }
                    }

                    objects.setItems(storageObjects);
                    return objects;
                }
            };
        }

        @Override
        public Delete delete(String deleteBucket, String deleteObject) {
            return new Delete(deleteBucket, deleteObject) {
                @Override
                public Void execute() throws IOException {
                    if (bucketName.equals(getBucket()) == false) {
                        throw newBucketNotFoundException(getBucket());
                    }

                    if (blobs.containsKey(getObject()) == false) {
                        throw newObjectNotFoundException(getObject());
                    }

                    blobs.remove(getObject());
                    return null;
                }

                @Override
                public HttpRequest buildHttpRequest() throws IOException {
                    final HttpRequest httpRequest = super.buildHttpRequest();
                    httpRequest.getHeaders().put(DELETION_HEADER, getObject());
                    return httpRequest;
                }
            };
        }

        @Override
        public Copy copy(String srcBucket, String srcObject, String destBucket, String destObject, StorageObject content) {
            return new Copy(srcBucket, srcObject, destBucket, destObject, content) {
                @Override
                public StorageObject execute() throws IOException {
                    if (bucketName.equals(getSourceBucket()) == false) {
                        throw newBucketNotFoundException(getSourceBucket());
                    }
                    if (bucketName.equals(getDestinationBucket()) == false) {
                        throw newBucketNotFoundException(getDestinationBucket());
                    }

                    final byte[] bytes = blobs.get(getSourceObject());
                    if (bytes == null) {
                        throw newObjectNotFoundException(getSourceObject());
                    }
                    blobs.put(getDestinationObject(), bytes);

                    final StorageObject storageObject = new StorageObject();
                    storageObject.setId(getDestinationObject());
                    return storageObject;
                }
            };
        }
    }

    private static GoogleJsonResponseException newBucketNotFoundException(final String bucket) {
        final HttpResponseException.Builder builder = new HttpResponseException.Builder(404, "Bucket not found: " + bucket, new HttpHeaders());
        return new GoogleJsonResponseException(builder, new GoogleJsonError());
    }

    private static GoogleJsonResponseException newObjectNotFoundException(final String object) {
        final HttpResponseException.Builder builder = new HttpResponseException.Builder(404, "Object not found: " + object, new HttpHeaders());
        return new GoogleJsonResponseException(builder, new GoogleJsonError());
    }

    /**
     * {@link MockedHttpTransport} extends the existing testing transport to analyze the content
     * of {@link com.google.api.client.googleapis.batch.BatchRequest} and delete the appropriates
     * blobs. We use this because {@link Storage#batch()} is final and there is no other way to
     * extend batch requests for testing purposes.
     */
    static class MockedHttpTransport extends MockHttpTransport {

        private final ConcurrentMap<String, byte[]> blobs;

        MockedHttpTransport(final ConcurrentMap<String, byte[]> blobs) {
            this.blobs = blobs;
        }

        @Override
        public LowLevelHttpRequest buildRequest(final String method, final String url) throws IOException {
            // We analyze the content of the Batch request to detect our custom HTTP header,
            // and extract from it the name of the blob to delete. Then we reply a simple
            // batch response so that the client parser is happy.
            //
            // See https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch for the
            // format of the batch request body.
            if (HttpMethods.POST.equals(method) && url.endsWith("/batch")) {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() throws IOException {
                        final String contentType = new MultipartContent().getType();

                        final StringBuilder builder = new StringBuilder();
                        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                            getStreamingContent().writeTo(out);

                            Streams.readAllLines(new ByteArrayInputStream(out.toByteArray()), line -> {
                                if ((line != null) && line.startsWith(DELETION_HEADER)) {
                                    builder.append("--__END_OF_PART__\r\n");
                                    builder.append("Content-Type: application/http").append("\r\n");
                                    builder.append("\r\n");
                                    builder.append("HTTP/1.1 ");

                                    final String blobName = line.substring(line.indexOf(':') + 1).trim();
                                    if (blobs.containsKey(blobName)) {
                                        builder.append(RestStatus.OK.getStatus());
                                        blobs.remove(blobName);
                                    } else {
                                        builder.append(RestStatus.NOT_FOUND.getStatus());
                                    }
                                    builder.append("\r\n");
                                    builder.append("Content-Type: application/json; charset=UTF-8").append("\r\n");
                                    builder.append("Content-Length: 0").append("\r\n");
                                    builder.append("\r\n");
                                }
                            });
                            builder.append("\r\n");
                            builder.append("--__END_OF_PART__--");
                        }

                        final MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                        response.setStatusCode(200);
                        response.setContent(builder.toString());
                        response.setContentType(contentType);
                        return response;
                    }
                };
            } else {
                return super.buildRequest(method, url);
            }
        }
    }
}

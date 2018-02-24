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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * {@link GoogleCloudStorageTestServer} emulates a Google Cloud Storage service through a {@link #handle(String, String, byte[])} method
 * that provides appropriate responses for specific requests like the real Google Cloud platform would do. It is largely based on official
 * documentation available at https://cloud.google.com/storage/docs/json_api/v1/.
 */
public class GoogleCloudStorageTestServer {

    private static byte[] EMPTY_BYTE = new byte[0];

    /** List of the buckets stored on this test server **/
    private final Map<String, Bucket> buckets = ConcurrentCollections.newConcurrentMap();

    /** Request handlers for the requests made by the Google Cloud Storage client **/
    private final PathTrie<RequestHandler> handlers;

    /**
     * Creates a {@link GoogleCloudStorageTestServer} with the default endpoint
     */
    GoogleCloudStorageTestServer() {
        this("https://www.googleapis.com", true);
    }

    /**
     * Creates a {@link GoogleCloudStorageTestServer} with a custom endpoint,
     * potentially prefixing the URL patterns to match with the endpoint name.
     */
    GoogleCloudStorageTestServer(final String endpoint, final boolean prefixWithEndpoint) {
        this.handlers = defaultHandlers(endpoint, prefixWithEndpoint, buckets);
    }

    /** Creates a bucket in the test server **/
    void createBucket(final String bucketName) {
        buckets.put(bucketName, new Bucket(bucketName));
    }

    public Response handle(final String method, final String url, byte[] content) throws IOException {
        final Map<String, String> params = new HashMap<>();

        // Splits the URL to extract query string parameters
        final String rawPath;
        int questionMark = url.indexOf('?');
        if (questionMark != -1) {
            rawPath = url.substring(0, questionMark);
            RestUtils.decodeQueryString(url, questionMark + 1, params);
        } else {
            rawPath = url;
        }

        final RequestHandler handler = handlers.retrieve(method + " " + rawPath, params);
        if (handler != null) {
            return handler.execute(url, params, content);
        } else {
            return newError(RestStatus.INTERNAL_SERVER_ERROR, "No handler defined for request [method: " + method + ", url: " + url + "]");
        }
    }

    @FunctionalInterface
    interface RequestHandler {

        /**
         * Simulates the execution of a Storage request and returns a corresponding response.
         *
         * @param url the request URL
         * @param params the request URL parameters
         * @param body the request body provided as a byte array
         * @return the corresponding response
         *
         * @throws IOException if something goes wrong
         */
        Response execute(String url, Map<String, String> params, byte[] body) throws IOException;
    }

    /** Builds the default request handlers **/
    private static PathTrie<RequestHandler> defaultHandlers(final String endpoint,
                                                            final boolean prefixWithEndpoint,
                                                            final Map<String, Bucket> buckets) {

        final PathTrie<RequestHandler> handlers = new PathTrie<>(RestUtils.REST_DECODER);
        final String prefix = prefixWithEndpoint ? endpoint :  "";

        // GET Bucket
        //
        // https://cloud.google.com/storage/docs/json_api/v1/buckets/get
        handlers.insert("GET " + prefix + "/storage/v1/b/{bucket}", (url, params, body) -> {
            String name = params.get("bucket");
            if (Strings.hasText(name) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "bucket name is missing");
            }

            if (buckets.containsKey(name)) {
                return newResponse(RestStatus.OK, emptyMap(), buildBucketResource(name));
            } else {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }
        });

        // GET Object
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/get
        handlers.insert("GET " + prefix + "/storage/v1/b/{bucket}/o/{object}", (url, params, body) -> {
            String objectName = params.get("object");
            if (Strings.hasText(objectName) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            final Bucket bucket = buckets.get(params.get("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            for (Map.Entry<String, byte[]> object : bucket.objects.entrySet()) {
                if (object.getKey().equals(objectName)) {
                    return newResponse(RestStatus.OK, emptyMap(), buildObjectResource(bucket.name, objectName, object.getValue()));
                }
            }
            return newError(RestStatus.NOT_FOUND, "object not found");
        });

        // Delete Object
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/delete
        handlers.insert("DELETE " + prefix + "/storage/v1/b/{bucket}/o/{object}", (url, params, body) -> {
            String objectName = params.get("object");
            if (Strings.hasText(objectName) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            final Bucket bucket = buckets.get(params.get("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            final byte[] bytes = bucket.objects.remove(objectName);
            if (bytes != null) {
                return new Response(RestStatus.NO_CONTENT, emptyMap(), XContentType.JSON.mediaType(), EMPTY_BYTE);
            }
            return newError(RestStatus.NOT_FOUND, "object not found");
        });

        // Insert Object (initialization)
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/insert
        handlers.insert("POST " + prefix + "/upload/storage/v1/b/{bucket}/o", (url, params, body) -> {
            if ("resumable".equals(params.get("uploadType")) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "upload type must be resumable");
            }

            final String objectName = params.get("name");
            if (Strings.hasText(objectName) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            final Bucket bucket = buckets.get(params.get("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            if (bucket.objects.put(objectName, EMPTY_BYTE) == null) {
                String location = endpoint + "/upload/storage/v1/b/" + bucket.name + "/o?uploadType=resumable&upload_id=" + objectName;
                return new Response(RestStatus.CREATED, singletonMap("Location", location), XContentType.JSON.mediaType(), EMPTY_BYTE);
            } else {
                return newError(RestStatus.CONFLICT, "object already exist");
            }
        });

        // Insert Object (upload)
        //
        // https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
        handlers.insert("PUT " + prefix + "/upload/storage/v1/b/{bucket}/o", (url, params, body) -> {
            String objectId = params.get("upload_id");
            if (Strings.hasText(objectId) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "upload id is missing");
            }

            final Bucket bucket = buckets.get(params.get("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            if (bucket.objects.containsKey(objectId) == false) {
                return newError(RestStatus.NOT_FOUND, "object name not found");
            }

            bucket.objects.put(objectId, body);
            return newResponse(RestStatus.OK, emptyMap(), buildObjectResource(bucket.name, objectId, body));
        });

        // Copy Object
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/copy
        handlers.insert("POST " + prefix + "/storage/v1/b/{srcBucket}/o/{src}/copyTo/b/{destBucket}/o/{dest}", (url, params, body) -> {
            String source = params.get("src");
            if (Strings.hasText(source) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "source object name is missing");
            }

            final Bucket srcBucket = buckets.get(params.get("srcBucket"));
            if (srcBucket == null) {
                return newError(RestStatus.NOT_FOUND, "source bucket not found");
            }

            String dest = params.get("dest");
            if (Strings.hasText(dest) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "destination object name is missing");
            }

            final Bucket destBucket = buckets.get(params.get("destBucket"));
            if (destBucket == null) {
                return newError(RestStatus.NOT_FOUND, "destination bucket not found");
            }

            final byte[] sourceBytes = srcBucket.objects.get(source);
            if (sourceBytes == null) {
                return newError(RestStatus.NOT_FOUND, "source object not found");
            }

            destBucket.objects.put(dest, sourceBytes);
            return newResponse(RestStatus.OK, emptyMap(), buildObjectResource(destBucket.name, dest, sourceBytes));
        });

        // List Objects
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/list
        handlers.insert("GET " + prefix + "/storage/v1/b/{bucket}/o", (url, params, body) -> {
            final Bucket bucket = buckets.get(params.get("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            builder.field("kind", "storage#objects");
            {
                builder.startArray("items");

                final String prefixParam = params.get("prefix");
                for (Map.Entry<String, byte[]> object : bucket.objects.entrySet()) {
                    if (prefixParam != null && object.getKey().startsWith(prefixParam) == false) {
                        continue;
                    }
                    buildObjectResource(builder, bucket.name, object.getKey(), object.getValue());
                }
                builder.endArray();
            }
            builder.endObject();
            return newResponse(RestStatus.OK, emptyMap(), builder);
        });

        // Download Object
        //
        // https://cloud.google.com/storage/docs/request-body
        handlers.insert("GET " + prefix + "/download/storage/v1/b/{bucket}/o/{object}", (url, params, body) -> {
            String object = params.get("object");
            if (Strings.hasText(object) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "object id is missing");
            }

            final Bucket bucket = buckets.get(params.get("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            if (bucket.objects.containsKey(object) == false) {
                return newError(RestStatus.NOT_FOUND, "object name not found");
            }

            return new Response(RestStatus.OK, emptyMap(), "application/octet-stream", bucket.objects.get(object));
        });

        // Batch
        //
        // https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
        handlers.insert("POST " + prefix + "/batch", (url, params, req) -> {
            final List<Response> batchedResponses = new ArrayList<>();

            // A batch request body looks like this:
            //
            //            --__END_OF_PART__
            //            Content-Length: 71
            //            Content-Type: application/http
            //            content-id: 1
            //            content-transfer-encoding: binary
            //
            //            DELETE https://www.googleapis.com/storage/v1/b/ohifkgu/o/foo%2Ftest HTTP/1.1
            //
            //
            //            --__END_OF_PART__
            //            Content-Length: 71
            //            Content-Type: application/http
            //            content-id: 2
            //            content-transfer-encoding: binary
            //
            //            DELETE https://www.googleapis.com/storage/v1/b/ohifkgu/o/bar%2Ftest HTTP/1.1
            //
            //
            //            --__END_OF_PART__--

            // Here we simply process the request body line by line and delegate to other handlers
            // if possible.
            Streams.readAllLines(new BufferedInputStream(new ByteArrayInputStream(req)), line -> {
                final int indexOfHttp = line.indexOf(" HTTP/1.1");
                if (indexOfHttp > 0) {
                    line = line.substring(0, indexOfHttp);
                }

                RequestHandler handler = handlers.retrieve(line, params);
                if (handler != null) {
                    try {
                        batchedResponses.add(handler.execute(line, params, req));
                    } catch (IOException e) {
                        batchedResponses.add(newError(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                    }
                }
            });

            // Now we can build the response
            String boundary = "__END_OF_PART__";
            String sep = "--";
            String line = "\r\n";

            StringBuilder builder = new StringBuilder();
            for (Response response : batchedResponses) {
                builder.append(sep).append(boundary).append(line);
                builder.append(line);
                builder.append("HTTP/1.1 ").append(response.status.getStatus());
                builder.append(' ').append(response.status.toString());
                builder.append(line);
                builder.append("Content-Length: ").append(response.body.length).append(line);
                builder.append(line);
            }
            builder.append(line);
            builder.append(sep).append(boundary).append(sep);

            byte[] content = builder.toString().getBytes(StandardCharsets.UTF_8);
            return new Response(RestStatus.OK, emptyMap(), "multipart/mixed; boundary=" + boundary, content);
        });

        return handlers;
    }

    /**
     * Represents a Storage bucket as if it was created on Google Cloud Storage.
     */
    static class Bucket {

        /** Bucket name **/
        final String name;

        /** Blobs contained in the bucket **/
        final Map<String, byte[]> objects;

        Bucket(final String name) {
            this.name = Objects.requireNonNull(name);
            this.objects = ConcurrentCollections.newConcurrentMap();
        }
    }

    /**
     * Represents a Storage HTTP Response.
     */
    static class Response {

        final RestStatus status;
        final Map<String, String> headers;
        final String contentType;
        final byte[] body;

        Response(final RestStatus status, final Map<String, String> headers, final String contentType, final byte[] body) {
            this.status = Objects.requireNonNull(status);
            this.headers = Objects.requireNonNull(headers);
            this.contentType = Objects.requireNonNull(contentType);
            this.body = Objects.requireNonNull(body);
        }
    }

    /**
     * Builds a JSON response
     */
    private static Response newResponse(final RestStatus status, final Map<String, String> headers, final XContentBuilder xContentBuilder) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            xContentBuilder.bytes().writeTo(out);
            return new Response(status, headers, XContentType.JSON.mediaType(), out.toByteArray());
        } catch (IOException e) {
            return newError(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    /**
     * Storage Error JSON representation
     */
    private static Response newError(final RestStatus status, final String message) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject()
                            .startObject("error")
                                .field("code", status.getStatus())
                                .field("message", message)
                                .startArray("errors")
                                    .startObject()
                                        .field("domain", "global")
                                        .field("reason", status.toString())
                                        .field("message", message)
                                    .endObject()
                                .endArray()
                            .endObject()
                        .endObject();
                builder.bytes().writeTo(out);
            }
            return new Response(status, emptyMap(), XContentType.JSON.mediaType(), out.toByteArray());
        } catch (IOException e) {
            byte[] bytes = (message != null ? message : "something went wrong").getBytes(StandardCharsets.UTF_8);
            return new Response(RestStatus.INTERNAL_SERVER_ERROR, emptyMap(), " text/plain", bytes);
        }
    }

    /**
     * Storage Bucket JSON representation as defined in
     * https://cloud.google.com/storage/docs/json_api/v1/bucket#resource
     */
    private static XContentBuilder buildBucketResource(final String name) throws IOException {
        return jsonBuilder().startObject()
                                .field("kind", "storage#bucket")
                                .field("id", name)
                            .endObject();
    }

    /**
     * Storage Object JSON representation as defined in
     * https://cloud.google.com/storage/docs/json_api/v1/objects#resource
     */
    private static XContentBuilder buildObjectResource(final String bucket, final String name, final byte[] bytes)
            throws IOException {
        return buildObjectResource(jsonBuilder(), bucket, name, bytes);
    }

    /**
     * Storage Object JSON representation as defined in
     * https://cloud.google.com/storage/docs/json_api/v1/objects#resource
     */
    private static XContentBuilder buildObjectResource(final XContentBuilder builder,
                                                       final String bucket,
                                                       final String name,
                                                       final byte[] bytes) throws IOException {
        return builder.startObject()
                            .field("kind", "storage#object")
                            .field("id", String.join("/", bucket, name))
                            .field("name", name)
                            .field("size", String.valueOf(bytes.length))
                        .endObject();
    }
}

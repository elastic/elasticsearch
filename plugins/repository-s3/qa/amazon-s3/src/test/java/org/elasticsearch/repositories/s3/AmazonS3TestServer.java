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
package org.elasticsearch.repositories.s3;

import com.amazonaws.util.DateUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * {@link AmazonS3TestServer} emulates a S3 service through a {@link #handle(String, String, String, Map, byte[])}
 * method that provides appropriate responses for specific requests like the real S3 platform would do.
 * It is largely based on official documentation available at https://docs.aws.amazon.com/AmazonS3/latest/API/.
 */
public class AmazonS3TestServer {

    private static byte[] EMPTY_BYTE = new byte[0];
    /** List of the buckets stored on this test server **/
    private final Map<String, Bucket> buckets = ConcurrentCollections.newConcurrentMap();

    /** Request handlers for the requests made by the S3 client **/
    private final PathTrie<RequestHandler> handlers;

    /** Server endpoint **/
    private final String endpoint;

    /** Increments for the requests ids **/
    private final AtomicLong requests = new AtomicLong(0);

    /**
     * Creates a {@link AmazonS3TestServer} with a custom endpoint
     */
    AmazonS3TestServer(final String endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        this.handlers = defaultHandlers(endpoint, buckets);
    }

    /** Creates a bucket in the test server **/
    void createBucket(final String bucketName) {
        buckets.put(bucketName, new Bucket(bucketName));
    }

    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Returns a response for the given request
     *
     * @param method  the HTTP method of the request
     * @param path    the path of the URL of the request
     * @param query   the queryString of the URL of request
     * @param headers the HTTP headers of the request
     * @param body    the HTTP request body
     * @return a {@link Response}
     * @throws IOException if something goes wrong
     */
    public Response handle(final String method,
                           final String path,
                           final String query,
                           final Map<String, List<String>> headers,
                           byte[] body) throws IOException {

        final long requestId = requests.incrementAndGet();

        final Map<String, String> params = new HashMap<>();
        if (query != null) {
            RestUtils.decodeQueryString(query, 0, params);
        }

        final List<String> authorizations = headers.get("Authorization");
        if (authorizations == null
            || (authorizations.isEmpty() == false & authorizations.get(0).contains("s3_integration_test_access_key") == false)) {
            return newError(requestId, RestStatus.FORBIDDEN, "AccessDenied", "Access Denied", "");
        }

        final RequestHandler handler = handlers.retrieve(method + " " + path, params);
        if (handler != null) {
            return handler.execute(params, headers, body, requestId);
        } else {
            return newInternalError(requestId, "No handler defined for request [method: " + method + ", path: " + path + "]");
        }
    }

    @FunctionalInterface
    interface RequestHandler {

        /**
         * Simulates the execution of a S3 request and returns a corresponding response.
         *
         * @param params the request's query string parameters
         * @param headers the request's headers
         * @param body the request body provided as a byte array
         * @param requestId a unique id for the incoming request
         * @return the corresponding response
         *
         * @throws IOException if something goes wrong
         */
        Response execute(Map<String, String> params, Map<String, List<String>> headers, byte[] body, long requestId) throws IOException;
    }

    /** Builds the default request handlers **/
    private static PathTrie<RequestHandler> defaultHandlers(final String endpoint, final Map<String, Bucket> buckets) {
        final PathTrie<RequestHandler> handlers = new PathTrie<>(RestUtils.REST_DECODER);

        // HEAD Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
        objectsPaths("HEAD " + endpoint + "/{bucket}").forEach(path ->
            handlers.insert(path, (params, headers, body, id) -> {
                final String bucketName = params.get("bucket");

                final Bucket bucket = buckets.get(bucketName);
                if (bucket == null) {
                    return newBucketNotFoundError(id, bucketName);
                }

                final String objectName = objectName(params);
                for (Map.Entry<String, byte[]> object : bucket.objects.entrySet()) {
                    if (object.getKey().equals(objectName)) {
                        return new Response(RestStatus.OK, emptyMap(), "text/plain", EMPTY_BYTE);
                    }
                }
                return newObjectNotFoundError(id, objectName);
            })
        );

        // PUT Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
        objectsPaths("PUT " + endpoint + "/{bucket}").forEach(path ->
            handlers.insert(path, (params, headers, body, id) -> {
                final String destBucketName = params.get("bucket");

                final Bucket destBucket = buckets.get(destBucketName);
                if (destBucket == null) {
                    return newBucketNotFoundError(id, destBucketName);
                }

                final String destObjectName = objectName(params);

                // This is a chunked upload request. We should have the header "Content-Encoding : aws-chunked,gzip"
                // to detect it but it seems that the AWS SDK does not follow the S3 guidelines here.
                //
                // See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
                //
                List<String> headerDecodedContentLength = headers.getOrDefault("X-amz-decoded-content-length", emptyList());
                if (headerDecodedContentLength.size() == 1) {
                    int contentLength = Integer.valueOf(headerDecodedContentLength.get(0));

                    // Chunked requests have a payload like this:
                    //
                    // 105;chunk-signature=01d0de6be013115a7f4794db8c4b9414e6ec71262cc33ae562a71f2eaed1efe8
                    // ...  bytes of data ....
                    // 0;chunk-signature=f890420b1974c5469aaf2112e9e6f2e0334929fd45909e03c0eff7a84124f6a4
                    //
                    try (BufferedInputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(body))) {
                        int b;
                        // Moves to the end of the first signature line
                        while ((b = inputStream.read()) != -1) {
                            if (b == '\n') {
                                break;
                            }
                        }

                        final byte[] bytes = new byte[contentLength];
                        inputStream.read(bytes, 0, contentLength);

                        destBucket.objects.put(destObjectName, bytes);
                        return new Response(RestStatus.OK, emptyMap(), "text/plain", EMPTY_BYTE);
                    }
                }

                return newInternalError(id, "Something is wrong with this PUT request");
            })
        );

        // DELETE Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
        objectsPaths("DELETE " + endpoint + "/{bucket}").forEach(path ->
            handlers.insert(path, (params, headers, body, id) -> {
                final String bucketName = params.get("bucket");

                final Bucket bucket = buckets.get(bucketName);
                if (bucket == null) {
                    return newBucketNotFoundError(id, bucketName);
                }

                final String objectName = objectName(params);
                if (bucket.objects.remove(objectName) != null) {
                    return new Response(RestStatus.OK, emptyMap(), "text/plain", EMPTY_BYTE);
                }
                return newObjectNotFoundError(id, objectName);
            })
        );

        // GET Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
        objectsPaths("GET " + endpoint + "/{bucket}").forEach(path ->
            handlers.insert(path, (params, headers, body, id) -> {
                final String bucketName = params.get("bucket");

                final Bucket bucket = buckets.get(bucketName);
                if (bucket == null) {
                    return newBucketNotFoundError(id, bucketName);
                }

                final String objectName = objectName(params);
                if (bucket.objects.containsKey(objectName)) {
                    return new Response(RestStatus.OK, emptyMap(), "application/octet-stream", bucket.objects.get(objectName));

                }
                return newObjectNotFoundError(id, objectName);
            })
        );

        // HEAD Bucket
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
        handlers.insert("HEAD " + endpoint + "/{bucket}", (params, headers, body, id) -> {
            String bucket = params.get("bucket");
            if (Strings.hasText(bucket) && buckets.containsKey(bucket)) {
                return new Response(RestStatus.OK, emptyMap(), "text/plain", EMPTY_BYTE);
            } else {
                return newBucketNotFoundError(id, bucket);
            }
        });

        // GET Bucket (List Objects) Version 1
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
        handlers.insert("GET " + endpoint + "/{bucket}/", (params, headers, body, id) -> {
            final String bucketName = params.get("bucket");

            final Bucket bucket = buckets.get(bucketName);
            if (bucket == null) {
                return newBucketNotFoundError(id, bucketName);
            }

            String prefix = params.get("prefix");
            if (prefix == null) {
                List<String> prefixes = headers.get("Prefix");
                if (prefixes != null && prefixes.size() == 1) {
                    prefix = prefixes.get(0);
                }
            }
            return newListBucketResultResponse(id, bucket, prefix);
        });

        // Delete Multiple Objects
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
        handlers.insert("POST " + endpoint + "/", (params, headers, body, id) -> {
            final List<String> deletes = new ArrayList<>();
            final List<String> errors = new ArrayList<>();

            if (params.containsKey("delete")) {
                // The request body is something like:
                // <Delete><Object><Key>...</Key></Object><Object><Key>...</Key></Object></Delete>
                String request = Streams.copyToString(new InputStreamReader(new ByteArrayInputStream(body), StandardCharsets.UTF_8));
                if (request.startsWith("<Delete>")) {
                    final String startMarker = "<Key>";
                    final String endMarker = "</Key>";

                    int offset = 0;
                    while (offset != -1) {
                        offset = request.indexOf(startMarker, offset);
                        if (offset > 0) {
                            int closingOffset = request.indexOf(endMarker, offset);
                            if (closingOffset != -1) {
                                offset = offset + startMarker.length();
                                final String objectName = request.substring(offset, closingOffset);

                                boolean found = false;
                                for (Bucket bucket : buckets.values()) {
                                    if (bucket.objects.remove(objectName) != null) {
                                        found = true;
                                    }
                                }

                                if (found) {
                                    deletes.add(objectName);
                                } else {
                                    errors.add(objectName);
                                }
                            }
                        }
                    }
                    return newDeleteResultResponse(id, deletes, errors);
                }
            }
            return newInternalError(id, "Something is wrong with this POST multiple deletes request");
        });

        return handlers;
    }

    /**
     * Represents a S3 bucket.
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
     * Represents a HTTP Response.
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
     * Decline a path like "http://host:port/{bucket}" into 10 derived paths like:
     * - http://host:port/{bucket}/{path0}
     * - http://host:port/{bucket}/{path0}/{path1}
     * - http://host:port/{bucket}/{path0}/{path1}/{path2}
     * - etc
     */
    private static List<String> objectsPaths(final String path) {
        final List<String> paths = new ArrayList<>();
        String p = path;
        for (int i = 0; i < 10; i++) {
            p = p + "/{path" + i + "}";
            paths.add(p);
        }
        return paths;
    }

    /**
     * Retrieves the object name from all derives paths named {pathX} where 0 <= X < 10.
     *
     * This is the counterpart of {@link #objectsPaths(String)}
     */
    private static String objectName(final Map<String, String> params) {
        final StringBuilder name = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            String value = params.getOrDefault("path" + i, null);
            if (value != null) {
                if (name.length() > 0) {
                    name.append('/');
                }
                name.append(value);
            }
        }
        return name.toString();
    }

    /**
     * S3 ListBucketResult Response
     */
    private static Response newListBucketResultResponse(final long requestId, final Bucket bucket, final String prefix) {
        final String id = Long.toString(requestId);
        final StringBuilder response = new StringBuilder();
        response.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        response.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        response.append("<Prefix>");
        if (prefix != null) {
            response.append(prefix);
        }
        response.append("</Prefix>");
        response.append("<Marker/>");
        response.append("<MaxKeys>1000</MaxKeys>");
        response.append("<IsTruncated>false</IsTruncated>");

        int count = 0;
        for (Map.Entry<String, byte[]> object : bucket.objects.entrySet()) {
            String objectName = object.getKey();
            if (prefix == null || objectName.startsWith(prefix)) {
                response.append("<Contents>");
                response.append("<Key>").append(objectName).append("</Key>");
                response.append("<LastModified>").append(DateUtils.formatISO8601Date(new Date())).append("</LastModified>");
                response.append("<ETag>&quot;").append(count++).append("&quot;</ETag>");
                response.append("<Size>").append(object.getValue().length).append("</Size>");
                response.append("</Contents>");
            }
        }
        response.append("</ListBucketResult>");
        return new Response(RestStatus.OK, singletonMap("x-amz-request-id", id), "application/xml", response.toString().getBytes(UTF_8));
    }

    /**
     * S3 DeleteResult Response
     */
    private static Response newDeleteResultResponse(final long requestId,
                                                    final List<String> deletedObjects,
                                                    final List<String> ignoredObjects) {
        final String id = Long.toString(requestId);

        final StringBuilder response = new StringBuilder();
        response.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        response.append("<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        for (String deletedObject : deletedObjects) {
            response.append("<Deleted>");
            response.append("<Key>").append(deletedObject).append("</Key>");
            response.append("</Deleted>");
        }
        for (String ignoredObject : ignoredObjects) {
            response.append("<Error>");
            response.append("<Key>").append(ignoredObject).append("</Key>");
            response.append("<Code>NoSuchKey</Code>");
            response.append("</Error>");
        }
        response.append("</DeleteResult>");
        return new Response(RestStatus.OK, singletonMap("x-amz-request-id", id), "application/xml", response.toString().getBytes(UTF_8));
    }

    private static Response newBucketNotFoundError(final long requestId, final String bucket) {
        return newError(requestId, RestStatus.NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist", bucket);
    }

    private static Response newObjectNotFoundError(final long requestId, final String object) {
        return newError(requestId, RestStatus.NOT_FOUND, "NoSuchKey", "The specified key does not exist", object);
    }

    private static Response newInternalError(final long requestId, final String resource) {
        return newError(requestId, RestStatus.INTERNAL_SERVER_ERROR, "InternalError", "We encountered an internal error", resource);
    }

    /**
     * S3 Error
     *
     * https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
     */
    private static Response newError(final long requestId,
                                     final RestStatus status,
                                     final String code,
                                     final String message,
                                     final String resource) {
        final String id = Long.toString(requestId);
        final StringBuilder response = new StringBuilder();
        response.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        response.append("<Error>");
        response.append("<Code>").append(code).append("</Code>");
        response.append("<Message>").append(message).append("</Message>");
        response.append("<Resource>").append(resource).append("</Resource>");
        response.append("<RequestId>").append(id).append("</RequestId>");
        response.append("</Error>");
        return new Response(status, singletonMap("x-amz-request-id", id), "application/xml", response.toString().getBytes(UTF_8));
    }
}

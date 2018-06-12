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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * {@link GoogleCloudStorageTestServer} emulates a Google Cloud Storage service through
 * a {@link #handle(String, String, String, Map, byte[])} method that provides appropriate
 * responses for specific requests like the real Google Cloud platform would do.
 * It is largely based on official documentation available at https://cloud.google.com/storage/docs/json_api/v1/.
 */
public class GoogleCloudStorageTestServer {

    private static final byte[] EMPTY_BYTE = new byte[0];

    /** List of the buckets stored on this test server **/
    private final Map<String, Bucket> buckets = ConcurrentCollections.newConcurrentMap();

    /** Request handlers for the requests made by the Google Cloud Storage client **/
    private final PathTrie<RequestHandler> handlers;

    /** Server endpoint **/
    private final String endpoint;

    /**
     * Creates a {@link GoogleCloudStorageTestServer} with a custom endpoint
     */
    GoogleCloudStorageTestServer(final String endpoint) {
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
     * Returns a Google Cloud Storage response for the given request
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

        final Map<String, String> params = new HashMap<>();
        if (query != null) {
            RestUtils.decodeQueryString(query, 0, params);
        }

        final RequestHandler handler = handlers.retrieve(method + " " + path, params);
        if (handler != null) {
            return handler.execute(params, headers, body);
        } else {
            return newError(RestStatus.INTERNAL_SERVER_ERROR,
                "No handler defined for request [method: " + method + ", path: " + path + "]");
        }
    }

    @FunctionalInterface
    interface RequestHandler {

        /**
         * Simulates the execution of a Storage request and returns a corresponding response.
         *
         * @param params the request's query string parameters
         * @param headers the request's headers
         * @param body the request body provided as a byte array
         * @return the corresponding response
         *
         * @throws IOException if something goes wrong
         */
        Response execute(Map<String, String> params, Map<String, List<String>> headers, byte[] body) throws IOException;
    }

    /** Builds the default request handlers **/
    private static PathTrie<RequestHandler> defaultHandlers(final String endpoint, final Map<String, Bucket> buckets) {
        final PathTrie<RequestHandler> handlers = new PathTrie<>(RestUtils.REST_DECODER);

        // GET Bucket
        //
        // https://cloud.google.com/storage/docs/json_api/v1/buckets/get
        handlers.insert("GET " + endpoint + "/storage/v1/b/{bucket}", (params, headers, body) -> {
            final String name = params.get("bucket");
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
        handlers.insert("GET " + endpoint + "/storage/v1/b/{bucket}/o/{object}", (params, headers, body) -> {
            final String objectName = params.get("object");
            if (Strings.hasText(objectName) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            final Bucket bucket = buckets.get(params.get("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            for (final Map.Entry<String, byte[]> object : bucket.objects.entrySet()) {
                if (object.getKey().equals(objectName)) {
                    return newResponse(RestStatus.OK, emptyMap(), buildObjectResource(bucket.name, objectName, object.getValue()));
                }
            }
            return newError(RestStatus.NOT_FOUND, "object not found");
        });

        // Delete Object
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/delete
        handlers.insert("DELETE " + endpoint + "/storage/v1/b/{bucket}/o/{object}", (params, headers, body) -> {
            final String objectName = params.get("object");
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
        handlers.insert("POST " + endpoint + "/upload/storage/v1/b/{bucket}/o", (params, headers, body) -> {
            final String uploadType = params.get("uploadType");
            if ("resumable".equals(uploadType)) {
                final String objectName = params.get("name");
                if (Strings.hasText(objectName) == false) {
                    return newError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
                }
                final Bucket bucket = buckets.get(params.get("bucket"));
                if (bucket == null) {
                    return newError(RestStatus.NOT_FOUND, "bucket not found");
                }
                if (bucket.objects.putIfAbsent(objectName, EMPTY_BYTE) == null) {
                    final String location = endpoint + "/upload/storage/v1/b/" + bucket.name + "/o?uploadType=resumable&upload_id="
                            + objectName;
                    return new Response(RestStatus.CREATED, singletonMap("Location", location), XContentType.JSON.mediaType(), EMPTY_BYTE);
                } else {
                    return newError(RestStatus.CONFLICT, "object already exist");
                }
            } else if ("multipart".equals(uploadType)) {
                /*
                 *  A multipart/related request body looks like this (note the binary dump inside a text blob! nice!):
                 * --__END_OF_PART__
                 * Content-Length: 135
                 * Content-Type: application/json; charset=UTF-8
                 * content-transfer-encoding: binary
                 *
                 * {"bucket":"bucket_test","crc32c":"7XacHQ==","md5Hash":"fVztGkklMlUamsSmJK7W+w==",
                 * "name":"tests-KEwE3bU4TuyetBgQIghmUw/master.dat-temp"}
                 * --__END_OF_PART__
                 * content-transfer-encoding: binary
                 *
                 * KEwE3bU4TuyetBgQIghmUw
                 * --__END_OF_PART__--
                 */
                String boundary = "__END_OF_PART__";
                // Determine the multipart boundary
                final List<String> contentTypes = headers.getOrDefault("Content-Type", headers.get("Content-type"));
                if (contentTypes != null) {
                    final String contentType = contentTypes.get(0);
                    if ((contentType != null) && contentType.contains("multipart/related; boundary=")) {
                        boundary = contentType.replace("multipart/related; boundary=", "");
                    }
                }
                InputStream inputStreamBody = new ByteArrayInputStream(body);
                final List<String> contentEncodings = headers.getOrDefault("Content-Encoding", headers.get("Content-encoding"));
                if (contentEncodings != null) {
                    if (contentEncodings.stream().anyMatch(x -> "gzip".equalsIgnoreCase(x))) {
                        inputStreamBody = new GZIPInputStream(inputStreamBody);
                    }
                }
                // Read line by line ?both? parts of the multipart. Decoding headers as
                // IS_8859_1 is safe.
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStreamBody, StandardCharsets.ISO_8859_1))) {
                    String line;
                    // read first part delimiter
                    line = reader.readLine();
                    if ((line == null) || (line.equals("--" + boundary) == false)) {
                        return newError(RestStatus.INTERNAL_SERVER_ERROR,
                                "Error parsing multipart request. Does not start with the part delimiter.");
                    }
                    final Map<String, List<String>> firstPartHeaders = new HashMap<>();
                    // Reads the first part's headers, if any
                    while ((line = reader.readLine()) != null) {
                        if (line.equals("\r\n") || (line.length() == 0)) {
                            // end of headers
                            break;
                        } else {
                            final String[] header = line.split(":", 2);
                            firstPartHeaders.put(header[0], singletonList(header[1]));
                        }
                    }
                    final List<String> firstPartContentTypes = firstPartHeaders.getOrDefault("Content-Type",
                            firstPartHeaders.get("Content-type"));
                    if ((firstPartContentTypes == null)
                            || (firstPartContentTypes.stream().noneMatch(x -> x.contains("application/json")))) {
                        return newError(RestStatus.INTERNAL_SERVER_ERROR,
                                "Error parsing multipart request. Metadata part expected to have the \"application/json\" content type.");
                    }
                    // read metadata part, a single line
                    line = reader.readLine();
                    final byte[] metadata = line.getBytes(StandardCharsets.ISO_8859_1);
                    if ((firstPartContentTypes != null) && (firstPartContentTypes.stream().anyMatch((x -> x.contains("charset=utf-8"))))) {
                        // decode as utf-8
                        line = new String(metadata, StandardCharsets.UTF_8);
                    }
                    final Matcher objectNameMatcher = Pattern.compile("\"name\":\"([^\"]*)\"").matcher(line);
                    objectNameMatcher.find();
                    final String objectName = objectNameMatcher.group(1);
                    final Matcher bucketNameMatcher = Pattern.compile("\"bucket\":\"([^\"]*)\"").matcher(line);
                    bucketNameMatcher.find();
                    final String bucketName = bucketNameMatcher.group(1);
                    // read second part delimiter
                    line = reader.readLine();
                    if ((line == null) || (line.equals("--" + boundary) == false)) {
                        return newError(RestStatus.INTERNAL_SERVER_ERROR,
                                "Error parsing multipart request. Second part does not start with delimiter. "
                                        + "Is the metadata multi-line?");
                    }
                    final Map<String, List<String>> secondPartHeaders = new HashMap<>();
                    // Reads the second part's headers, if any
                    while ((line = reader.readLine()) != null) {
                        if (line.equals("\r\n") || (line.length() == 0)) {
                            // end of headers
                            break;
                        } else {
                            final String[] header = line.split(":", 2);
                            secondPartHeaders.put(header[0], singletonList(header[1]));
                        }
                    }
                    final List<String> secondPartTransferEncoding = secondPartHeaders.getOrDefault("Content-Transfer-Encoding",
                            secondPartHeaders.get("content-transfer-encoding"));
                    if ((secondPartTransferEncoding == null)
                            || (secondPartTransferEncoding.stream().noneMatch(x -> x.contains("binary")))) {
                        return newError(RestStatus.INTERNAL_SERVER_ERROR,
                                "Error parsing multipart request. Data part expected to have the \"binary\" content transfer encoding.");
                    }
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    int c;
                    while ((c = reader.read()) != -1) {
                        // one char to one byte, because of the ISO_8859_1 encoding
                        baos.write(c);
                    }
                    final byte[] temp = baos.toByteArray();
                    final byte[] trailingEnding = ("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.ISO_8859_1);
                    // check trailing
                    for (int i = trailingEnding.length - 1; i >= 0; i--) {
                        if (trailingEnding[i] != temp[(temp.length - trailingEnding.length) + i]) {
                            return newError(RestStatus.INTERNAL_SERVER_ERROR, "Error parsing multipart request.");
                        }
                    }
                    final Bucket bucket = buckets.get(bucketName);
                    if (bucket == null) {
                        return newError(RestStatus.NOT_FOUND, "bucket not found");
                    }
                    final byte[] objectData = Arrays.copyOf(temp, temp.length - trailingEnding.length);
                    if ((objectName != null) && (bucketName != null) && (objectData != null)) {
                        bucket.objects.put(objectName, objectData);
                        return new Response(RestStatus.OK, emptyMap(), XContentType.JSON.mediaType(), metadata);
                    } else {
                        return newError(RestStatus.INTERNAL_SERVER_ERROR, "error parsing multipart request");
                    }
                }
            } else {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "upload type must be resumable or multipart");
            }
        });

        // Insert Object (upload)
        //
        // https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
        handlers.insert("PUT " + endpoint + "/upload/storage/v1/b/{bucket}/o", (params, headers, body) -> {
            final String objectId = params.get("upload_id");
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

        // List Objects
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/list
        handlers.insert("GET " + endpoint + "/storage/v1/b/{bucket}/o", (params, headers, body) -> {
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
                for (final Map.Entry<String, byte[]> object : bucket.objects.entrySet()) {
                    if ((prefixParam != null) && (object.getKey().startsWith(prefixParam) == false)) {
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
        handlers.insert("GET " + endpoint + "/download/storage/v1/b/{bucket}/o/{object}", (params, headers, body) -> {
            final String object = params.get("object");
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
        handlers.insert("POST " + endpoint + "/batch/storage/v1", (params, headers, body) -> {
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

            // Default multipart boundary
            String boundary = "__END_OF_PART__";

            // Determine the multipart boundary
            final List<String> contentTypes = headers.getOrDefault("Content-Type", headers.get("Content-type"));
            if (contentTypes != null) {
                final String contentType = contentTypes.get(0);
                if ((contentType != null) && contentType.contains("multipart/mixed; boundary=")) {
                    boundary = contentType.replace("multipart/mixed; boundary=", "");
                }
            }

            // Read line by line the batched requests
            try (BufferedReader reader = new BufferedReader(
                                              new InputStreamReader(
                                                  new ByteArrayInputStream(body), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Start of a batched request
                    if (line.equals("--" + boundary)) {
                        final Map<String, List<String>> batchedHeaders = new HashMap<>();

                        // Reads the headers, if any
                        while ((line = reader.readLine()) != null) {
                            if (line.equals("\r\n") || (line.length() == 0)) {
                                // end of headers
                                break;
                            } else {
                                final String[] header = line.split(":", 2);
                                batchedHeaders.put(header[0], singletonList(header[1]));
                            }
                        }

                        // Reads the method and URL
                        line = reader.readLine();
                        final String batchedUrl = line.substring(0, line.lastIndexOf(' '));

                        final Map<String, String> batchedParams = new HashMap<>();
                        final int questionMark = batchedUrl.indexOf('?');
                        if (questionMark != -1) {
                            RestUtils.decodeQueryString(batchedUrl.substring(questionMark + 1), 0, batchedParams);
                        }

                        // Reads the body
                        line = reader.readLine();
                        byte[] batchedBody = new byte[0];
                        if ((line != null) || (line.startsWith("--" + boundary) == false)) {
                            batchedBody = line.getBytes(StandardCharsets.UTF_8);
                        }

                        // Executes the batched request
                        final RequestHandler handler = handlers.retrieve(batchedUrl, batchedParams);
                        if (handler != null) {
                            try {
                                batchedResponses.add(handler.execute(batchedParams, batchedHeaders, batchedBody));
                            } catch (final IOException e) {
                                batchedResponses.add(newError(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                            }
                        }
                    }
                }
            }

            // Now we can build the response
            final String sep = "--";
            final String line = "\r\n";

            final StringBuilder builder = new StringBuilder();
            for (final Response response : batchedResponses) {
                builder.append(sep).append(boundary).append(line);
                builder.append("Content-Type: application/http").append(line);
                builder.append(line);
                builder.append("HTTP/1.1 ")
                    .append(response.status.getStatus())
                    .append(' ')
                    .append(response.status.toString())
                    .append(line);
                builder.append("Content-Length: ").append(response.body.length).append(line);
                builder.append("Content-Type: ").append(response.contentType).append(line);
                response.headers.forEach((k, v) -> builder.append(k).append(": ").append(v).append(line));
                builder.append(line);
                builder.append(new String(response.body, StandardCharsets.UTF_8)).append(line);
                builder.append(line);
            }
            builder.append(line);
            builder.append(sep).append(boundary).append(sep);

            final byte[] content = builder.toString().getBytes(StandardCharsets.UTF_8);
            return new Response(RestStatus.OK, emptyMap(), "multipart/mixed; boundary=" + boundary, content);
        });

        // Fake refresh of an OAuth2 token
        //
        handlers.insert("POST " + endpoint + "/o/oauth2/token", (url, params, req) ->
            newResponse(RestStatus.OK, emptyMap(), jsonBuilder()
                .startObject()
                    .field("access_token", "unknown")
                    .field("token_type", "Bearer")
                    .field("expires_in", 3600)
                .endObject())
        );

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
            BytesReference.bytes(xContentBuilder).writeTo(out);
            return new Response(status, headers, XContentType.JSON.mediaType(), out.toByteArray());
        } catch (final IOException e) {
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
                BytesReference.bytes(builder).writeTo(out);
            }
            return new Response(status, emptyMap(), XContentType.JSON.mediaType(), out.toByteArray());
        } catch (final IOException e) {
            final byte[] bytes = (message != null ? message : "something went wrong").getBytes(StandardCharsets.UTF_8);
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
                                .field("name", name)
                                .field("id", name)
                            .endObject();
    }

    /**
     * Storage Object JSON representation as defined in
     * https://cloud.google.com/storage/docs/json_api/v1/objects#resource
     */
    private static XContentBuilder buildObjectResource(final String bucket, final String name, final byte[] bytes) throws IOException {
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
                            .field("bucket", bucket)
                            .field("size", String.valueOf(bytes.length))
                        .endObject();
    }
}

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

import org.elasticsearch.test.fixture.AbstractHttpFixture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * {@link GoogleCloudStorageFixture} emulates a Google Cloud Storage service.
 *
 * The implementation is based on official documentation available at https://cloud.google.com/storage/docs/json_api/v1/.
 */
public class GoogleCloudStorageFixture extends AbstractHttpFixture {

    /** List of the buckets stored on this test server **/
    private final Map<String, Bucket> buckets = ConcurrentCollections.newConcurrentMap();

    /** Request handlers for the requests made by the Google Cloud Storage client **/
    private final PathTrie<RequestHandler> handlers;

    /**
     * Creates a {@link GoogleCloudStorageFixture}
     */
    private GoogleCloudStorageFixture(final String workingDir, final String bucket) {
        super(workingDir);
        this.buckets.put(bucket, new Bucket(bucket));
        this.handlers = defaultHandlers(buckets);
    }

    @Override
    protected Response handle(final Request request) throws IOException {
        final RequestHandler handler = handlers.retrieve(request.getMethod() + " " + request.getPath(), request.getParameters());
        if (handler != null) {
            return handler.handle(request);
        }
        return null;
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("GoogleCloudStorageFixture <working directory> <bucket>");
        }

        final GoogleCloudStorageFixture fixture = new GoogleCloudStorageFixture(args[0], args[1]);
        fixture.listen();
    }

    /** Builds the default request handlers **/
    private static PathTrie<RequestHandler> defaultHandlers(final Map<String, Bucket> buckets) {
        final PathTrie<RequestHandler> handlers = new PathTrie<>(RestUtils.REST_DECODER);

        // GET Bucket
        //
        // https://cloud.google.com/storage/docs/json_api/v1/buckets/get
        handlers.insert("GET /storage/v1/b/{bucket}", (request) -> {
            final String name = request.getParam("bucket");
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
        handlers.insert("GET /storage/v1/b/{bucket}/o/{object}", (request) -> {
            final String objectName = request.getParam("object");
            if (Strings.hasText(objectName) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            final Bucket bucket = buckets.get(request.getParam("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            for (final Map.Entry<String, Item> object : bucket.objects.entrySet()) {
                if (object.getKey().equals(objectName)) {
                    return newResponse(RestStatus.OK, emptyMap(), buildObjectResource(bucket.name, object.getValue()));
                }
            }
            return newError(RestStatus.NOT_FOUND, "object not found");
        });

        // Delete Object
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/delete
        handlers.insert("DELETE /storage/v1/b/{bucket}/o/{object}", (request) -> {
            final String objectName = request.getParam("object");
            if (Strings.hasText(objectName) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            final Bucket bucket = buckets.get(request.getParam("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            final Item item = bucket.objects.remove(objectName);
            if (item != null) {
                return new Response(RestStatus.NO_CONTENT.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
            }
            return newError(RestStatus.NOT_FOUND, "object not found");
        });

        // Insert Object (initialization)
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/insert
        handlers.insert("POST /upload/storage/v1/b/{bucket}/o", (request) -> {
            final String ifGenerationMatch = request.getParam("ifGenerationMatch");
            final String uploadType = request.getParam("uploadType");
            if ("resumable".equals(uploadType)) {
                final String objectName = request.getParam("name");
                if (Strings.hasText(objectName) == false) {
                    return newError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
                }
                final Bucket bucket = buckets.get(request.getParam("bucket"));
                if (bucket == null) {
                    return newError(RestStatus.NOT_FOUND, "bucket not found");
                }
                if ("0".equals(ifGenerationMatch)) {
                    if (bucket.objects.putIfAbsent(objectName, Item.empty(objectName)) == null) {
                        final String location = /*endpoint +*/ "/upload/storage/v1/b/" + bucket.name + "/o?uploadType=resumable&upload_id="
                            + objectName;
                        return newResponse(RestStatus.CREATED, singletonMap("Location", location), jsonBuilder());
                    } else {
                        return newError(RestStatus.PRECONDITION_FAILED, "object already exist");
                    }
                } else {
                    bucket.objects.put(objectName, Item.empty(objectName));
                    final String location = /*endpoint +*/ "/upload/storage/v1/b/" + bucket.name + "/o?uploadType=resumable&upload_id="
                        + objectName;
                    return newResponse(RestStatus.CREATED, singletonMap("Location", location), jsonBuilder());
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
                final String contentType = request.getContentType();
                if ((contentType != null) && contentType.contains("multipart/related; boundary=")) {
                    boundary = contentType.replace("multipart/related; boundary=", "");
                }

                InputStream inputStreamBody = new ByteArrayInputStream(request.getBody());
                final String contentEncoding = request.getHeader("Content-Encoding");
                if (contentEncoding != null) {
                    if ("gzip".equalsIgnoreCase(contentEncoding)) {
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
                        bucket.objects.put(objectName, new Item(objectName, objectData));
                        return new Response(RestStatus.OK.getStatus(), JSON_CONTENT_TYPE, metadata);
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
        handlers.insert("PUT /upload/storage/v1/b/{bucket}/o", (request) -> {
            final String objectId = request.getParam("upload_id");
            if (Strings.hasText(objectId) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "upload id is missing");
            }

            final Bucket bucket = buckets.get(request.getParam("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            if (bucket.objects.containsKey(objectId) == false) {
                return newError(RestStatus.NOT_FOUND, "object name not found");
            }

            final Item item = new Item(objectId, request.getBody());

            bucket.objects.put(objectId, item);
            return newResponse(RestStatus.OK, emptyMap(), buildObjectResource(bucket.name, item));
        });

        // List Objects
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/list
        handlers.insert("GET /storage/v1/b/{bucket}/o", (request) -> {
            final Bucket bucket = buckets.get(request.getParam("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            builder.field("kind", "storage#objects");
            final Set<String> prefixes = new HashSet<>();
            {
                builder.startArray("items");

                final String prefixParam = request.getParam("prefix");
                final String delimiter = request.getParam("delimiter");

                for (final Map.Entry<String, Item> object : bucket.objects.entrySet()) {
                    String objectKey = object.getKey();
                    if ((prefixParam != null) && (objectKey.startsWith(prefixParam) == false)) {
                        continue;
                    }

                    if (Strings.isNullOrEmpty(delimiter)) {
                        buildObjectResource(builder, bucket.name, object.getValue());
                    } else {
                        int prefixLength = prefixParam.length();
                        String rest = objectKey.substring(prefixLength);
                        int delimiterPos;
                        if ((delimiterPos = rest.indexOf(delimiter)) != -1) {
                            String key = objectKey.substring(0, prefixLength + delimiterPos + 1);
                            prefixes.add(key);
                        } else {
                            buildObjectResource(builder, bucket.name, object.getValue());
                        }
                    }
                }
                builder.endArray();
            }
            {
                if (prefixes.isEmpty() == false) {
                    builder.array("prefixes", prefixes.toArray());
                }
            }
            builder.endObject();
            return newResponse(RestStatus.OK, emptyMap(), builder);
        });

        // Download Object
        //
        // https://cloud.google.com/storage/docs/request-body
        handlers.insert("GET /download/storage/v1/b/{bucket}/o/{object}", (request) -> {
            final String object = request.getParam("object");
            if (Strings.hasText(object) == false) {
                return newError(RestStatus.INTERNAL_SERVER_ERROR, "object id is missing");
            }

            final Bucket bucket = buckets.get(request.getParam("bucket"));
            if (bucket == null) {
                return newError(RestStatus.NOT_FOUND, "bucket not found");
            }

            if (bucket.objects.containsKey(object) == false) {
                return newError(RestStatus.NOT_FOUND, "object name not found");
            }

            return new Response(RestStatus.OK.getStatus(), contentType("application/octet-stream"), bucket.objects.get(object).bytes);
        });

        // Batch
        //
        // https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
        handlers.insert("POST /batch/storage/v1", (request) -> {
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
            final String contentType = request.getContentType();
            if ((contentType != null) && contentType.contains("multipart/mixed; boundary=")) {
                boundary = contentType.replace("multipart/mixed; boundary=", "");
            }

            long batchedRequests = 0L;

            // Read line by line the batched requests
            try (BufferedReader reader = new BufferedReader(
                                              new InputStreamReader(
                                                  new ByteArrayInputStream(request.getBody()), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Start of a batched request
                    if (line.equals("--" + boundary)) {
                        final Map<String, String> batchedHeaders = new HashMap<>();

                        // Reads the headers, if any
                        while ((line = reader.readLine()) != null) {
                            if (line.equals("\r\n") || (line.length() == 0)) {
                                // end of headers
                                break;
                            } else {
                                final String[] header = line.split(":", 2);
                                batchedHeaders.put(header[0], header[1]);
                            }
                        }

                        // Reads the method and URL
                        line = reader.readLine();
                        final String batchedMethod = line.substring(0, line.indexOf(' '));
                        final URI batchedUri = URI.create(line.substring(batchedMethod.length() + 1, line.lastIndexOf(' ')));

                        // Reads the body
                        line = reader.readLine();
                        byte[] batchedBody = new byte[0];
                        if ((line != null) || (line.startsWith("--" + boundary) == false)) {
                            batchedBody = line.getBytes(StandardCharsets.UTF_8);
                        }

                        final Request batchedRequest = new Request(batchedRequests, batchedMethod, batchedUri, batchedHeaders, batchedBody);
                        batchedRequests = batchedRequests + 1;

                        // Executes the batched request
                        final RequestHandler handler =
                            handlers.retrieve(batchedRequest.getMethod() + " " + batchedRequest.getPath(), batchedRequest.getParameters());
                        if (handler != null) {
                            try {
                                batchedResponses.add(handler.handle(batchedRequest));
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
                    .append(response.getStatus())
                    .append(' ')
                    .append(RestStatus.fromCode(response.getStatus()).toString())
                    .append(line);
                builder.append("Content-Length: ").append(response.getBody().length).append(line);
                builder.append("Content-Type: ").append(response.getContentType()).append(line);
                response.getHeaders().forEach((k, v) -> builder.append(k).append(": ").append(v).append(line));
                builder.append(line);
                builder.append(new String(response.getBody(), StandardCharsets.UTF_8)).append(line);
                builder.append(line);
            }
            builder.append(line);
            builder.append(sep).append(boundary).append(sep);

            final byte[] content = builder.toString().getBytes(StandardCharsets.UTF_8);
            return new Response(RestStatus.OK.getStatus(), contentType("multipart/mixed; boundary=" + boundary), content);
        });

        // Fake refresh of an OAuth2 token
        //
        handlers.insert("POST /o/oauth2/token", (request) ->
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
        final Map<String, Item> objects;

        Bucket(final String name) {
            this.name = Objects.requireNonNull(name);
            this.objects = ConcurrentCollections.newConcurrentMap();
        }
    }

    static class Item {
        final String name;
        final LocalDateTime created;
        final byte[] bytes;

        Item(String name, byte[] bytes) {
            this.name = name;
            this.bytes = bytes;
            this.created = LocalDateTime.now(ZoneOffset.UTC);
        }

        public static Item empty(String name) {
            return new Item(name, EMPTY_BYTE);
        }
    }

    /**
     * Builds a JSON response
     */
    private static Response newResponse(final RestStatus status, final Map<String, String> headers, final XContentBuilder xContentBuilder) {
        final Map<String, String> responseHeaders = new HashMap<>(JSON_CONTENT_TYPE);
        responseHeaders.putAll(headers);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BytesReference.bytes(xContentBuilder).writeTo(out);

            return new Response(status.getStatus(), responseHeaders, out.toByteArray());
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
            return new Response(status.getStatus(), JSON_CONTENT_TYPE, out.toByteArray());
        } catch (final IOException e) {
            final byte[] bytes = (message != null ? message : "something went wrong").getBytes(StandardCharsets.UTF_8);
            return new Response(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), TEXT_PLAIN_CONTENT_TYPE, bytes);
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
    private static XContentBuilder buildObjectResource(final String bucket, final Item item) throws IOException {
        return buildObjectResource(jsonBuilder(), bucket, item);
    }

    /**
     * Storage Object JSON representation as defined in
     * https://cloud.google.com/storage/docs/json_api/v1/objects#resource
     */
    private static XContentBuilder buildObjectResource(final XContentBuilder builder,
                                                       final String bucket,
                                                       final Item item) throws IOException {
        return builder.startObject()
                            .field("kind", "storage#object")
                            .field("id", String.join("/", bucket, item.name))
                            .field("name", item.name)
                            .field("timeCreated", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(item.created))
                            .field("bucket", bucket)
                            .field("size", String.valueOf(item.bytes.length))
                        .endObject();
    }
}
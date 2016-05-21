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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.support.RestUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Mock for {@link HttpTransport} to test Google Cloud Storage service.
 * <p>
 * This basically handles each type of request used by the {@link GoogleCloudStorageBlobStore} and provides appropriate responses like
 * the Google Cloud Storage service would do. It is largely based on official documentation available at https://cloud.google
 * .com/storage/docs/json_api/v1/.
 */
public class MockHttpTransport extends com.google.api.client.testing.http.MockHttpTransport {

    private final AtomicInteger objectsCount = new AtomicInteger(0);
    private final Map<String, String> objectsNames = ConcurrentCollections.newConcurrentMap();
    private final Map<String, byte[]> objectsContent = ConcurrentCollections.newConcurrentMap();

    private final PathTrie<Handler> handlers = new PathTrie<>(RestUtils.REST_DECODER);

    public MockHttpTransport(String bucket) {

        // GET Bucket
        //
        // https://cloud.google.com/storage/docs/json_api/v1/buckets/get
        handlers.insert("GET https://www.googleapis.com/storage/v1/b/{bucket}", (url, params, req) -> {
            String name = params.get("bucket");
            if (Strings.hasText(name) == false) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "bucket name is missing");
            }

            if (name.equals(bucket)) {
                return newMockResponse().setContent(buildBucketResource(bucket));
            } else {
                return newMockError(RestStatus.NOT_FOUND, "bucket not found");
            }
        });

        // GET Object
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/get
        handlers.insert("GET https://www.googleapis.com/storage/v1/b/{bucket}/o/{object}", (url, params, req) -> {
            String name = params.get("object");
            if (Strings.hasText(name) == false) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            for (Map.Entry<String, String> object : objectsNames.entrySet()) {
                if (object.getValue().equals(name)) {
                    byte[] content = objectsContent.get(object.getKey());
                    if (content != null) {
                        return newMockResponse().setContent(buildObjectResource(bucket, name, object.getKey(), content.length));
                    }
                }
            }
            return newMockError(RestStatus.NOT_FOUND, "object not found");
        });

        // Download Object
        //
        // https://cloud.google.com/storage/docs/request-endpoints
        handlers.insert("GET https://www.googleapis.com/download/storage/v1/b/{bucket}/o/{object}", (url, params, req) -> {
            String name = params.get("object");
            if (Strings.hasText(name) == false) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            for (Map.Entry<String, String> object : objectsNames.entrySet()) {
                if (object.getValue().equals(name)) {
                    byte[] content = objectsContent.get(object.getKey());
                    if (content == null) {
                        return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "object content is missing");
                    }
                    return newMockResponse().setContent(new ByteArrayInputStream(content));
                }
            }
            return newMockError(RestStatus.NOT_FOUND, "object not found");
        });

        // Insert Object (initialization)
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/insert
        handlers.insert("POST https://www.googleapis.com/upload/storage/v1/b/{bucket}/o", (url, params, req) -> {
            if ("resumable".equals(params.get("uploadType")) == false) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "upload type must be resumable");
            }

            String name = params.get("name");
            if (Strings.hasText(name) == false) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            String objectId = String.valueOf(objectsCount.getAndIncrement());
            objectsNames.put(objectId, name);

            return newMockResponse()
                    .setStatusCode(RestStatus.CREATED.getStatus())
                    .addHeader("Location", "https://www.googleapis.com/upload/storage/v1/b/" + bucket +
                            "/o?uploadType=resumable&upload_id=" + objectId);
        });

        // Insert Object (upload)
        //
        // https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
        handlers.insert("PUT https://www.googleapis.com/upload/storage/v1/b/{bucket}/o", (url, params, req) -> {
            String objectId = params.get("upload_id");
            if (Strings.hasText(objectId) == false) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "upload id is missing");
            }

            String name = objectsNames.get(objectId);
            if (Strings.hasText(name) == false) {
                return newMockError(RestStatus.NOT_FOUND, "object name not found");
            }

            ByteArrayOutputStream os = new ByteArrayOutputStream((int) req.getContentLength());
            try {
                req.getStreamingContent().writeTo(os);
                os.close();
            } catch (IOException e) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            byte[] content = os.toByteArray();
            objectsContent.put(objectId, content);
            return newMockResponse().setContent(buildObjectResource(bucket, name, objectId, content.length));
        });

        // List Objects
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/list
        handlers.insert("GET https://www.googleapis.com/storage/v1/b/{bucket}/o", (url, params, req) -> {
            String prefix = params.get("prefix");

            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                builder.field("kind", "storage#objects");
                builder.startArray("items");
                for (Map.Entry<String, String> o : objectsNames.entrySet()) {
                    if (prefix != null && o.getValue().startsWith(prefix) == false) {
                        continue;
                    }
                    buildObjectResource(builder, bucket, o.getValue(), o.getKey(), objectsContent.get(o.getKey()).length);
                }
                builder.endArray();
                builder.endObject();
                return newMockResponse().setContent(builder.string());
            } catch (IOException e) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        });

        // Delete Object
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/delete
        handlers.insert("DELETE https://www.googleapis.com/storage/v1/b/{bucket}/o/{object}", (url, params, req) -> {
            String name = params.get("object");
            if (Strings.hasText(name) == false) {
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "object name is missing");
            }

            String objectId = null;
            for (Map.Entry<String, String> object : objectsNames.entrySet()) {
                if (object.getValue().equals(name)) {
                    objectId = object.getKey();
                    break;
                }
            }

            if (objectId != null) {
                objectsNames.remove(objectId);
                objectsContent.remove(objectId);
                return newMockResponse().setStatusCode(RestStatus.NO_CONTENT.getStatus());
            }
            return newMockError(RestStatus.NOT_FOUND, "object not found");
        });

        // Copy Object
        //
        // https://cloud.google.com/storage/docs/json_api/v1/objects/copy
        handlers.insert("POST https://www.googleapis.com/storage/v1/b/{srcBucket}/o/{srcObject}/copyTo/b/{destBucket}/o/{destObject}",
                (url, params, req) -> {
                    String source = params.get("srcObject");
                    if (Strings.hasText(source) == false) {
                        return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "source object name is missing");
                    }

                    String dest = params.get("destObject");
                    if (Strings.hasText(dest) == false) {
                        return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "destination object name is missing");
                    }

                    String srcObjectId = null;
                    for (Map.Entry<String, String> object : objectsNames.entrySet()) {
                        if (object.getValue().equals(source)) {
                            srcObjectId = object.getKey();
                            break;
                        }
                    }

                    if (srcObjectId == null) {
                        return newMockError(RestStatus.NOT_FOUND, "source object not found");
                    }

                    byte[] content = objectsContent.get(srcObjectId);
                    if (content == null) {
                        return newMockError(RestStatus.NOT_FOUND, "source content can not be found");
                    }

                    String destObjectId = String.valueOf(objectsCount.getAndIncrement());
                    objectsNames.put(destObjectId, dest);
                    objectsContent.put(destObjectId, content);

                    return newMockResponse().setContent(buildObjectResource(bucket, dest, destObjectId, content.length));
                });

        // Batch
        //
        // https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
        handlers.insert("POST https://www.googleapis.com/batch", (url, params, req) -> {
            List<MockLowLevelHttpResponse> responses = new ArrayList<>();

            // A batch request body looks like this:
            //
            //            --__END_OF_PART__
            //            Content-Length: 71
            //            Content-Type: application/http
            //            content-id: 1
            //            content-transfer-encoding: binary
            //
            //            DELETE https://www.googleapis.com/storage/v1/b/ohifkgu/o/foo%2Ftest
            //
            //
            //            --__END_OF_PART__
            //            Content-Length: 71
            //            Content-Type: application/http
            //            content-id: 2
            //            content-transfer-encoding: binary
            //
            //            DELETE https://www.googleapis.com/storage/v1/b/ohifkgu/o/bar%2Ftest
            //
            //
            //            --__END_OF_PART__--

            // Here we simply process the request body line by line and delegate to other handlers
            // if possible.
            try (ByteArrayOutputStream os = new ByteArrayOutputStream((int) req.getContentLength())) {
                req.getStreamingContent().writeTo(os);

                Streams.readAllLines(new ByteArrayInputStream(os.toByteArray()), new Callback<String>() {
                    @Override
                    public void handle(String line) {
                        Handler handler = handlers.retrieve(line, params);
                        if (handler != null) {
                            try {
                                responses.add(handler.execute(line, params, req));
                            } catch (IOException e) {
                                responses.add(newMockError(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                            }
                        }
                    }
                });
            }

            // Now we can build the response
            String boundary = "__END_OF_PART__";
            String sep = "--";
            String line = "\r\n";

            StringBuilder builder = new StringBuilder();
            for (MockLowLevelHttpResponse resp : responses) {
                builder.append(sep).append(boundary).append(line);
                builder.append(line);
                builder.append("HTTP/1.1 ").append(resp.getStatusCode()).append(' ').append(resp.getReasonPhrase()).append(line);
                builder.append("Content-Length: ").append(resp.getContentLength()).append(line);
                builder.append(line);
            }
            builder.append(line);
            builder.append(sep).append(boundary).append(sep);

            return newMockResponse().setContentType("multipart/mixed; boundary=" + boundary).setContent(builder.toString());
        });
    }

    @Override
    public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
        return new MockLowLevelHttpRequest() {
            @Override
            public LowLevelHttpResponse execute() throws IOException {
                String rawPath = url;
                Map<String, String> params = new HashMap<>();

                int pathEndPos = url.indexOf('?');
                if (pathEndPos != -1) {
                    rawPath = url.substring(0, pathEndPos);
                    RestUtils.decodeQueryString(url, pathEndPos + 1, params);
                }

                Handler handler = handlers.retrieve(method + " " + rawPath, params);
                if (handler != null) {
                    return handler.execute(rawPath, params, this);
                }
                return newMockError(RestStatus.INTERNAL_SERVER_ERROR, "Unable to handle request [method=" + method + ", url=" + url + "]");
            }
        };
    }

    private static MockLowLevelHttpResponse newMockResponse() {
        return new MockLowLevelHttpResponse()
                .setContentType(Json.MEDIA_TYPE)
                .setStatusCode(RestStatus.OK.getStatus())
                .setReasonPhrase(RestStatus.OK.name());
    }

    private static MockLowLevelHttpResponse newMockError(RestStatus status, String message) {
        MockLowLevelHttpResponse response = newMockResponse().setStatusCode(status.getStatus()).setReasonPhrase(status.name());
        try {
            response.setContent(buildErrorResource(status, message));
        } catch (IOException e) {
            response.setContent("Failed to build error resource [" + message + "] because of: " + e.getMessage());
        }
        return response;
    }

    /**
     * Storage Error JSON representation
     */
    private static String buildErrorResource(RestStatus status, String message) throws IOException {
        return jsonBuilder()
                .startObject()
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
                .endObject()
                .string();
    }

    /**
     * Storage Bucket JSON representation as defined in
     * https://cloud.google.com/storage/docs/json_api/v1/bucket#resource
     */
    private static String buildBucketResource(String name) throws IOException {
        return jsonBuilder().startObject()
                                .field("kind", "storage#bucket")
                                .field("id", name)
                            .endObject()
                            .string();
    }

    /**
     * Storage Object JSON representation as defined in
     * https://cloud.google.com/storage/docs/json_api/v1/objects#resource
     */
    private static XContentBuilder buildObjectResource(XContentBuilder builder, String bucket, String name, String id, int size)
            throws IOException {
        return builder.startObject()
                        .field("kind", "storage#object")
                        .field("id", String.join("/", bucket, name, id))
                        .field("name", name)
                        .field("size", String.valueOf(size))
                    .endObject();
    }

    private static String buildObjectResource(String bucket, String name, String id, int size) throws IOException {
        return buildObjectResource(jsonBuilder(), bucket, name, id, size).string();
    }

    interface Handler {
        MockLowLevelHttpResponse execute(String url, Map<String, String> params, MockLowLevelHttpRequest request) throws IOException;
    }

    /**
     * Instanciates a mocked Storage client for tests.
     */
    public static Storage newStorage(String bucket, String applicationName) {
        return new Storage.Builder(new MockHttpTransport(bucket), new JacksonFactory(), null)
                .setApplicationName(applicationName)
                .build();
    }
}

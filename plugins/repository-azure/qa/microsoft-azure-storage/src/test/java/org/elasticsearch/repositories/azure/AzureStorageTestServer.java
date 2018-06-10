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
package org.elasticsearch.repositories.azure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * {@link AzureStorageTestServer} emulates an Azure Storage service through a {@link #handle(String, String, String, Map, byte[])}
 * method that provides appropriate responses for specific requests like the real Azure platform would do.
 * It is based on official documentation available at https://docs.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api.
 */
public class AzureStorageTestServer {

    private static byte[] EMPTY_BYTE = new byte[0];

    /** List of the containers stored on this test server **/
    private final Map<String, Container> containers = ConcurrentCollections.newConcurrentMap();

    /** Request handlers for the requests made by the Azure client **/
    private final PathTrie<RequestHandler> handlers;

    /** Server endpoint **/
    private final String endpoint;

    /** Increments for the requests ids **/
    private final AtomicLong requests = new AtomicLong(0);

    /**
     * Creates a {@link AzureStorageTestServer} with a custom endpoint
     */
    AzureStorageTestServer(final String endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        this.handlers = defaultHandlers(endpoint, containers);
    }

    /** Creates a container in the test server **/
    void createContainer(final String containerName) {
        containers.put(containerName, new Container(containerName));
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

        final RequestHandler handler = handlers.retrieve(method + " " + path, params);
        if (handler != null) {
            return handler.execute(params, headers, body, requestId);
        } else {
            return newInternalError(requestId);
        }
    }

    @FunctionalInterface
    interface RequestHandler {

        /**
         * Simulates the execution of a Azure Storage request and returns a corresponding response.
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
    private static PathTrie<RequestHandler> defaultHandlers(final String endpoint, final Map<String, Container> containers) {
        final PathTrie<RequestHandler> handlers = new PathTrie<>(RestUtils.REST_DECODER);

        // Get Blob Properties
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties
        objectsPaths("HEAD " + endpoint + "/{container}").forEach(path ->
            handlers.insert(path, (params, headers, body, requestId) -> {
                final String containerName = params.get("container");

                final Container container =containers.get(containerName);
                if (container == null) {
                    return newContainerNotFoundError(requestId);
                }

                final String blobName = objectName(params);
                for (Map.Entry<String, byte[]> object : container.objects.entrySet()) {
                    if (object.getKey().equals(blobName)) {
                        Map<String, String> responseHeaders = new HashMap<>();
                        responseHeaders.put("x-ms-blob-content-length", String.valueOf(object.getValue().length));
                        responseHeaders.put("x-ms-blob-type", "blockblob");
                        return new Response(RestStatus.OK, responseHeaders, "text/plain", EMPTY_BYTE);
                    }
                }
                return newBlobNotFoundError(requestId);
            })
        );

        // PUT Blob
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob
        objectsPaths("PUT " + endpoint + "/{container}").forEach(path ->
            handlers.insert(path, (params, headers, body, requestId) -> {
                final String destContainerName = params.get("container");
                final String destBlobName = objectName(params);

                final Container destContainer =containers.get(destContainerName);
                if (destContainer == null) {
                    return newContainerNotFoundError(requestId);
                }
                destContainer.objects.put(destBlobName, body);
                return new Response(RestStatus.CREATED, emptyMap(), "text/plain", EMPTY_BYTE);
            })
        );

        // GET Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
        objectsPaths("GET " + endpoint + "/{container}").forEach(path ->
            handlers.insert(path, (params, headers, body, requestId) -> {
                final String containerName = params.get("container");

                final Container container =containers.get(containerName);
                if (container == null) {
                    return newContainerNotFoundError(requestId);
                }

                final String blobName = objectName(params);
                if (container.objects.containsKey(blobName)) {
                    Map<String, String> responseHeaders = new HashMap<>();
                    responseHeaders.put("x-ms-copy-status", "success");
                    responseHeaders.put("x-ms-blob-type", "blockblob");
                    return new Response(RestStatus.OK, responseHeaders, "application/octet-stream", container.objects.get(blobName));

                }
                return newBlobNotFoundError(requestId);
            })
        );

        // Delete Blob
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob
        objectsPaths("DELETE " + endpoint + "/{container}").forEach(path ->
            handlers.insert(path, (params, headers, body, requestId) -> {
                final String containerName = params.get("container");

                final Container container =containers.get(containerName);
                if (container == null) {
                    return newContainerNotFoundError(requestId);
                }

                final String blobName = objectName(params);
                if (container.objects.remove(blobName) != null) {
                    return new Response(RestStatus.ACCEPTED, emptyMap(), "text/plain", EMPTY_BYTE);
                }
                return newBlobNotFoundError(requestId);
            })
        );

        // List Blobs
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs
        handlers.insert("GET " + endpoint + "/{container}/", (params, headers, body, requestId) -> {
            final String containerName = params.get("container");

            final Container container =containers.get(containerName);
            if (container == null) {
                return newContainerNotFoundError(requestId);
            }

            final String prefix = params.get("prefix");
            return newEnumerationResultsResponse(requestId, container, prefix);
        });

        // Get Container Properties
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-properties
        handlers.insert("HEAD " + endpoint + "/{container}", (params, headers, body, requestId) -> {
            String container = params.get("container");
            if (Strings.hasText(container) && containers.containsKey(container)) {
                return new Response(RestStatus.OK, emptyMap(), "text/plain", EMPTY_BYTE);
            } else {
                return newContainerNotFoundError(requestId);
            }
        });

        return handlers;
    }

    /**
     * Represents a Azure Storage container.
     */
    static class Container {

        /** Container name **/
        final String name;

        /** Blobs contained in the container **/
        final Map<String, byte[]> objects;

        Container(final String name) {
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
     * Retrieves the object name from all derived paths named {pathX} where 0 <= X < 10.
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
     * Azure EnumerationResults Response
     */
    private static Response newEnumerationResultsResponse(final long requestId, final Container container, final String prefix) {
        final String id = Long.toString(requestId);
        final StringBuilder response = new StringBuilder();
        response.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
        response.append("<EnumerationResults ServiceEndpoint=\"http://myaccount.blob.core.windows.net/\"");
        response.append(" ContainerName=\"").append(container.name).append("\">");
        if (prefix != null) {
            response.append("<Prefix>").append(prefix).append("</Prefix>");
        } else {
            response.append("<Prefix/>");
        }
        response.append("<MaxResults>").append(container.objects.size()).append("</MaxResults>");
        response.append("<Blobs>");

        int count = 0;
        for (Map.Entry<String, byte[]> object : container.objects.entrySet()) {
            String objectName = object.getKey();
            if (prefix == null || objectName.startsWith(prefix)) {
                response.append("<Blob>");
                response.append("<Name>").append(objectName).append("</Name>");
                response.append("<Properties>");
                response.append("<Content-Length>").append(object.getValue().length).append("</Content-Length>");
                response.append("<CopyId>").append(count++).append("</CopyId>");
                response.append("<CopyStatus>success</CopyStatus>");
                response.append("<BlobType>BlockBlob</BlobType>");
                response.append("</Properties>");
                response.append("</Blob>");
            }
        }

        response.append("</Blobs>");
        response.append("<NextMarker />");
        response.append("</EnumerationResults>");

        return new Response(RestStatus.OK, singletonMap("x-amz-request-id", id), "application/xml", response.toString().getBytes(UTF_8));
    }

    private static Response newContainerNotFoundError(final long requestId) {
        return newError(requestId, RestStatus.NOT_FOUND, "ContainerNotFound", "The specified container does not exist");
    }

    private static Response newBlobNotFoundError(final long requestId) {
        return newError(requestId, RestStatus.NOT_FOUND, "BlobNotFound", "The specified blob does not exist");
    }

    private static Response newInternalError(final long requestId) {
        return newError(requestId, RestStatus.INTERNAL_SERVER_ERROR, "InternalError", "The server encountered an internal error");
    }

    /**
     * Azure Error
     *
     * https://docs.microsoft.com/en-us/rest/api/storageservices/status-and-error-codes2
     */
    private static Response newError(final long requestId,
                                     final RestStatus status,
                                     final String code,
                                     final String message) {

        final StringBuilder response = new StringBuilder();
        response.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        response.append("<Error>");
        response.append("<Code>").append(code).append("</Code>");
        response.append("<Message>").append(message).append("</Message>");
        response.append("</Error>");

        final Map<String, String> headers = new HashMap<>(2);
        headers.put("x-ms-request-id", String.valueOf(requestId));
        headers.put("x-ms-error-code", code);

        return new Response(status, headers, "application/xml", response.toString().getBytes(UTF_8));
    }
}

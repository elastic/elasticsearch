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

import org.elasticsearch.test.fixture.AbstractHttpFixture;
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

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * {@link AzureStorageFixture} emulates an Azure Storage service.
 * <p>
 * The implementation is based on official documentation available at
 * https://docs.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api.
 */
public class AzureStorageFixture extends AbstractHttpFixture {

    /**
     * List of the containers stored on this test server
     **/
    private final Map<String, Container> containers = ConcurrentCollections.newConcurrentMap();

    /**
     * Request handlers for the requests made by the Azure client
     **/
    private final PathTrie<RequestHandler> handlers;

    /**
     * Creates a {@link AzureStorageFixture} with a custom endpoint
     */
    private AzureStorageFixture(final String workingDir, final String container) {
        super(workingDir);
        this.containers.put(container, new Container(container));
        this.handlers = defaultHandlers(containers);
    }

    @Override
    protected AbstractHttpFixture.Response handle(final Request request) throws IOException {
        final RequestHandler handler = handlers.retrieve(request.getMethod() + " " + request.getPath(), request.getParameters());
        if (handler != null) {
            final String authorization = request.getHeader("Authorization");
            if (authorization == null
                || (authorization.length() > 0 && authorization.contains("azure_integration_test_account") == false)) {
                return newError(request.getId(), RestStatus.FORBIDDEN, "AccessDenied", "Access Denied");
            }
            return handler.handle(request);
        }
        return null;
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("AzureStorageFixture <working directory> <container>");
        }

        final AzureStorageFixture fixture = new AzureStorageFixture(args[0], args[1]);
        fixture.listen();
    }

    /**
     * Builds the default request handlers
     **/
    private static PathTrie<RequestHandler> defaultHandlers(final Map<String, Container> containers) {
        final PathTrie<RequestHandler> handlers = new PathTrie<>(RestUtils.REST_DECODER);

        // Get Blob Properties
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties
        objectsPaths("HEAD /{container}").forEach(path ->
            handlers.insert(path, (request) -> {
                final String containerName = request.getParam("container");

                final Container container = containers.get(containerName);
                if (container == null) {
                    return newContainerNotFoundError(request.getId());
                }

                final String blobName = objectName(request.getParameters());
                for (Map.Entry<String, byte[]> object : container.objects.entrySet()) {
                    if (object.getKey().equals(blobName)) {
                        Map<String, String> responseHeaders = new HashMap<>();
                        responseHeaders.put("x-ms-blob-content-length", String.valueOf(object.getValue().length));
                        responseHeaders.put("x-ms-blob-type", "blockblob");
                        return new Response(RestStatus.OK.getStatus(), responseHeaders, EMPTY_BYTE);
                    }
                }
                return newBlobNotFoundError(request.getId());
            })
        );

        // PUT Blob
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob
        objectsPaths("PUT /{container}").forEach(path ->
            handlers.insert(path, (request) -> {
                final String destContainerName = request.getParam("container");
                final String destBlobName = objectName(request.getParameters());
                final String ifNoneMatch = request.getHeader("If-None-Match");

                final Container destContainer = containers.get(destContainerName);
                if (destContainer == null) {
                    return newContainerNotFoundError(request.getId());
                }

                if ("*".equals(ifNoneMatch)) {
                    byte[] existingBytes = destContainer.objects.putIfAbsent(destBlobName, request.getBody());
                    if (existingBytes != null) {
                        return newBlobAlreadyExistsError(request.getId());
                    }
                } else {
                    destContainer.objects.put(destBlobName, request.getBody());
                }

                return new Response(RestStatus.CREATED.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);            })
        );

        // GET Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
        objectsPaths("GET /{container}").forEach(path ->
            handlers.insert(path, (request) -> {
                final String containerName = request.getParam("container");

                final Container container = containers.get(containerName);
                if (container == null) {
                    return newContainerNotFoundError(request.getId());
                }

                final String blobName = objectName(request.getParameters());
                if (container.objects.containsKey(blobName)) {
                    Map<String, String> responseHeaders = new HashMap<>(contentType("application/octet-stream"));
                    responseHeaders.put("x-ms-copy-status", "success");
                    responseHeaders.put("x-ms-blob-type", "blockblob");
                    return new Response(RestStatus.OK.getStatus(), responseHeaders, container.objects.get(blobName));

                }
                return newBlobNotFoundError(request.getId());
            })
        );

        // Delete Blob
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob
        objectsPaths("DELETE /{container}").forEach(path ->
            handlers.insert(path, (request) -> {
                final String containerName = request.getParam("container");

                final Container container = containers.get(containerName);
                if (container == null) {
                    return newContainerNotFoundError(request.getId());
                }

                final String blobName = objectName(request.getParameters());
                if (container.objects.remove(blobName) != null) {
                    return new Response(RestStatus.ACCEPTED.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
                }
                return newBlobNotFoundError(request.getId());
            })
        );

        // List Blobs
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs
        handlers.insert("GET /{container}/", (request) -> {
            final String containerName = request.getParam("container");

            final Container container = containers.get(containerName);
            if (container == null) {
                return newContainerNotFoundError(request.getId());
            }

            final String prefix = request.getParam("prefix");
            return newEnumerationResultsResponse(request.getId(), container, prefix);
        });

        // Get Container Properties
        //
        // https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-properties
        handlers.insert("HEAD /{container}", (request) -> {
            String container = request.getParam("container");
            if (Strings.hasText(container) && containers.containsKey(container)) {
                return new Response(RestStatus.OK.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
            } else {
                return newContainerNotFoundError(request.getId());
            }
        });

        return handlers;
    }

    /**
     * Represents a Azure Storage container.
     */
    static class Container {

        /**
         * Container name
         **/
        final String name;

        /**
         * Blobs contained in the container
         **/
        final Map<String, byte[]> objects;

        Container(final String name) {
            this.name = Objects.requireNonNull(name);
            this.objects = ConcurrentCollections.newConcurrentMap();
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
     * <p>
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

        final Map<String, String> headers = new HashMap<>(contentType("application/xml"));
        headers.put("x-ms-request-id", id);

        return new Response(RestStatus.OK.getStatus(), headers, response.toString().getBytes(UTF_8));
    }

    private static Response newContainerNotFoundError(final long requestId) {
        return newError(requestId, RestStatus.NOT_FOUND, "ContainerNotFound", "The specified container does not exist");
    }

    private static Response newBlobNotFoundError(final long requestId) {
        return newError(requestId, RestStatus.NOT_FOUND, "BlobNotFound", "The specified blob does not exist");
    }

    private static Response newBlobAlreadyExistsError(final long requestId) {
        return newError(requestId, RestStatus.CONFLICT, "BlobAlreadyExists", "The specified blob already exists");
    }

    /**
     * Azure Error
     * <p>
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

        final Map<String, String> headers = new HashMap<>(contentType("application/xml"));
        headers.put("x-ms-request-id", String.valueOf(requestId));
        headers.put("x-ms-error-code", code);

        return new Response(status.getStatus(), headers, response.toString().getBytes(UTF_8));
    }
}

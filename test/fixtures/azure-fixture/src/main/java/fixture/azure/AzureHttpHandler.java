/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.azure;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.repositories.azure.AzureFixtureHelper.assertValidBlockId;

/**
 * Minimal HTTP handler that acts as an Azure compliant server
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate an Azure endpoint")
public class AzureHttpHandler implements HttpHandler {
    private static final Logger logger = LogManager.getLogger(AzureHttpHandler.class);
    private static final Pattern RANGE_HEADER_PATTERN = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$");
    static final String X_MS_LEASE_ID = "x-ms-lease-id";
    static final String X_MS_PROPOSED_LEASE_ID = "x-ms-proposed-lease-id";
    static final String X_MS_LEASE_DURATION = "x-ms-lease-duration";
    static final String X_MS_BLOB_TYPE = "x-ms-blob-type";
    static final String X_MS_BLOB_CONTENT_LENGTH = "x-ms-blob-content-length";

    private final String account;
    private final String container;
    private final Predicate<String> authHeaderPredicate;
    private final MockAzureBlobStore mockAzureBlobStore;

    public AzureHttpHandler(final String account, final String container, @Nullable Predicate<String> authHeaderPredicate) {
        this.account = Objects.requireNonNull(account);
        this.container = Objects.requireNonNull(container);
        this.authHeaderPredicate = authHeaderPredicate;
        this.mockAzureBlobStore = new MockAzureBlobStore();
    }

    private static List<String> getAuthHeader(HttpExchange exchange) {
        return exchange.getRequestHeaders().get("Authorization");
    }

    private boolean isValidAuthHeader(HttpExchange exchange) {
        if (authHeaderPredicate == null) {
            return true;
        }

        final var authHeader = getAuthHeader(exchange);
        if (authHeader == null) {
            return false;
        }

        if (authHeader.size() != 1) {
            return false;
        }

        return authHeaderPredicate.test(authHeader.get(0));
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        if (isValidAuthHeader(exchange) == false) {
            try (exchange; var builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                builder.startObject();
                builder.field("method", exchange.getRequestMethod());
                builder.field("uri", exchange.getRequestURI().toString());
                builder.field("predicate", authHeaderPredicate.toString());
                builder.field("authorization", Objects.toString(getAuthHeader(exchange)));
                builder.startObject("headers");
                for (final var header : exchange.getRequestHeaders().entrySet()) {
                    if (header.getValue() == null) {
                        builder.nullField(header.getKey());
                    } else {
                        builder.startArray(header.getKey());
                        for (final var value : header.getValue()) {
                            builder.value(value);
                        }
                        builder.endArray();
                    }
                }
                builder.endObject();
                builder.endObject();
                final var responseBytes = BytesReference.bytes(builder);
                exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(RestStatus.FORBIDDEN.getStatus(), responseBytes.length());
                responseBytes.writeTo(exchange.getResponseBody());
                return;
            }
        }

        final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
        if (request.startsWith("GET") || request.startsWith("HEAD") || request.startsWith("DELETE")) {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been empty but saw [" + read + "]";
        }
        try {
            if (Regex.simpleMatch("PUT /" + account + "/" + container + "/*blockid=*", request)) {
                // Put Block (https://docs.microsoft.com/en-us/rest/api/storageservices/put-block)
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getRawQuery(), 0, params);

                final String blockId = params.get("blockid");
                assert assertValidBlockId(blockId);
                mockAzureBlobStore.putBlock(blobPath(exchange), blockId, Streams.readFully(exchange.getRequestBody()), leaseId(exchange));
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("PUT /" + account + "/" + container + "/*comp=blocklist*", request)) {
                // Put Block List (https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list)
                final String blockList = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
                final List<String> blockIds = Arrays.stream(blockList.split("<Latest>"))
                    .filter(line -> line.contains("</Latest>"))
                    .map(line -> line.substring(0, line.indexOf("</Latest>")))
                    .toList();

                mockAzureBlobStore.putBlockList(blobPath(exchange), blockIds, leaseId(exchange));
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("PUT /" + account + "/" + container + "*comp=lease*", request)) {
                // Lease Blob (https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob)
                final String leaseAction = requireHeader(exchange, "x-ms-lease-action");

                switch (leaseAction) {
                    case "acquire" -> {
                        final int leaseDurationSeconds = requireIntegerHeader(exchange, X_MS_LEASE_DURATION);
                        final String proposedLeaseId = exchange.getRequestHeaders().getFirst(X_MS_PROPOSED_LEASE_ID);
                        final String newLeaseId = mockAzureBlobStore.acquireLease(
                            blobPath(exchange),
                            leaseDurationSeconds,
                            proposedLeaseId
                        );
                        exchange.getResponseHeaders().set(X_MS_LEASE_ID, newLeaseId);
                        exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    }
                    case "release" -> {
                        final String leaseId = requireHeader(exchange, X_MS_LEASE_ID);
                        mockAzureBlobStore.releaseLease(blobPath(exchange), leaseId);
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                    }
                    case "renew", "change", "break" -> throw new MockAzureBlobStore.AzureBlobStoreError(
                        RestStatus.NOT_IMPLEMENTED,
                        "NotImplemented",
                        "Attempted to use unsupported lease API: " + leaseAction
                    );
                    default -> throw new MockAzureBlobStore.BadRequestException(
                        "InvalidHeaderValue",
                        "Invalid x-ms-lease-action header: " + leaseAction
                    );
                }
            } else if (Regex.simpleMatch("PUT /" + account + "/" + container + "/*", request)) {
                // PUT Blob (see https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob)
                final String blobTypeHeader = requireHeader(exchange, X_MS_BLOB_TYPE);
                final MockAzureBlobStore.BlobType blobType = MockAzureBlobStore.BlobType.fromXMSBlobType(blobTypeHeader);
                if (blobType == null) {
                    throw new MockAzureBlobStore.ConflictException("InvalidHeaderValue", "Unable to parse blobType: " + blobTypeHeader);
                }
                final String ifNoneMatch = exchange.getRequestHeaders().getFirst("If-None-Match");
                mockAzureBlobStore.putBlob(
                    blobPath(exchange),
                    Streams.readFully(exchange.getRequestBody()),
                    blobType,
                    ifNoneMatch,
                    leaseId(exchange)
                );
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("HEAD /" + account + "/" + container + "/*", request)) {
                // Get Blob Properties (see https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties)
                final MockAzureBlobStore.AzureBlob azureBlob = mockAzureBlobStore.getBlob(blobPath(exchange), leaseId(exchange));

                final Headers responseHeaders = exchange.getResponseHeaders();
                responseHeaders.add(X_MS_BLOB_CONTENT_LENGTH, String.valueOf(azureBlob.length()));
                responseHeaders.add("Content-Length", String.valueOf(azureBlob.length()));
                responseHeaders.add(X_MS_BLOB_TYPE, azureBlob.type().getXMsBlobType());
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);

            } else if (Regex.simpleMatch("GET /" + account + "/" + container + "/*", request)) {
                // Get Blob (https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob)
                final MockAzureBlobStore.AzureBlob blob = mockAzureBlobStore.getBlob(blobPath(exchange), leaseId(exchange));

                final BytesReference responseContent;
                // see Constants.HeaderConstants.STORAGE_RANGE_HEADER
                final String range = exchange.getRequestHeaders().getFirst("x-ms-range");
                if (range != null) {
                    final Matcher matcher = RANGE_HEADER_PATTERN.matcher(range);
                    if (matcher.matches() == false) {
                        throw new MockAzureBlobStore.BadRequestException(
                            "InvalidHeaderValue",
                            "Range header does not match expected format: " + range
                        );
                    }

                    final long start = Long.parseLong(matcher.group(1));
                    final long end = Long.parseLong(matcher.group(2));

                    if (blob.length() <= start) {
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.sendResponseHeaders(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), -1);
                        return;
                    }

                    responseContent = blob.slice(Math.toIntExact(start), Math.toIntExact(Math.min(end - start + 1, blob.length() - start)));
                } else {
                    responseContent = blob.getContents();
                }

                exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().add(X_MS_BLOB_CONTENT_LENGTH, String.valueOf(responseContent.length()));
                exchange.getResponseHeaders().add(X_MS_BLOB_TYPE, blob.type().getXMsBlobType());
                exchange.getResponseHeaders().add("ETag", "\"blockblob\"");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseContent.length() == 0 ? -1 : responseContent.length());
                responseContent.writeTo(exchange.getResponseBody());

            } else if (Regex.simpleMatch("DELETE /" + account + "/" + container + "/*", request)) {
                // Delete Blob (https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob)
                mockAzureBlobStore.deleteBlob(blobPath(exchange), leaseId(exchange));
                exchange.sendResponseHeaders(RestStatus.ACCEPTED.getStatus(), -1);

            } else if (Regex.simpleMatch("GET /" + account + "/" + container + "?*restype=container*comp=list*", request)) {
                // List Blobs (https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs)
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                final StringBuilder list = new StringBuilder();
                list.append("""
                    <?xml version="1.0" encoding="UTF-8"?>
                    <EnumerationResults>""");
                final String prefix = params.get("prefix");
                final Set<String> blobPrefixes = new HashSet<>();
                final String delimiter = params.get("delimiter");
                if (delimiter != null) {
                    list.append("<Delimiter>").append(delimiter).append("</Delimiter>");
                }
                list.append("<Blobs>");
                final Map<String, MockAzureBlobStore.AzureBlob> matchingBlobs = mockAzureBlobStore.listBlobs(prefix, leaseId(exchange));
                for (Map.Entry<String, MockAzureBlobStore.AzureBlob> blob : matchingBlobs.entrySet()) {
                    final String blobPath = blob.getKey();
                    if (delimiter != null) {
                        int fromIndex = (prefix != null ? prefix.length() : 0);
                        int delimiterPosition = blobPath.indexOf(delimiter, fromIndex);
                        if (delimiterPosition > 0) {
                            blobPrefixes.add(blobPath.substring(0, delimiterPosition) + delimiter);
                            continue;
                        }
                    }
                    list.append(String.format(Locale.ROOT, """
                        <Blob>
                           <Name>%s</Name>
                           <Properties>
                             <Content-Length>%s</Content-Length>
                             <BlobType>BlockBlob</BlobType>
                           </Properties>
                        </Blob>""", blobPath, blob.getValue().length()));
                }
                if (blobPrefixes.isEmpty() == false) {
                    blobPrefixes.forEach(p -> list.append("<BlobPrefix><Name>").append(p).append("</Name></BlobPrefix>"));
                }
                list.append("""
                    </Blobs>
                    <NextMarker/>
                    </EnumerationResults>""");

                byte[] response = list.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("POST /" + account + "/" + container + "*restype=container*comp=batch*", request)) {
                // Blob Batch (https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch)
                final StringBuilder response = new StringBuilder();

                try (BufferedReader requestReader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                    final String batchBoundary = requestReader.readLine();
                    final String responseBoundary = "batch_" + UUID.randomUUID();

                    String line;
                    String contentId = null, requestId = null, toDelete = null;
                    while ((line = requestReader.readLine()) != null) {
                        if (batchBoundary.equals(line) || (batchBoundary + "--").equals(line)) {
                            // Found the end of a single request, process it
                            if (contentId == null || requestId == null || toDelete == null) {
                                throw new IllegalStateException(
                                    "Missing contentId/requestId/toDelete: " + contentId + "/" + requestId + "/" + toDelete
                                );
                            }

                            // Process the deletion
                            try {
                                mockAzureBlobStore.deleteBlob(toDelete, leaseId(exchange));
                                final String acceptedPart = Strings.format("""
                                    --%s
                                    Content-Type: application/http
                                    Content-ID: %s

                                    HTTP/1.1 202 Accepted
                                    x-ms-delete-type-permanent: true
                                    x-ms-request-id: %s
                                    x-ms-version: 2018-11-09

                                    """, responseBoundary, contentId, requestId).replaceAll("\n", "\r\n");
                                response.append(acceptedPart);
                            } catch (MockAzureBlobStore.AzureBlobStoreError e) {
                                final String errorResponseBody = Strings.format(
                                    """
                                        <?xml version="1.0" encoding="utf-8"?>
                                        <Error><Code>%s</Code><Message>%s
                                        RequestId:%s
                                        Time:%s</Message></Error>""",
                                    e.getErrorCode(),
                                    e.getMessage(),
                                    requestId,
                                    DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now(ZoneId.of("UTC")))
                                );
                                final String errorResponsePart = Strings.format(
                                    """
                                        --%s
                                        Content-Type: application/http
                                        Content-ID: %s

                                        HTTP/1.1 %s %s
                                        x-ms-error-code: %s
                                        x-ms-request-id: %s
                                        x-ms-version: 2018-11-09
                                        Content-Length: %d
                                        Content-Type: application/xml

                                        %s
                                        """,
                                    responseBoundary,
                                    contentId,
                                    e.getRestStatus().getStatus(),
                                    e.getMessage(),
                                    e.getErrorCode(),
                                    requestId,
                                    errorResponseBody.length(),
                                    errorResponseBody
                                ).replaceAll("\n", "\r\n");
                                response.append(errorResponsePart);
                            }

                            // Clear the state
                            toDelete = null;
                            contentId = null;
                            requestId = null;
                        } else if (Regex.simpleMatch("x-ms-client-request-id: *", line)) {
                            if (requestId != null) {
                                throw new IllegalStateException("Got multiple request IDs in a single request?");
                            }
                            requestId = line.split("\\s")[1];
                        } else if (Regex.simpleMatch("Content-ID: *", line)) {
                            if (contentId != null) {
                                throw new IllegalStateException("Got multiple content IDs in a single request?");
                            }
                            contentId = line.split("\\s")[1];
                        } else if (Regex.simpleMatch("DELETE /" + container + "/*", line)) {
                            final String blobName = RestUtils.decodeComponent(line.split("(\\s|\\?)")[1]);
                            if (toDelete != null) {
                                throw new IllegalStateException("Got multiple deletes in a single request?");
                            }
                            // strip off the "/{container}/" prefix
                            toDelete = blobName.substring(container.length() + 2);
                        }
                    }
                    response.append("--").append(responseBoundary).append("--\r\n0\r\n");
                    // Send the response
                    exchange.getResponseHeaders().add("Content-Type", "multipart/mixed; boundary=" + responseBoundary);
                    exchange.sendResponseHeaders(RestStatus.ACCEPTED.getStatus(), response.length());
                    logger.debug("--> Sending response:\n{}", response);
                    exchange.getResponseBody().write(response.toString().getBytes(StandardCharsets.UTF_8));
                }
            } else {
                logger.warn("--> Unrecognised request received: {}", request);
                sendError(exchange, RestStatus.NOT_FOUND, "ResourceNotFound", "The specified resource doesn't exist.");
            }
        } catch (MockAzureBlobStore.AzureBlobStoreError e) {
            sendError(exchange, e);
        } catch (Exception e) {
            logger.error("Uncaught exception", e);
            sendError(exchange, RestStatus.INTERNAL_SERVER_ERROR, "InternalError", e.getMessage());
        } finally {
            exchange.close();
        }
    }

    private String requireHeader(HttpExchange exchange, String headerName) {
        final String headerValue = exchange.getRequestHeaders().getFirst(headerName);
        if (headerValue == null) {
            throw new MockAzureBlobStore.BadRequestException("MissingRequiredHeader", "Missing " + headerName + " header");
        }
        return headerValue;
    }

    private int requireIntegerHeader(HttpExchange exchange, String headerName) {
        final String headerValue = requireHeader(exchange, headerName);
        try {
            return Integer.parseInt(headerValue);
        } catch (NumberFormatException e) {
            throw new MockAzureBlobStore.BadRequestException("InvalidHeaderValue", "Invalid " + X_MS_LEASE_DURATION + " header");
        }
    }

    @Nullable
    private String leaseId(HttpExchange exchange) {
        return exchange.getRequestHeaders().getFirst(X_MS_LEASE_ID);
    }

    private String blobPath(HttpExchange exchange) {
        // Strip off "/{account}/{container}/" prefix
        return exchange.getRequestURI().getPath().substring(account.length() + container.length() + 3);
    }

    public Map<String, BytesReference> blobs() {
        return mockAzureBlobStore.blobs();
    }

    public static void sendError(HttpExchange exchange, MockAzureBlobStore.AzureBlobStoreError error) throws IOException {
        sendError(exchange, error.getRestStatus(), error.getErrorCode(), error.getMessage());
    }

    public static void sendError(final HttpExchange exchange, final RestStatus status) throws IOException {
        final String errorCode = toAzureErrorCode(status);
        sendError(exchange, status, errorCode, status.toString());
    }

    public static void sendError(HttpExchange exchange, RestStatus restStatus, String errorCode, String errorMessage) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/xml");

        // see Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER
        final String requestId = exchange.getRequestHeaders().getFirst("x-ms-client-request-id");
        if (requestId != null) {
            // see Constants.HeaderConstants.STORAGE_RANGE_HEADER
            headers.add("x-ms-request-id", requestId);
        }

        // see Constants.HeaderConstants.ERROR_CODE
        headers.add("x-ms-error-code", errorCode);

        if ("HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(restStatus.getStatus(), -1L);
        } else {
            final byte[] response = (String.format(Locale.ROOT, """
                <?xml version="1.0" encoding="UTF-8"?>
                <Error>
                    <Code>%s</Code>
                    <Message>%s</Message>
                </Error>""", errorCode, errorMessage)).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(restStatus.getStatus(), response.length);
            exchange.getResponseBody().write(response);
        }
    }

    // See https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
    private static String toAzureErrorCode(final RestStatus status) {
        assert status.getStatus() >= 400;
        return switch (status) {
            case BAD_REQUEST -> "InvalidMetadata";
            case NOT_FOUND -> "BlobNotFound";
            case INTERNAL_SERVER_ERROR -> "InternalError";
            case SERVICE_UNAVAILABLE -> "ServerBusy";
            case CONFLICT -> "BlobAlreadyExists";
            default -> throw new IllegalArgumentException(
                "Error code [" + status.getStatus() + "] is not mapped to an existing Azure code"
            );
        };
    }
}

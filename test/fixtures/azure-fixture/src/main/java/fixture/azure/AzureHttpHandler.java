/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.azure;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.repositories.azure.AzureFixtureHelper.assertValidBlockId;

/**
 * Minimal HTTP handler that acts as an Azure compliant server
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate an Azure endpoint")
public class AzureHttpHandler implements HttpHandler {
    private final Map<String, BytesReference> blobs;
    private final String account;
    private final String container;
    private final Predicate<String> authHeaderPredicate;

    public AzureHttpHandler(final String account, final String container, @Nullable Predicate<String> authHeaderPredicate) {
        this.account = Objects.requireNonNull(account);
        this.container = Objects.requireNonNull(container);
        this.authHeaderPredicate = authHeaderPredicate;
        this.blobs = new ConcurrentHashMap<>();
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
                blobs.put(blockId, Streams.readFully(exchange.getRequestBody()));
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("PUT /" + account + "/" + container + "/*comp=blocklist*", request)) {
                // Put Block List (https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list)
                final String blockList = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8));
                final List<String> blockIds = Arrays.stream(blockList.split("<Latest>"))
                    .filter(line -> line.contains("</Latest>"))
                    .map(line -> line.substring(0, line.indexOf("</Latest>")))
                    .toList();

                final ByteArrayOutputStream blob = new ByteArrayOutputStream();
                for (String blockId : blockIds) {
                    BytesReference block = blobs.remove(blockId);
                    assert block != null;
                    block.writeTo(blob);
                }
                blobs.put(exchange.getRequestURI().getPath(), new BytesArray(blob.toByteArray()));
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("PUT /" + account + "/" + container + "/*", request)) {
                // PUT Blob (see https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob)
                final String ifNoneMatch = exchange.getRequestHeaders().getFirst("If-None-Match");
                if ("*".equals(ifNoneMatch)) {
                    if (blobs.putIfAbsent(exchange.getRequestURI().getPath(), Streams.readFully(exchange.getRequestBody())) != null) {
                        sendError(exchange, RestStatus.CONFLICT);
                        return;
                    }
                } else {
                    blobs.put(exchange.getRequestURI().getPath(), Streams.readFully(exchange.getRequestBody()));
                }
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("HEAD /" + account + "/" + container + "/*", request)) {
                // Get Blob Properties (see https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties)
                final BytesReference blob = blobs.get(exchange.getRequestURI().getPath());
                if (blob == null) {
                    sendError(exchange, RestStatus.NOT_FOUND);
                    return;
                }
                exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blob.length()));
                exchange.getResponseHeaders().add("Content-Length", String.valueOf(blob.length()));
                exchange.getResponseHeaders().add("x-ms-blob-type", "BlockBlob");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);

            } else if (Regex.simpleMatch("GET /" + account + "/" + container + "/*", request)) {
                // GET Object (https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html)
                final BytesReference blob = blobs.get(exchange.getRequestURI().getPath());
                if (blob == null) {
                    sendError(exchange, RestStatus.NOT_FOUND);
                    return;
                }

                // see Constants.HeaderConstants.STORAGE_RANGE_HEADER
                final String range = exchange.getRequestHeaders().getFirst("x-ms-range");
                final Matcher matcher = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$").matcher(range);
                if (matcher.matches() == false) {
                    throw new AssertionError("Range header does not match expected format: " + range);
                }

                final long start = Long.parseLong(matcher.group(1));
                final long end = Long.parseLong(matcher.group(2));

                if (blob.length() <= start) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), -1);
                    return;
                }

                var responseBlob = blob.slice(Math.toIntExact(start), Math.toIntExact(Math.min(end - start + 1, blob.length() - start)));

                exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(responseBlob.length()));
                exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                exchange.getResponseHeaders().add("ETag", "\"blockblob\"");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBlob.length());
                responseBlob.writeTo(exchange.getResponseBody());

            } else if (Regex.simpleMatch("DELETE /" + account + "/" + container + "/*", request)) {
                // Delete Blob (https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob)
                final boolean deleted = blobs.entrySet().removeIf(blob -> blob.getKey().startsWith(exchange.getRequestURI().getPath()));
                if (deleted) {
                    exchange.sendResponseHeaders(RestStatus.ACCEPTED.getStatus(), -1);
                } else {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                }

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
                for (Map.Entry<String, BytesReference> blob : blobs.entrySet()) {
                    if (prefix != null && blob.getKey().startsWith("/" + account + "/" + container + "/" + prefix) == false) {
                        continue;
                    }
                    String blobPath = blob.getKey().replace("/" + account + "/" + container + "/", "");
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

            } else {
                sendError(exchange, RestStatus.BAD_REQUEST);
            }
        } finally {
            exchange.close();
        }
    }

    public Map<String, BytesReference> blobs() {
        return blobs;
    }

    public static void sendError(final HttpExchange exchange, final RestStatus status) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/xml");

        // see Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER
        final String requestId = exchange.getRequestHeaders().getFirst("x-ms-client-request-id");
        if (requestId != null) {
            // see Constants.HeaderConstants.STORAGE_RANGE_HEADER
            headers.add("x-ms-request-id", requestId);
        }

        final String errorCode = toAzureErrorCode(status);
        // see Constants.HeaderConstants.ERROR_CODE
        headers.add("x-ms-error-code", errorCode);

        if ("HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status.getStatus(), -1L);
        } else {
            final byte[] response = (String.format(Locale.ROOT, """
                <?xml version="1.0" encoding="UTF-8"?>
                <Error>
                    <Code>%s</Code>
                    <Message>%s</Message>
                </Error>""", errorCode, status)).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.getStatus(), response.length);
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

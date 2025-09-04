/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package fixture.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.fixture.HttpHeaderParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static fixture.gcs.MockGcsBlobStore.failAndThrow;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Minimal HTTP handler that acts as a Google Cloud Storage compliant server
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageHttpHandler implements HttpHandler {

    private static final String IF_GENERATION_MATCH = "ifGenerationMatch";
    private static final String GENERATION = "generation";

    private final AtomicInteger defaultPageLimit = new AtomicInteger(1_000);
    private final MockGcsBlobStore mockGcsBlobStore;
    private final String bucket;

    public GoogleCloudStorageHttpHandler(final String bucket) {
        this.bucket = Objects.requireNonNull(bucket);
        this.mockGcsBlobStore = new MockGcsBlobStore();
    }

    /**
     * Set the default page limit
     *
     * @param limit The new limit
     */
    public void setDefaultPageLimit(final int limit) {
        this.defaultPageLimit.set(limit);
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
        if (request.startsWith("GET") || request.startsWith("HEAD") || request.startsWith("DELETE")) {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been empty but saw [" + read + "]";
        }
        try {
            // Request body is closed in the finally block
            final BytesReference requestBody = Streams.readFully(Streams.noCloseStream(exchange.getRequestBody()));
            if (request.equals("GET /") && "Google".equals(exchange.getRequestHeaders().getFirst("Metadata-Flavor"))) {
                // the SDK checks this endpoint to determine if it's running within Google Compute Engine
                exchange.getResponseHeaders().add("Metadata-Flavor", "Google");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
            } else if (Regex.simpleMatch("GET /storage/v1/b/" + bucket + "/o/*", request)) {
                final String key = exchange.getRequestURI().getPath().replace("/storage/v1/b/" + bucket + "/o/", "");
                final Long ifGenerationMatch = parseOptionalLongParameter(exchange, IF_GENERATION_MATCH);
                final Long generation = parseOptionalLongParameter(exchange, GENERATION);
                final MockGcsBlobStore.BlobVersion blob = mockGcsBlobStore.getBlob(key, ifGenerationMatch, generation);
                writeBlobVersionAsJson(exchange, blob);
            } else if (Regex.simpleMatch("GET /storage/v1/b/" + bucket + "/o*", request)) {
                // List Objects https://cloud.google.com/storage/docs/json_api/v1/objects/list
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI(), params);
                final String prefix = params.getOrDefault("prefix", "");
                final int maxResults = Integer.parseInt(params.getOrDefault("maxResults", String.valueOf(defaultPageLimit.get())));
                final String delimiter = params.getOrDefault("delimiter", "");
                final String pageToken = params.get("pageToken");

                final MockGcsBlobStore.PageOfBlobs pageOfBlobs;
                if (pageToken != null) {
                    pageOfBlobs = mockGcsBlobStore.listBlobs(pageToken);
                } else {
                    pageOfBlobs = mockGcsBlobStore.listBlobs(maxResults, delimiter, prefix);
                }

                ListBlobsResponse response = new ListBlobsResponse(bucket, pageOfBlobs);
                try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
                    response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    BytesReference responseBytes = BytesReference.bytes(builder);
                    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBytes.length());
                    responseBytes.writeTo(exchange.getResponseBody());
                }
            } else if (Regex.simpleMatch("GET /storage/v1/b/" + bucket + "*", request)) {
                // GET Bucket https://cloud.google.com/storage/docs/json_api/v1/buckets/get
                throw new AssertionError("Should not call get bucket API");

            } else if (Regex.simpleMatch("GET /download/storage/v1/b/" + bucket + "/o/*", request)) {
                // Download Object https://cloud.google.com/storage/docs/request-body
                final String path = exchange.getRequestURI().getPath().replace("/download/storage/v1/b/" + bucket + "/o/", "");
                final Long ifGenerationMatch = parseOptionalLongParameter(exchange, IF_GENERATION_MATCH);
                final Long generation = parseOptionalLongParameter(exchange, GENERATION);
                final MockGcsBlobStore.BlobVersion blob = mockGcsBlobStore.getBlob(path, ifGenerationMatch, generation);
                if (blob != null) {
                    final String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                    final BytesReference response;
                    final int statusCode;
                    if (rangeHeader == null) {
                        response = blob.contents();
                        statusCode = RestStatus.OK.getStatus();
                    } else {
                        final HttpHeaderParser.Range range = HttpHeaderParser.parseRangeHeader(rangeHeader);
                        if (range == null) {
                            throw new AssertionError("Range bytes header does not match expected format: " + rangeHeader);
                        }

                        if (range.start() >= blob.contents().length()) {
                            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                            exchange.sendResponseHeaders(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), -1);
                            return;
                        }

                        final long lastIndex = Math.min(range.end(), blob.contents().length() - 1);
                        response = blob.contents().slice(Math.toIntExact(range.start()), Math.toIntExact(lastIndex - range.start() + 1));
                        statusCode = RestStatus.PARTIAL_CONTENT.getStatus();
                    }
                    // I think it's enough to use the generation here, at least until
                    // we implement "metageneration", at that point we must incorporate both
                    // See: https://cloud.google.com/storage/docs/metadata#etags
                    exchange.getResponseHeaders().add("ETag", String.valueOf(blob.generation()));
                    exchange.getResponseHeaders().add("x-goog-generation", String.valueOf(blob.generation()));
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(statusCode, response.length());
                    response.writeTo(exchange.getResponseBody());
                } else {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                }

            } else if (Regex.simpleMatch("POST /batch/storage/v1", request)) {
                // Batch https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
                final String uri = "/storage/v1/b/" + bucket + "/o/";
                final StringBuilder batch = new StringBuilder();
                for (String line : Streams.readAllLines(requestBody.streamInput())) {
                    if (line.isEmpty() || line.startsWith("--") || line.toLowerCase(Locale.ROOT).startsWith("content")) {
                        batch.append(line).append("\r\n");
                    } else if (line.startsWith("DELETE")) {
                        final String name = line.substring(line.indexOf(uri) + uri.length(), line.lastIndexOf(" HTTP"));
                        if (Strings.hasText(name)) {
                            mockGcsBlobStore.deleteBlob(URLDecoder.decode(name, UTF_8));
                            batch.append("HTTP/1.1 204 NO_CONTENT").append("\r\n");
                            batch.append("\r\n");
                        }
                    }
                }
                byte[] response = batch.toString().getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", exchange.getRequestHeaders().getFirst("Content-Type"));
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/" + bucket + "/*uploadType=multipart*", request)) {
                try {
                    final var multipartUpload = MultipartUpload.parseBody(exchange, requestBody.streamInput());
                    final Long ifGenerationMatch = parseOptionalLongParameter(exchange, IF_GENERATION_MATCH);
                    final MockGcsBlobStore.BlobVersion newBlobVersion = mockGcsBlobStore.updateBlob(
                        multipartUpload.name(),
                        ifGenerationMatch,
                        multipartUpload.content()
                    );
                    writeBlobVersionAsJson(exchange, newBlobVersion);
                } catch (IllegalArgumentException e) {
                    throw new AssertionError(e);
                }
            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/" + bucket + "/*uploadType=resumable*", request)) {
                // Resumable upload initialization https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI(), params);
                final String blobName = params.get("name");
                final Long ifGenerationMatch = parseOptionalLongParameter(exchange, IF_GENERATION_MATCH);
                final MockGcsBlobStore.ResumableUpload resumableUpload = mockGcsBlobStore.createResumableUpload(
                    blobName,
                    ifGenerationMatch
                );

                byte[] response = requestBody.utf8ToString().getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.getResponseHeaders()
                    .add(
                        "Location",
                        httpServerUrl(exchange)
                            + "/upload/storage/v1/b/"
                            + bucket
                            + "/o?"
                            + "uploadType=resumable"
                            + "&upload_id="
                            + resumableUpload.uploadId()
                    );
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("PUT /upload/storage/v1/b/" + bucket + "/o?*uploadType=resumable*", request)) {
                // Resumable upload https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI(), params);

                final String contentRangeValue = requireHeader(exchange, "Content-Range");
                final HttpHeaderParser.ContentRange contentRange = HttpHeaderParser.parseContentRangeHeader(contentRangeValue);
                if (contentRange == null) {
                    throw failAndThrow("Invalid Content-Range: " + contentRangeValue);
                }

                final MockGcsBlobStore.UpdateResponse updateResponse = mockGcsBlobStore.updateResumableUpload(
                    params.get("upload_id"),
                    contentRange,
                    requestBody
                );

                if (updateResponse.rangeHeader() != null) {
                    exchange.getResponseHeaders().add("Range", updateResponse.rangeHeader().headerString());
                }
                exchange.getResponseHeaders().add("x-goog-stored-content-length", String.valueOf(updateResponse.storedContentLength()));
                exchange.sendResponseHeaders(updateResponse.statusCode(), -1);
            } else {
                exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
            }
        } catch (MockGcsBlobStore.GcsRestException e) {
            sendError(exchange, e);
        } finally {
            exchange.close();
        }
    }

    private void writeBlobVersionAsJson(HttpExchange exchange, MockGcsBlobStore.BlobVersion newBlobVersion) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            writeBlobAsXContent(newBlobVersion, builder, bucket);
            BytesReference responseBytes = BytesReference.bytes(builder);
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), responseBytes.length());
            responseBytes.writeTo(exchange.getResponseBody());
        }
    }

    record ListBlobsResponse(String bucket, MockGcsBlobStore.PageOfBlobs pageOfBlobs) implements ToXContent {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("kind", "storage#objects");
            if (pageOfBlobs.nextPageToken() != null) {
                builder.field("nextPageToken", pageOfBlobs.nextPageToken());
            }
            builder.startArray("items");
            for (MockGcsBlobStore.BlobVersion blobVersion : pageOfBlobs().blobs()) {
                writeBlobAsXContent(blobVersion, builder, bucket);
            }
            builder.endArray();
            builder.field("prefixes", pageOfBlobs.prefixes());
            builder.endObject();
            return builder;
        }
    }

    private static void writeBlobAsXContent(MockGcsBlobStore.BlobVersion blobVersion, XContentBuilder builder, String bucket)
        throws IOException {
        builder.startObject();
        builder.field("kind", "storage#object");
        builder.field("bucket", bucket);
        builder.field("name", blobVersion.path());
        builder.field("id", blobVersion.path());
        builder.field("size", String.valueOf(blobVersion.contents().length()));
        builder.field("generation", String.valueOf(blobVersion.generation()));
        builder.endObject();
    }

    private void sendError(HttpExchange exchange, MockGcsBlobStore.GcsRestException e) throws IOException {
        final String responseBody = Strings.format("""
            {
                "error": {
                    "errors": [],
                    "code": %d,
                    "message": "%s"
                }
            }
            """, e.getStatus().getStatus(), e.getMessage());
        exchange.sendResponseHeaders(e.getStatus().getStatus(), responseBody.length());
        exchange.getResponseBody().write(responseBody.getBytes(UTF_8));
    }

    public Map<String, BytesReference> blobs() {
        return mockGcsBlobStore.listBlobs()
            .stream()
            .collect(Collectors.toMap(MockGcsBlobStore.BlobVersion::path, MockGcsBlobStore.BlobVersion::contents));
    }

    private static String httpServerUrl(final HttpExchange exchange) {
        return "http://" + exchange.getRequestHeaders().get("HOST").get(0);
    }

    private static String requireHeader(HttpExchange exchange, String headerName) {
        final String headerValue = exchange.getRequestHeaders().getFirst(headerName);
        if (headerValue != null) {
            return headerValue;
        }
        throw failAndThrow("Missing required header: " + headerName);
    }

    private static Long parseOptionalLongParameter(HttpExchange exchange, String parameterName) {
        final Map<String, String> params = new HashMap<>();
        RestUtils.decodeQueryString(exchange.getRequestURI(), params);
        if (params.containsKey(parameterName)) {
            try {
                return Long.parseLong(params.get(parameterName));
            } catch (NumberFormatException e) {
                throw failAndThrow("Invalid long parameter: " + parameterName);
            }
        }
        return null;
    }
}

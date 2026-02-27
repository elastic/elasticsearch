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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.fixture.HttpHeaderParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static fixture.gcs.MockGcsBlobStore.failAndThrow;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.rest.RestStatus.BAD_GATEWAY;
import static org.elasticsearch.rest.RestStatus.GATEWAY_TIMEOUT;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.NO_CONTENT;
import static org.elasticsearch.rest.RestStatus.REQUEST_TIMEOUT;
import static org.elasticsearch.rest.RestStatus.SERVICE_UNAVAILABLE;
import static org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS;

/**
 * Minimal HTTP handler that acts as a Google Cloud Storage compliant server
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageHttpHandler implements HttpHandler {

    private static final String CRLF = "\r\n";

    private static final String IF_GENERATION_MATCH = "ifGenerationMatch";
    private static final String GENERATION = "generation";

    private final AtomicInteger defaultPageLimit = new AtomicInteger(1_000);
    private final MockGcsBlobStore mockGcsBlobStore;
    private final String bucket;

    // track delete failures for individual blobs to avoid unbounded retries
    private final Map<String, Integer> batchDeleteFailureCounters = new HashMap<>();
    // maximum number of delete failures for individual blob
    private static final int MAX_DELETE_FAILURES = 3;

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
                // https://docs.cloud.google.com/storage/docs/batch#http
                final var boundary = MultipartContent.Reader.getBoundary(exchange);
                final var batchReader = MultipartContent.Reader.readStream(boundary, requestBody.streamInput());
                final var responseStream = new ByteArrayOutputStream();
                final var batchWriter = new MultipartContent.Writer(boundary, responseStream);
                // allow some batches to proceed without partial failures
                final var allowPartialFailures = ESTestCase.randomBoolean();
                while (batchReader.hasNext()) {
                    final var batchItem = batchReader.next();
                    final var contentId = batchItem.headers().get("content-id");
                    // batch supports only deletions
                    final var objectName = parseBatchItemDeleteObject(bucket, batchItem.content());
                    final var deleteStatus = allowPartialFailures ? deleteObjectOrRandomlyFail(objectName) : deleteObject(objectName);
                    final var partHeaders = new LinkedHashMap<String, String>() {
                        {
                            put("content-type", "application/http");
                            put("content-id", "response-" + contentId);
                        }
                    };
                    final var partContent = deleteItemStatusToHttpContent(deleteStatus);
                    batchWriter.write(MultipartContent.Part.of(partHeaders, partContent));
                }
                batchWriter.end();

                byte[] response = responseStream.toByteArray();
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
            } else if (Regex.simpleMatch("DELETE /storage/v1/b/" + bucket + "/o*", request)) {
                final var object = readObjectName(request);
                // don't fail deletes here, fixture will inject failures before reaching this point
                final var deleteStatus = deleteObject(object);
                exchange.sendResponseHeaders(deleteStatus.getStatus(), -1);
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

    // Example of request line
    static final Pattern METHOD_BUCKET_OBJECT_PATTERN = Pattern.compile(
        "(?<method>\\w+) .+/v1/b/(?<bucket>[a-zA-Z0-9._-]+)/o/" + "(?<object>[^?^\\s]+)"
    );

    private String readObjectName(String requestLine) {
        var m = METHOD_BUCKET_OBJECT_PATTERN.matcher(requestLine);
        if (m.find()) {
            final var _bucket = m.group("bucket");
            if (bucket.equals(_bucket) == false) {
                throw failAndThrow("bucket name does not match, expected: " + bucket + ", got: " + _bucket);
            }
            return URLDecoder.decode(m.group("object"), UTF_8);
        } else {
            throw failAndThrow("cannot parse bucket and object from uri: " + requestLine);
        }
    }

    private RestStatus deleteObjectOrRandomlyFail(String objectName) {
        synchronized (batchDeleteFailureCounters) {
            final var failures = batchDeleteFailureCounters.getOrDefault(objectName, 0);
            // 10% failure is an arbitrary number, not too small, not too big
            if (fail10Percent()) {
                if (failures < MAX_DELETE_FAILURES) {
                    batchDeleteFailureCounters.put(objectName, failures + 1);
                    return randomRetryableError();
                }
            }
            return deleteObject(objectName);
        }
    }

    RestStatus deleteObject(String objectName) {
        synchronized (batchDeleteFailureCounters) {
            batchDeleteFailureCounters.remove(objectName);
            final var deleted = mockGcsBlobStore.deleteBlob(objectName);
            return deleted ? NO_CONTENT : NOT_FOUND;
        }
    }

    static RestStatus randomRetryableError() {
        return ESTestCase.randomFrom(
            REQUEST_TIMEOUT,
            TOO_MANY_REQUESTS,
            INTERNAL_SERVER_ERROR,
            BAD_GATEWAY,
            SERVICE_UNAVAILABLE,
            GATEWAY_TIMEOUT
        );
    }

    // returns HTTP content for a part in multipart batch delete response
    static String deleteItemStatusToHttpContent(RestStatus itemStatus) {
        final var responseText = new StringBuilder();
        final var statusLine = switch (itemStatus) {
            case NO_CONTENT -> "204 No Content";
            case NOT_FOUND -> "404 Not Found";
            case REQUEST_TIMEOUT -> "408 Request Timeout";
            case TOO_MANY_REQUESTS -> "429 Too Many Requests";
            case INTERNAL_SERVER_ERROR -> "500 Internal Server Error";
            case BAD_GATEWAY -> "502 Bad Gateway";
            case SERVICE_UNAVAILABLE -> "503 Service Unavailable";
            case GATEWAY_TIMEOUT -> "504 Gateway Timeout";
            default -> throw failAndThrow("HTTP status line is not implemented for " + itemStatus);
        };
        responseText.append("HTTP/1.1 ").append(statusLine).append(CRLF);
        // an error must contain a JSON object describing error, a minimal description needs at least an error code
        if (itemStatus != NO_CONTENT) {
            final var errorObj = """
                {
                  "error" : {
                    "code": $code
                  }
                }
                """.replace("$code", Integer.toString(itemStatus.getStatus()));
            responseText.append("content-type: application/json")
                .append(CRLF)
                .append("content-length: ")
                .append(errorObj.length())
                .append(CRLF)
                .append(CRLF)
                .append(errorObj)
                .append(CRLF);
        }
        return responseText.toString();
    }

    static boolean fail10Percent() {
        return ESTestCase.between(1, 10) == 1;
    }

    static String parseBatchItemDeleteObject(String bucket, BytesReference bytes) {
        final var s = bytes.utf8ToString();
        return s.lines().findFirst().map(line -> {
            var matcher = METHOD_BUCKET_OBJECT_PATTERN.matcher(line);
            if (matcher.find() == false) {
                throw failAndThrow("Cannot parse batch item HTTP line: " + line);
            }
            var method = matcher.group("method");
            if (method.equals("DELETE") == false) {
                throw failAndThrow("Expected DELETE item, found " + line);
            }
            var _bucket = matcher.group("bucket");
            if (bucket.equals(_bucket) == false) {
                throw failAndThrow("Bucket does not match expected: " + bucket + ", got: " + _bucket);
            }
            return URLDecoder.decode(matcher.group("object"), UTF_8);

        }).orElseThrow(() -> failAndThrow("Empty batch item"));
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

    /**
     * Directly insert a blob into the mock store. Useful for pre-loading test fixtures
     * without going through the HTTP API.
     */
    public void putBlob(String path, BytesReference contents) {
        mockGcsBlobStore.updateBlob(path, null, contents);
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

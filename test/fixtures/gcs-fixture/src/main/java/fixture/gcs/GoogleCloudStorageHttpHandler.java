/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Minimal HTTP handler that acts as a Google Cloud Storage compliant server
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageHttpHandler implements HttpHandler {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageHttpHandler.class);

    private static final Pattern RANGE_MATCHER = Pattern.compile("bytes=([0-9]*)-([0-9]*)");

    private final ConcurrentMap<String, BytesReference> blobs;
    private final String bucket;

    public GoogleCloudStorageHttpHandler(final String bucket) {
        this.bucket = Objects.requireNonNull(bucket);
        this.blobs = new ConcurrentHashMap<>();
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
                final BytesReference blob = blobs.get(key);
                if (blob == null) {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                } else {
                    final byte[] response = buildBlobInfoJson(key, blob.length()).getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                }
            } else if (Regex.simpleMatch("GET /storage/v1/b/" + bucket + "/o*", request)) {
                // List Objects https://cloud.google.com/storage/docs/json_api/v1/objects/list
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                final String prefix = params.getOrDefault("prefix", "");
                final String delimiter = params.get("delimiter");

                final Set<String> prefixes = new HashSet<>();
                final List<String> listOfBlobs = new ArrayList<>();

                for (final Map.Entry<String, BytesReference> blob : blobs.entrySet()) {
                    final String blobName = blob.getKey();
                    if (prefix.isEmpty() || blobName.startsWith(prefix)) {
                        int delimiterPos = (delimiter != null) ? blobName.substring(prefix.length()).indexOf(delimiter) : -1;
                        if (delimiterPos > -1) {
                            prefixes.add("\"" + blobName.substring(0, prefix.length() + delimiterPos + 1) + "\"");
                        } else {
                            listOfBlobs.add(buildBlobInfoJson(blobName, blob.getValue().length()));
                        }
                    }
                }

                byte[] response = ("""
                    {"kind":"storage#objects","items":[%s],"prefixes":[%s]}\
                    """.formatted(String.join(",", listOfBlobs), String.join(",", prefixes))).getBytes(UTF_8);

                exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("GET /storage/v1/b/" + bucket + "*", request)) {
                // GET Bucket https://cloud.google.com/storage/docs/json_api/v1/buckets/get
                throw new AssertionError("Should not call get bucket API");

            } else if (Regex.simpleMatch("GET /download/storage/v1/b/" + bucket + "/o/*", request)) {
                // Download Object https://cloud.google.com/storage/docs/request-body
                BytesReference blob = blobs.get(exchange.getRequestURI().getPath().replace("/download/storage/v1/b/" + bucket + "/o/", ""));
                if (blob != null) {
                    final String range = exchange.getRequestHeaders().getFirst("Range");
                    final int offset;
                    final int end;
                    if (range == null) {
                        offset = 0;
                        end = blob.length() - 1;
                    } else {
                        Matcher matcher = RANGE_MATCHER.matcher(range);
                        if (matcher.find() == false) {
                            throw new AssertionError("Range bytes header does not match expected format: " + range);
                        }
                        offset = Integer.parseInt(matcher.group(1));
                        end = Integer.parseInt(matcher.group(2));
                    }
                    BytesReference response = blob;
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    final int bufferedLength = response.length();
                    if (offset > 0 || bufferedLength > end) {
                        response = response.slice(offset, Math.min(end + 1 - offset, bufferedLength - offset));
                    }
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length());
                    response.writeTo(exchange.getResponseBody());
                } else {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                }

            } else if (Regex.simpleMatch("POST /batch/storage/v1", request)) {
                // Batch https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
                final String uri = "/storage/v1/b/" + bucket + "/o/";
                final StringBuilder batch = new StringBuilder();
                for (String line : Streams.readAllLines(requestBody.streamInput())) {
                    if (line.length() == 0 || line.startsWith("--") || line.toLowerCase(Locale.ROOT).startsWith("content")) {
                        batch.append(line).append("\r\n");
                    } else if (line.startsWith("DELETE")) {
                        final String name = line.substring(line.indexOf(uri) + uri.length(), line.lastIndexOf(" HTTP"));
                        if (Strings.hasText(name)) {
                            blobs.remove(URLDecoder.decode(name, UTF_8));
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
                // Multipart upload
                Optional<Tuple<String, BytesReference>> content = parseMultipartRequestBody(requestBody.streamInput());
                if (content.isPresent()) {
                    blobs.put(content.get().v1(), content.get().v2());

                    byte[] response = """
                        {"bucket":"%s","name":"%s"}
                        """.formatted(bucket, content.get().v1()).getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                } else {
                    throw new AssertionError(
                        "Could not read multi-part request to ["
                            + request
                            + "] with headers ["
                            + new HashMap<>(exchange.getRequestHeaders())
                            + "]"
                    );
                }

            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/" + bucket + "/*uploadType=resumable*", request)) {
                // Resumable upload initialization https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                final String blobName = params.get("name");
                blobs.put(blobName, BytesArray.EMPTY);

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
                            + UUIDs.randomBase64UUID()
                            + "&test_blob_name="
                            + blobName
                    ); // not a Google Storage parameter, but it allows to pass the blob name
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("PUT /upload/storage/v1/b/" + bucket + "/o?*uploadType=resumable*", request)) {
                // Resumable upload https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                final String blobName = params.get("test_blob_name");
                if (blobs.containsKey(blobName) == false) {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                    return;
                }
                BytesReference blob = blobs.get(blobName);
                final String range = exchange.getRequestHeaders().getFirst("Content-Range");
                final Integer limit = getContentRangeLimit(range);

                blob = CompositeBytesReference.of(blob, requestBody);
                blobs.put(blobName, blob);

                if (limit == null) {
                    if ("bytes */*".equals(range) == false) {
                        final int start = getContentRangeStart(range);
                        final int end = getContentRangeEnd(range);
                        exchange.getResponseHeaders().add("Range", String.format(Locale.ROOT, "bytes=%d-%d", start, end));
                    }
                    exchange.getResponseHeaders().add("Content-Length", "0");
                    exchange.sendResponseHeaders(308 /* Resume Incomplete */, -1);
                } else {
                    if (limit > blob.length()) {
                        throw new AssertionError("Requesting more bytes than available for blob");
                    }
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                }
            } else {
                exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
            }
        } finally {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been fully read here but saw [" + read + "]";
            exchange.close();
        }
    }

    private String buildBlobInfoJson(String blobName, int size) {
        return """
            {"kind":"storage#object","bucket":"%s","name":"%s","id":"%s","size":"%s"}
            """.formatted(bucket, blobName, blobName, size);
    }

    public Map<String, BytesReference> blobs() {
        return blobs;
    }

    private String httpServerUrl(final HttpExchange exchange) {
        return "http://" + exchange.getRequestHeaders().get("HOST").get(0);
    }

    private static final Pattern NAME_PATTERN = Pattern.compile("\"name\":\"([^\"]*)\"");

    public static Optional<Tuple<String, BytesReference>> parseMultipartRequestBody(final InputStream requestBody) throws IOException {
        Tuple<String, BytesReference> content = null;
        final BytesReference fullRequestBody;
        try (InputStream in = new GZIPInputStream(requestBody)) {
            fullRequestBody = Streams.readFully(in);
        }
        String name = null;
        boolean skippedEmptyLine = false;
        int startPos = 0;
        int endPos = 0;
        while (startPos < fullRequestBody.length()) {
            do {
                endPos = fullRequestBody.indexOf((byte) '\r', endPos + 1);
            } while (endPos >= 0 && fullRequestBody.get(endPos + 1) != '\n');
            boolean markAndContinue = false;
            final String bucketPrefix = "{\"bucket\":";
            if (startPos > 0) {
                startPos += 2;
            }
            if (name == null || skippedEmptyLine == false) {
                if ((skippedEmptyLine == false && endPos == startPos)
                    || (fullRequestBody.get(startPos) == '-' && fullRequestBody.get(startPos + 1) == '-')) {
                    markAndContinue = true;
                } else {
                    final String start = fullRequestBody.slice(startPos, Math.min(endPos - startPos, bucketPrefix.length())).utf8ToString();
                    if (start.toLowerCase(Locale.ROOT).startsWith("content")) {
                        markAndContinue = true;
                    } else if (start.startsWith(bucketPrefix)) {
                        markAndContinue = true;
                        final String line = fullRequestBody.slice(
                            startPos + bucketPrefix.length(),
                            endPos - startPos - bucketPrefix.length()
                        ).utf8ToString();
                        Matcher matcher = NAME_PATTERN.matcher(line);
                        if (matcher.find()) {
                            name = matcher.group(1);
                        }
                    }
                }
                skippedEmptyLine = markAndContinue && endPos == startPos;
                startPos = endPos;
            } else {
                while (isEndOfPart(fullRequestBody, endPos) == false) {
                    endPos = fullRequestBody.indexOf((byte) '\r', endPos + 1);
                }
                content = Tuple.tuple(name, fullRequestBody.slice(startPos, endPos - startPos));
                break;
            }
        }
        if (content == null) {
            final InputStream stream = fullRequestBody.streamInput();
            logger.warn(
                () -> new ParameterizedMessage(
                    "Failed to find multi-part upload in [{}]",
                    new BufferedReader(new InputStreamReader(stream)).lines().collect(Collectors.joining("\n"))
                )
            );
        }
        return Optional.ofNullable(content);
    }

    private static final byte[] END_OF_PARTS_MARKER = "\r\n--__END_OF_PART__".getBytes(UTF_8);

    private static boolean isEndOfPart(BytesReference fullRequestBody, int endPos) {
        for (int i = 0; i < END_OF_PARTS_MARKER.length; i++) {
            final byte b = END_OF_PARTS_MARKER[i];
            if (fullRequestBody.get(endPos + i) != b) {
                return false;
            }
        }
        return true;
    }

    private static final Pattern PATTERN_CONTENT_RANGE = Pattern.compile("bytes ([^/]*)/([0-9\\*]*)");
    private static final Pattern PATTERN_CONTENT_RANGE_BYTES = Pattern.compile("([0-9]*)-([0-9]*)");

    private static Integer parse(final Pattern pattern, final String contentRange, final BiFunction<String, String, Integer> fn) {
        final Matcher matcher = pattern.matcher(contentRange);
        if (matcher.matches() == false || matcher.groupCount() != 2) {
            throw new IllegalArgumentException("Unable to parse content range header");
        }
        return fn.apply(matcher.group(1), matcher.group(2));
    }

    public static Integer getContentRangeLimit(final String contentRange) {
        return parse(PATTERN_CONTENT_RANGE, contentRange, (bytes, limit) -> "*".equals(limit) ? null : Integer.parseInt(limit));
    }

    public static int getContentRangeStart(final String contentRange) {
        return parse(
            PATTERN_CONTENT_RANGE,
            contentRange,
            (bytes, limit) -> parse(PATTERN_CONTENT_RANGE_BYTES, bytes, (start, end) -> Integer.parseInt(start))
        );
    }

    public static int getContentRangeEnd(final String contentRange) {
        return parse(
            PATTERN_CONTENT_RANGE,
            contentRange,
            (bytes, limit) -> parse(PATTERN_CONTENT_RANGE_BYTES, bytes, (start, end) -> Integer.parseInt(end))
        );
    }
}

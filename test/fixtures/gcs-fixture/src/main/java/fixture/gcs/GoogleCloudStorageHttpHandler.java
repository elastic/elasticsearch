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
package fixture.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Minimal HTTP handler that acts as a Google Cloud Storage compliant server
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageHttpHandler implements HttpHandler {

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
            final InputStream wrappedRequest = Streams.noCloseStream(exchange.getRequestBody());
            if (Regex.simpleMatch("GET /storage/v1/b/" + bucket + "/o*", request)) {
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
                            listOfBlobs.add("{\"kind\":\"storage#object\","
                                + "\"bucket\":\"" + bucket + "\","
                                + "\"name\":\"" + blobName + "\","
                                + "\"id\":\"" + blobName + "\","
                                + "\"size\":\"" + blob.getValue().length() + "\""
                                + "}");
                        }
                    }
                }

                byte[] response = ("{\"kind\":\"storage#objects\",\"items\":[" +
                    String.join(",", listOfBlobs) +
                    "],\"prefixes\":[" +
                    String.join(",", prefixes) +
                    "]}").getBytes(UTF_8);

                exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("GET /storage/v1/b/" + bucket + "*", request)) {
                // GET Bucket https://cloud.google.com/storage/docs/json_api/v1/buckets/get
                byte[] response = ("{\"kind\":\"storage#bucket\",\"name\":\""+ bucket + "\",\"id\":\"0\"}").getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("GET /download/storage/v1/b/" + bucket + "/o/*", request)) {
                // Download Object https://cloud.google.com/storage/docs/request-body
                BytesReference blob = blobs.get(exchange.getRequestURI().getPath().replace("/download/storage/v1/b/" + bucket + "/o/", ""));
                if (blob != null) {
                    final String range = exchange.getRequestHeaders().getFirst("Range");
                    Matcher matcher = Pattern.compile("bytes=([0-9]*)-([0-9]*)").matcher(range);
                    if (matcher.find() == false) {
                        throw new AssertionError("Range bytes header does not match expected format: " + range);
                    }

                    byte[] response = Integer.parseInt(matcher.group(1)) == 0 ? BytesReference.toBytes(blob) : new byte[0];
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                } else {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                }

            } else if (Regex.simpleMatch("POST /batch/storage/v1", request)) {
                // Batch https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
                final String uri = "/storage/v1/b/" + bucket + "/o/";
                final StringBuilder batch = new StringBuilder();
                for (String line : Streams.readAllLines(wrappedRequest)) {
                    if (line.length() == 0 || line.startsWith("--") || line.toLowerCase(Locale.ROOT).startsWith("content")) {
                        batch.append(line).append('\n');
                    } else if (line.startsWith("DELETE")) {
                        final String name = line.substring(line.indexOf(uri) + uri.length(), line.lastIndexOf(" HTTP"));
                        if (Strings.hasText(name)) {
                            blobs.remove(URLDecoder.decode(name, UTF_8));
                            batch.append("HTTP/1.1 204 NO_CONTENT").append('\n');
                            batch.append('\n');
                        }
                    }
                }
                byte[] response = batch.toString().getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", exchange.getRequestHeaders().getFirst("Content-Type"));
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/" + bucket + "/*uploadType=multipart*", request)) {
                // Multipart upload
                Optional<Tuple<String, BytesArray>> content = parseMultipartRequestBody(wrappedRequest);
                if (content.isPresent()) {
                    blobs.put(content.get().v1(), content.get().v2());

                    byte[] response = ("{\"bucket\":\"" + bucket + "\",\"name\":\"" + content.get().v1() + "\"}").getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                } else {
                    throw new AssertionError("Could not read multi-part request to [" + request + "] with headers ["
                        + new HashMap<>(exchange.getRequestHeaders()) + "]");
                }

            } else if (Regex.simpleMatch("POST /upload/storage/v1/b/" + bucket + "/*uploadType=resumable*", request)) {
                // Resumable upload initialization https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                final String blobName = params.get("name");
                blobs.put(blobName, BytesArray.EMPTY);

                byte[] response = Streams.readFully(wrappedRequest).utf8ToString().getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.getResponseHeaders().add("Location", httpServerUrl(exchange) + "/upload/storage/v1/b/" + bucket + "/o?"
                    + "uploadType=resumable"
                    + "&upload_id=" + UUIDs.randomBase64UUID()
                    + "&test_blob_name=" + blobName); // not a Google Storage parameter, but it allows to pass the blob name
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
                byte[] blob = BytesReference.toBytes(blobs.get(blobName));
                final String range = exchange.getRequestHeaders().getFirst("Content-Range");
                final Integer limit = getContentRangeLimit(range);
                final int start = getContentRangeStart(range);
                final int end = getContentRangeEnd(range);

                final ByteArrayOutputStream out = new ByteArrayOutputStream() {
                    @Override
                    public byte[] toByteArray() {
                        return buf;
                    }
                };
                long bytesRead = Streams.copy(wrappedRequest, out, new byte[128]);
                int length = Math.max(end + 1, limit != null ? limit : 0);
                if ((int) bytesRead > length) {
                    throw new AssertionError("Requesting more bytes than available for blob");
                }
                if (length > blob.length) {
                    blob = ArrayUtil.growExact(blob, length);
                }
                System.arraycopy(out.toByteArray(), 0, blob, start, Math.toIntExact(bytesRead));
                blobs.put(blobName, new BytesArray(blob));

                if (limit == null) {
                    exchange.getResponseHeaders().add("Range", String.format(Locale.ROOT, "bytes=%d/%d", start, end));
                    exchange.getResponseHeaders().add("Content-Length", "0");
                    exchange.sendResponseHeaders(308 /* Resume Incomplete */, -1);
                } else {
                    if (limit > blob.length) {
                        throw new AssertionError("Requesting more bytes than available for blob");
                    }
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                }
            } else {
                exchange.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
            }
        } finally {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been fully read here but saw [" + read + "]";
            exchange.close();
        }
    }

    public Map<String, BytesReference> blobs() {
        return blobs;
    }

    private String httpServerUrl(final HttpExchange exchange) {
        final InetSocketAddress address = exchange.getLocalAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    private static final Pattern NAME_PATTERN = Pattern.compile("\"name\":\"([^\"]*)\"");

    public static Optional<Tuple<String, BytesArray>> parseMultipartRequestBody(final InputStream requestBody) throws IOException {
        Tuple<String, BytesArray> content = null;
        try (BufferedInputStream in = new BufferedInputStream(new GZIPInputStream(requestBody))) {
            String name = null;
            int read;
            ByteArrayOutputStream out = new ByteArrayOutputStream() {
                @Override
                public byte[] toByteArray() {
                    return buf;
                }
            };
            while ((read = in.read()) != -1) {
                out.reset();
                boolean markAndContinue = false;
                do { // search next consecutive {carriage return, new line} chars and stop
                    if ((char) read == '\r') {
                        int next = in.read();
                        if (next != -1) {
                            if (next == '\n') {
                                break;
                            }
                            out.write(read);
                            out.write(next);
                            continue;
                        }
                    }
                    out.write(read);
                } while ((read = in.read()) != -1);
                final String bucketPrefix = "{\"bucket\":";
                final String start = new String(out.toByteArray(), 0, Math.min(out.size(), bucketPrefix.length()), UTF_8);
                if (start.length() == 0 || start.equals("\r\n") || start.startsWith("--")
                    || start.toLowerCase(Locale.ROOT).startsWith("content")) {
                    markAndContinue = true;
                } else if (start.startsWith(bucketPrefix)) {
                    markAndContinue = true;
                    final String line = new String(out.toByteArray(), bucketPrefix.length(), out.size() - bucketPrefix.length(), UTF_8);
                    Matcher matcher = NAME_PATTERN.matcher(line);
                    if (matcher.find()) {
                        name = matcher.group(1);
                    }
                }
                if (markAndContinue) {
                    in.mark(Integer.MAX_VALUE);
                    continue;
                }
                if (name != null) {
                    in.reset();
                    out.reset();
                    while ((read = in.read()) != -1) {
                        out.write(read);
                    }
                    // removes the trailing end "\r\n--__END_OF_PART__--\r\n" which is 23 bytes long
                    content = Tuple.tuple(name, new BytesArray(Arrays.copyOf(out.toByteArray(), out.size() - 23)));
                    break;
                }
            }
        }
        return Optional.ofNullable(content);
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
        return parse(PATTERN_CONTENT_RANGE, contentRange,
            (bytes, limit) -> parse(PATTERN_CONTENT_RANGE_BYTES, bytes,
                (start, end) -> Integer.parseInt(start)));
    }

    public static int getContentRangeEnd(final String contentRange) {
        return parse(PATTERN_CONTENT_RANGE, contentRange,
            (bytes, limit) -> parse(PATTERN_CONTENT_RANGE_BYTES, bytes,
                (start, end) -> Integer.parseInt(end)));
    }
}

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
package fixture.s3;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Minimal HTTP handler that acts as a S3 compliant server
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
public class S3HttpHandler implements HttpHandler {

    private final String bucket;
    private final String path;

    private final ConcurrentMap<String, BytesReference> blobs = new ConcurrentHashMap<>();

    public S3HttpHandler(final String bucket) {
        this(bucket, null);
    }

    public S3HttpHandler(final String bucket, @Nullable final String basePath) {
        this.bucket = Objects.requireNonNull(bucket);
        this.path = bucket + (basePath != null && basePath.isEmpty() == false ? "/" + basePath : "");
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
        if (request.startsWith("GET") || request.startsWith("HEAD") || request.startsWith("DELETE")) {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been empty but saw [" + read + "]";
        }
        try {
            if (Regex.simpleMatch("POST /" + path + "/*?uploads", request)) {
                final String uploadId = UUIDs.randomBase64UUID();
                byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<InitiateMultipartUploadResult>\n" +
                    "  <Bucket>" + bucket + "</Bucket>\n" +
                    "  <Key>" + exchange.getRequestURI().getPath() + "</Key>\n" +
                    "  <UploadId>" + uploadId + "</UploadId>\n" +
                    "</InitiateMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                blobs.put(multipartKey(uploadId, 0), BytesArray.EMPTY);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("PUT /" + path + "/*?uploadId=*&partNumber=*", request)) {
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                final String uploadId = params.get("uploadId");
                if (blobs.containsKey(multipartKey(uploadId, 0))) {
                    final Tuple<String, BytesReference> blob = parseRequestBody(exchange);
                    final int partNumber = Integer.parseInt(params.get("partNumber"));
                    blobs.put(multipartKey(uploadId, partNumber), blob.v2());
                    exchange.getResponseHeaders().add("ETag", blob.v1());
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                } else {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                }

            } else if (Regex.simpleMatch("POST /" + path + "/*?uploadId=*", request)) {
                Streams.readFully(exchange.getRequestBody());
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                final String uploadId = params.get("uploadId");

                final int nbParts = blobs.keySet().stream()
                    .filter(blobName -> blobName.startsWith(uploadId))
                    .map(blobName -> blobName.replaceFirst(uploadId + '\n', ""))
                    .mapToInt(Integer::parseInt)
                    .max()
                    .orElse(0);

                final ByteArrayOutputStream blob = new ByteArrayOutputStream();
                for (int partNumber = 0; partNumber <= nbParts; partNumber++) {
                    BytesReference part = blobs.remove(multipartKey(uploadId, partNumber));
                    if (part == null) {
                        throw new AssertionError("Upload part is null");
                    }
                    part.writeTo(blob);
                }
                blobs.put(exchange.getRequestURI().getPath(), new BytesArray(blob.toByteArray()));

                byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<CompleteMultipartUploadResult>\n" +
                    "  <Bucket>" + bucket + "</Bucket>\n" +
                    "  <Key>" + exchange.getRequestURI().getPath() + "</Key>\n" +
                    "</CompleteMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("PUT /" + path + "/*", request)) {
                final Tuple<String, BytesReference> blob = parseRequestBody(exchange);
                blobs.put(exchange.getRequestURI().toString(), blob.v2());
                exchange.getResponseHeaders().add("ETag", blob.v1());
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);

            } else if (Regex.simpleMatch("GET /" + bucket + "/?prefix=*", request)) {
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                if (params.get("list-type") != null) {
                    throw new AssertionError("Test must be adapted for GET Bucket (List Objects) Version 2");
                }

                final StringBuilder list = new StringBuilder();
                list.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                list.append("<ListBucketResult>");
                final String prefix = params.get("prefix");
                if (prefix != null) {
                    list.append("<Prefix>").append(prefix).append("</Prefix>");
                }
                final Set<String> commonPrefixes = new HashSet<>();
                final String delimiter = params.get("delimiter");
                if (delimiter != null) {
                    list.append("<Delimiter>").append(delimiter).append("</Delimiter>");
                }
                for (Map.Entry<String, BytesReference> blob : blobs.entrySet()) {
                    if (prefix != null && blob.getKey().startsWith("/" + bucket + "/" + prefix) == false) {
                        continue;
                    }
                    String blobPath = blob.getKey().replace("/" + bucket + "/", "");
                    if (delimiter != null) {
                        int fromIndex = (prefix != null ? prefix.length() : 0);
                        int delimiterPosition = blobPath.indexOf(delimiter, fromIndex);
                        if (delimiterPosition > 0) {
                            commonPrefixes.add(blobPath.substring(0, delimiterPosition) + delimiter);
                            continue;
                        }
                    }
                    list.append("<Contents>");
                    list.append("<Key>").append(blobPath).append("</Key>");
                    list.append("<Size>").append(blob.getValue().length()).append("</Size>");
                    list.append("</Contents>");
                }
                if (commonPrefixes.isEmpty() == false) {
                    list.append("<CommonPrefixes>");
                    commonPrefixes.forEach(commonPrefix -> list.append("<Prefix>").append(commonPrefix).append("</Prefix>"));
                    list.append("</CommonPrefixes>");

                }
                list.append("</ListBucketResult>");

                byte[] response = list.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else if (Regex.simpleMatch("GET /" + path + "/*", request)) {
                final BytesReference blob = blobs.get(exchange.getRequestURI().toString());
                if (blob != null) {
                    final String range = exchange.getRequestHeaders().getFirst("Range");
                    if (range == null) {
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                        blob.writeTo(exchange.getResponseBody());
                    } else {
                        final Matcher matcher = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$").matcher(range);
                        if (matcher.matches() == false) {
                            throw new AssertionError("Bytes range does not match expected pattern: " + range);
                        }

                        final int start = Integer.parseInt(matcher.group(1));
                        final int end = Integer.parseInt(matcher.group(2));

                        final BytesReference rangeBlob = blob.slice(start, end + 1 - start);
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.getResponseHeaders().add("Content-Range", String.format(Locale.ROOT, "bytes %d-%d/%d",
                            start, end, rangeBlob.length()));
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), rangeBlob.length());
                        rangeBlob.writeTo(exchange.getResponseBody());
                    }
                } else {
                    exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                }

            } else if (Regex.simpleMatch("DELETE /" + path + "/*", request)) {
                int deletions = 0;
                for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext(); ) {
                    Map.Entry<String, BytesReference> blob = iterator.next();
                    if (blob.getKey().startsWith(exchange.getRequestURI().toString())) {
                        iterator.remove();
                        deletions++;
                    }
                }
                exchange.sendResponseHeaders((deletions > 0 ? RestStatus.OK : RestStatus.NO_CONTENT).getStatus(), -1);

            } else if (Regex.simpleMatch("POST /" + bucket + "/?delete", request)) {
                final String requestBody = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), UTF_8));

                final StringBuilder deletes = new StringBuilder();
                deletes.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                deletes.append("<DeleteResult>");
                for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext(); ) {
                    Map.Entry<String, BytesReference> blob = iterator.next();
                    String key = blob.getKey().replace("/" + path + "/", "");
                    if (requestBody.contains("<Key>" + key + "</Key>")) {
                        deletes.append("<Deleted><Key>").append(key).append("</Key></Deleted>");
                        iterator.remove();
                    }
                }
                deletes.append("</DeleteResult>");

                byte[] response = deletes.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else {
                exchange.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
            }
        } finally {
            exchange.close();
        }
    }

    public Map<String, BytesReference> blobs() {
        return blobs;
    }

    private static String multipartKey(final String uploadId, int partNumber) {
        return uploadId + "\n" + partNumber;
    }

    private static CheckedInputStream createCheckedInputStream(final InputStream inputStream, final MessageDigest digest) {
        return new CheckedInputStream(inputStream, new Checksum() {
            @Override
            public void update(int b) {
                digest.update((byte) b);
            }

            @Override
            public void update(byte[] b, int off, int len) {
                digest.update(b, off, len);
            }

            @Override
            public long getValue() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void reset() {
                digest.reset();
            }
        });
    }

    private static final Pattern chunkSignaturePattern = Pattern.compile("^([0-9a-z]+);chunk-signature=([^\\r\\n]*)$");

    private static Tuple<String, BytesReference> parseRequestBody(final HttpExchange exchange) throws IOException {
        final BytesReference bytesReference;

        final String headerDecodedContentLength = exchange.getRequestHeaders().getFirst("x-amz-decoded-content-length");
        if (headerDecodedContentLength == null) {
            bytesReference = Streams.readFully(exchange.getRequestBody());
        } else {
            BytesReference cc = Streams.readFully(exchange.getRequestBody());

            final ByteArrayOutputStream blob = new ByteArrayOutputStream();
            try (BufferedInputStream in = new BufferedInputStream(cc.streamInput())) {
                int chunkSize = 0;
                int read;
                while ((read = in.read()) != -1) {
                    boolean markAndContinue = false;
                    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
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

                        final String line = new String(out.toByteArray(), UTF_8);
                        if (line.length() == 0 || line.equals("\r\n")) {
                            markAndContinue = true;
                        } else {
                            Matcher matcher = chunkSignaturePattern.matcher(line);
                            if (matcher.find()) {
                                markAndContinue = true;
                                chunkSize = Integer.parseUnsignedInt(matcher.group(1), 16);
                            }
                        }
                        if (markAndContinue) {
                            in.mark(Integer.MAX_VALUE);
                            continue;
                        }
                    }
                    if (chunkSize > 0) {
                        in.reset();
                        final byte[] buffer = new byte[chunkSize];
                        in.read(buffer, 0, buffer.length);
                        blob.write(buffer);
                        blob.flush();
                        chunkSize = 0;
                    }
                }
            }
            if (blob.size() != Integer.parseInt(headerDecodedContentLength)) {
                throw new IllegalStateException("Something went wrong when parsing the chunked request " +
                    "[bytes read=" + blob.size() + ", expected=" + headerDecodedContentLength + "]");
            }
            bytesReference = new BytesArray(blob.toByteArray());
        }

        final MessageDigest digest = MessageDigests.md5();
        Streams.readFully(createCheckedInputStream(bytesReference.streamInput(), digest));
        return Tuple.tuple(MessageDigests.toHexString(digest.digest()), bytesReference);
    }

    public static void sendError(final HttpExchange exchange,
                                 final RestStatus status,
                                 final String errorCode,
                                 final String message) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/xml");

        final String requestId = exchange.getRequestHeaders().getFirst("x-amz-request-id");
        if (requestId != null) {
            headers.add("x-amz-request-id", requestId);
        }

        if (errorCode == null || "HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status.getStatus(), -1L);
            exchange.close();
        } else {
            final byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>" +
                "<Code>" + errorCode + "</Code>" +
                "<Message>" + message + "</Message>"
                + "<RequestId>" + requestId + "</RequestId>"
                + "</Error>").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.getStatus(), response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        }
    }
}

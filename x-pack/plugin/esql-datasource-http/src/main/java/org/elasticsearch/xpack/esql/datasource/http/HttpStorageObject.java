/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractMeteredStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Executor;

/**
 * StorageObject implementation using HTTP Range requests for efficient partial reads.
 * Uses standard Java HttpClient and InputStream - no custom stream classes needed.
 * <p>
 * Supports:
 * <ul>
 *   <li>Full object reads via GET</li>
 *   <li>Range reads via HTTP Range header for columnar formats</li>
 *   <li>Metadata retrieval via HEAD requests</li>
 * </ul>
 */
public final class HttpStorageObject extends AbstractMeteredStorageObject {

    private final HttpClient client;
    private final StoragePath path;
    private final URI uri;  // Cached URI to avoid repeated parsing
    private final HttpConfiguration config;

    // Cached metadata to avoid repeated HEAD requests
    private Long cachedLength;
    private Instant cachedLastModified;
    private Boolean cachedExists;

    /**
     * Creates an HttpStorageObject without pre-known metadata.
     */
    public HttpStorageObject(HttpClient client, StoragePath path, HttpConfiguration config) {
        if (client == null) {
            throw new IllegalArgumentException("client cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.client = client;
        this.path = path;
        this.uri = URI.create(path.toString());
        this.config = config;
    }

    /**
     * Creates an HttpStorageObject with pre-known length.
     */
    public HttpStorageObject(HttpClient client, StoragePath path, HttpConfiguration config, long length) {
        this(client, path, config);
        this.cachedLength = length;
    }

    /**
     * Creates an HttpStorageObject with pre-known length and last modified time.
     */
    public HttpStorageObject(HttpClient client, StoragePath path, HttpConfiguration config, long length, Instant lastModified) {
        this(client, path, config, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        long startNanos = System.nanoTime();
        long[] bytesHolder = new long[] { 0L };
        try {
            return sendRequest(this::buildGetRequest, HttpResponse.BodyHandlers.ofInputStream(), response -> {
                int statusCode = response.statusCode();
                if (statusCode != HttpStatus.SC_OK) {
                    throw throwReadFailure("Failed to read object from", statusCode, readErrorBody(response.body()));
                }
                OptionalLong contentLength = response.headers().firstValueAsLong(HttpHeaders.CONTENT_LENGTH);
                if (contentLength.isPresent()) {
                    bytesHolder[0] = contentLength.getAsLong();
                }
                return new HttpTransientTypingInputStream(response.body(), path);
            });
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, bytesHolder[0]);
        }
    }

    /** Cap on the error-response body snippet folded into a failure message, in bytes. */
    private static final int MAX_ERROR_BODY_BYTES = 512;

    /**
     * Maps a non-success HTTP status into the exception to surface to ES|QL. A retryable status
     * (5xx/429) becomes an {@link ExternalUnavailableException} (503 — the read may succeed on retry);
     * any other status becomes an {@link IOException}, which the external source operator classifies as
     * a client-class 400. {@code detail} is an optional truncated error-body snippet appended for triage
     * (a raw status alone is opaque; stores typically return a descriptive body). Returns (never throws)
     * so both the synchronous and async read paths can route it.
     */
    private Exception mapReadFailure(String context, int statusCode, String detail) {
        String suffix = (detail == null || detail.isEmpty()) ? "" : ", body: " + detail;
        if (ExternalUnavailableException.isRetryableStatus(statusCode)) {
            boolean throttling = ExternalUnavailableException.isThrottlingStatus(statusCode);
            return new ExternalUnavailableException(
                throttling,
                "HTTP store unavailable reading [{}] (HTTP {}){}",
                path,
                statusCode,
                suffix
            );
        }
        return new IOException(context + " " + path + ", HTTP status: " + statusCode + suffix);
    }

    /**
     * Synchronous-path bridge for {@link #mapReadFailure}: rethrows the mapped exception. The return
     * type lets callers write {@code throw throwReadFailure(...)} so the compiler sees an exit.
     */
    private RuntimeException throwReadFailure(String context, int statusCode, String detail) throws IOException {
        Exception mapped = mapReadFailure(context, statusCode, detail);
        if (mapped instanceof RuntimeException re) {
            throw re;
        }
        throw (IOException) mapped;
    }

    /**
     * Best-effort read of a truncated, UTF-8 error-response body for inclusion in a failure message.
     * Reads at most {@link #MAX_ERROR_BODY_BYTES} and closes the stream. Never throws: error-body
     * extraction must never mask or replace the real failure, so any problem yields {@code null}.
     */
    private static String readErrorBody(InputStream body) {
        if (body == null) {
            return null;
        }
        try (body) {
            byte[] bytes = body.readNBytes(MAX_ERROR_BODY_BYTES);
            if (bytes.length == 0) {
                return null;
            }
            String text = new String(bytes, StandardCharsets.UTF_8).strip();
            return text.isEmpty() ? null : text;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        boolean toEnd = length == READ_TO_END;
        if (toEnd == false && length <= 0) {
            throw new IllegalArgumentException("length must be positive or READ_TO_END, got: " + length);
        }

        long startNanos = System.nanoTime();
        // Bytes: response Content-Length when known, else fall back to the requested range length.
        long[] bytesHolder = new long[] { toEnd ? 0L : length };
        try {
            return sendRequest(() -> buildRangeRequest(position, length), HttpResponse.BodyHandlers.ofInputStream(), response -> {
                int statusCode = response.statusCode();
                OptionalLong contentLength = response.headers().firstValueAsLong(HttpHeaders.CONTENT_LENGTH);
                if (contentLength.isPresent()) {
                    bytesHolder[0] = contentLength.getAsLong();
                }
                // 206 = Partial Content (successful range request)
                // 200 = OK (server doesn't support ranges but returned full content)
                if (statusCode == HttpStatus.SC_PARTIAL_CONTENT) {
                    return new HttpTransientTypingInputStream(response.body(), path);
                } else if (statusCode == HttpStatus.SC_OK) {
                    // Server doesn't support Range requests, skip to position manually. The skip runs on the raw
                    // body (it is open-phase setup, retried by the open loop on failure); typing wraps the
                    // delivered tail so a mid-read drop after the skip resumes byte-exactly.
                    InputStream stream = response.body();
                    long skipped = stream.skip(position);
                    if (skipped != position) {
                        stream.close();
                        throw new IOException("Failed to skip to position " + position + ", only skipped " + skipped + " bytes");
                    }
                    InputStream typed = new HttpTransientTypingInputStream(stream, path);
                    // READ_TO_END: read to the end (no bound); otherwise cap at the requested length.
                    return toEnd ? typed : new BoundedInputStream(typed, length);
                } else if (toEnd && statusCode == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
                    // Open-ended read at/after the end of an (empty or shorter) object: nothing to read. The SPI
                    // contract for an open-ended read past the end is an empty stream.
                    return InputStream.nullInputStream();
                } else {
                    throw throwReadFailure("Range request failed for", statusCode, readErrorBody(response.body()));
                }
            });
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, bytesHolder[0]);
        }
    }

    @Override
    public long length() throws IOException {
        if (cachedLength == null) {
            fetchMetadata();
        }
        if (cachedExists != null && cachedExists == false) {
            throw new IOException("Object not found: " + path);
        }
        return cachedLength;
    }

    @Override
    public Instant lastModified() throws IOException {
        if (cachedLastModified == null) {
            fetchMetadata();
        }
        if (cachedExists != null && cachedExists == false) {
            throw new IOException("Object not found: " + path);
        }
        return cachedLastModified;
    }

    @Override
    public boolean exists() throws IOException {
        if (cachedExists == null) {
            fetchMetadata();
        }
        return cachedExists;
    }

    @Override
    public StoragePath path() {
        return path;
    }

    // === ASYNC API (native implementation using HttpClient.sendAsync) ===

    /**
     * Async byte read using HttpClient.sendAsync() for native non-blocking I/O.
     * <p>
     * This implementation uses Java's built-in async HTTP client to avoid blocking
     * threads during I/O. The executor parameter is ignored since HttpClient manages
     * its own thread pool for async operations (configured at client creation time).
     *
     * @param position the starting byte position
     * @param length the number of bytes to read
     * @param factory produces the destination {@link DirectReadBuffer} for the response body
     * @param executor executor (unused - HttpClient uses executor configured at creation)
     * @param listener callback for the result or failure
     */
    @Override
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        if (position < 0) {
            listener.onFailure(new IllegalArgumentException("position must be non-negative, got: " + position));
            return;
        }
        if (length < 0) {
            listener.onFailure(new IllegalArgumentException("length must be non-negative, got: " + length));
            return;
        }
        if (length > Integer.MAX_VALUE) {
            listener.onFailure(new IllegalArgumentException("length must fit in an int for async reads, got: " + length));
            return;
        }

        HttpRequest request = buildRangeRequest(position, length);

        long startNanos = System.nanoTime();
        onReadComplete(
            client.sendAsync(request, DirectByteBufferBodyHandlers.ofRangeRead(position, (int) length, factory)),
            (response, throwable) -> {
                if (throwable != null) {
                    counters.addRequest(System.nanoTime() - startNanos, 0L);
                    // Wrap with path context so stack-trace-only triage names the offending URL.
                    // The original cause (CompletionException, body subscriber's IOException,
                    // transport error, etc.) is preserved in the cause chain.
                    listener.onFailure(new IOException("HTTP read failed for " + path, throwable));
                    return;
                }

                int statusCode = response.statusCode();
                // The DirectByteBufferBodyHandlers.ofRangeRead handler already performs the range
                // slicing internally for both 206 (server-side range) and 200 (full body) responses,
                // returning a DirectReadBuffer scoped to the requested window.
                if (statusCode == HttpStatus.SC_PARTIAL_CONTENT || statusCode == HttpStatus.SC_OK) {
                    deliverRead(listener, response.body(), startNanos);
                } else {
                    counters.addRequest(System.nanoTime() - startNanos, 0L);
                    // Discarding subscriber returned no allocator-backed memory but close()-ing is a no-op safe call.
                    response.body().close();
                    listener.onFailure(new IOException("Range request failed for " + path + ", HTTP status: " + statusCode));
                }
            }
        );
    }

    /**
     * Returns true - HttpStorageObject has native async support via HttpClient.sendAsync().
     */
    @Override
    public boolean supportsNativeAsync() {
        return true;
    }

    // === Private helper methods ===

    /**
     * Builds a simple GET request without Range header.
     */
    private HttpRequest buildGetRequest() {
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(uri).GET().timeout(config.requestTimeout());
        addCustomHeaders(builder);
        return builder.build();
    }

    /**
     * Builds a GET request with Range header for partial content.
     */
    private HttpRequest buildRangeRequest(long position, long length) {
        // HTTP Range uses inclusive end: "bytes=start-end". READ_TO_END is the open-ended form "bytes=start-".
        String rangeValue = length == READ_TO_END ? "bytes=" + position + "-" : "bytes=" + position + "-" + (position + length - 1);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(uri)
            .header(HttpHeaders.RANGE, rangeValue)
            .GET()
            .timeout(config.requestTimeout());
        addCustomHeaders(builder);
        return builder.build();
    }

    /**
     * Builds a HEAD request for metadata retrieval.
     */
    private HttpRequest buildHeadRequest() {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(uri)
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .timeout(config.requestTimeout());
        addCustomHeaders(builder);
        return builder.build();
    }

    /**
     * Adds custom headers from configuration to the request builder.
     */
    private void addCustomHeaders(HttpRequest.Builder builder) {
        Map<String, String> headers = config.customHeaders();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.header(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Sends a synchronous HTTP request with proper interrupt handling.
     * <p>
     * This method centralizes the try/catch for InterruptedException, ensuring:
     * <ul>
     *   <li>The interrupt flag is restored via Thread.currentThread().interrupt()</li>
     *   <li>The exception is wrapped in IOException to match the interface contract</li>
     * </ul>
     *
     * @param requestSupplier supplies the HTTP request to send
     * @param bodyHandler handles the response body
     * @param responseHandler processes the response and returns the result
     * @return the result from responseHandler
     * @throws IOException on I/O errors or if interrupted
     */
    private <T, R> R sendRequest(
        CheckedFunction<Void, HttpRequest, IOException> requestSupplier,
        HttpResponse.BodyHandler<T> bodyHandler,
        CheckedFunction<HttpResponse<T>, R, IOException> responseHandler
    ) throws IOException {
        HttpRequest request = requestSupplier.apply(null);
        try {
            HttpResponse<T> response = client.send(request, bodyHandler);
            return responseHandler.apply(response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request interrupted for " + path, e);
        }
    }

    /**
     * Overload for request suppliers that don't throw.
     */
    @FunctionalInterface
    private interface RequestSupplier {
        HttpRequest get();
    }

    private <T, R> R sendRequest(
        RequestSupplier requestSupplier,
        HttpResponse.BodyHandler<T> bodyHandler,
        CheckedFunction<HttpResponse<T>, R, IOException> responseHandler
    ) throws IOException {
        HttpRequest request = requestSupplier.get();
        try {
            HttpResponse<T> response = client.send(request, bodyHandler);
            return responseHandler.apply(response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request interrupted for " + path, e);
        }
    }

    /**
     * Fetches metadata via HEAD request and caches the results.
     */
    private void fetchMetadata() throws IOException {
        sendRequest(this::buildHeadRequest, HttpResponse.BodyHandlers.discarding(), response -> {
            int statusCode = response.statusCode();
            if (statusCode == HttpStatus.SC_OK) {
                cachedExists = true;

                // Extract Content-Length
                OptionalLong contentLength = response.headers().firstValueAsLong(HttpHeaders.CONTENT_LENGTH);
                if (contentLength.isPresent() == false) {
                    throw new IOException("Server did not return " + HttpHeaders.CONTENT_LENGTH + " for " + path);
                }
                cachedLength = contentLength.getAsLong();

                // Extract Last-Modified (optional)
                java.util.Optional<String> lastModified = response.headers().firstValue(HttpHeaders.LAST_MODIFIED);
                cachedLastModified = lastModified.isPresent() ? parseHttpDate(lastModified.get()) : null;
            } else if (statusCode == HttpStatus.SC_NOT_FOUND) {
                cachedExists = false;
                cachedLength = 0L;
                cachedLastModified = null;
            } else {
                throw new IOException("HEAD request failed for " + path + ", HTTP status: " + statusCode);
            }
            return null;  // Void return
        });
    }

    /**
     * Parses HTTP date format (RFC 1123).
     * Example: "Wed, 21 Oct 2015 07:28:00 GMT"
     */
    private Instant parseHttpDate(String dateString) {
        try {
            return ZonedDateTime.parse(dateString, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
        } catch (DateTimeParseException e) {
            // If parsing fails, return null rather than throwing
            return null;
        }
    }

    /**
     * InputStream wrapper that limits the number of bytes that can be read.
     * Used when server doesn't support Range requests.
     */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream delegate;
        private long remaining;

        BoundedInputStream(InputStream delegate, long limit) {
            this.delegate = delegate;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = delegate.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            int bytesRead = delegate.read(b, off, toRead);
            if (bytesRead > 0) {
                remaining -= bytesRead;
            }
            return bytesRead;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}

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
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
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
public final class HttpStorageObject implements StorageObject {

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
        return sendRequest(this::buildGetRequest, HttpResponse.BodyHandlers.ofInputStream(), response -> {
            int statusCode = response.statusCode();
            if (statusCode != HttpStatus.SC_OK) {
                throw new IOException("Failed to read object from " + path + ", HTTP status: " + statusCode);
            }
            return response.body();
        });
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }

        return sendRequest(() -> buildRangeRequest(position, length), HttpResponse.BodyHandlers.ofInputStream(), response -> {
            int statusCode = response.statusCode();
            // 206 = Partial Content (successful range request)
            // 200 = OK (server doesn't support ranges but returned full content)
            if (statusCode == HttpStatus.SC_PARTIAL_CONTENT) {
                return response.body();
            } else if (statusCode == HttpStatus.SC_OK) {
                // Server doesn't support Range requests, skip to position manually
                InputStream stream = response.body();
                long skipped = stream.skip(position);
                if (skipped != position) {
                    stream.close();
                    throw new IOException("Failed to skip to position " + position + ", only skipped " + skipped + " bytes");
                }
                // Wrap in a limited stream to ensure we only read 'length' bytes
                return new BoundedInputStream(stream, length);
            } else {
                throw new IOException("Range request failed for " + path + ", HTTP status: " + statusCode);
            }
        });
    }

    @Override
    public long length() throws IOException {
        if (cachedLength == null) {
            fetchMetadata();
        }
        return cachedLength;
    }

    @Override
    public Instant lastModified() throws IOException {
        if (cachedLastModified == null) {
            fetchMetadata();
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
     * @param executor executor (unused - HttpClient uses executor configured at creation)
     * @param listener callback for the result or failure
     */
    @Override
    public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        if (position < 0) {
            listener.onFailure(new IllegalArgumentException("position must be non-negative, got: " + position));
            return;
        }
        if (length < 0) {
            listener.onFailure(new IllegalArgumentException("length must be non-negative, got: " + length));
            return;
        }

        HttpRequest request = buildRangeRequest(position, length);

        // Use native async HTTP - no blocking, no extra threads needed
        client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).whenComplete((response, throwable) -> {
            if (throwable != null) {
                listener.onFailure(throwable instanceof Exception ex ? ex : new RuntimeException(throwable));
                return;
            }

            int statusCode = response.statusCode();
            // 206 = Partial Content (successful range request)
            // 200 = OK (server doesn't support ranges but returned full content - need to slice)
            if (statusCode == HttpStatus.SC_PARTIAL_CONTENT) {
                listener.onResponse(ByteBuffer.wrap(response.body()));
            } else if (statusCode == HttpStatus.SC_OK) {
                // Server doesn't support Range requests, slice the response
                byte[] fullBody = response.body();
                int bodyLength = fullBody.length;
                if (position >= bodyLength) {
                    listener.onFailure(
                        new IOException("Position " + position + " is beyond content length " + bodyLength + " for " + path)
                    );
                    return;
                }
                int actualLength = (int) Math.min(length, bodyLength - position);
                byte[] slice = new byte[actualLength];
                System.arraycopy(fullBody, (int) position, slice, 0, actualLength);
                listener.onResponse(ByteBuffer.wrap(slice));
            } else {
                listener.onFailure(new IOException("Range request failed for " + path + ", HTTP status: " + statusCode));
            }
        });
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
        // HTTP Range uses inclusive end: "bytes=start-end"
        long endPosition = position + length - 1;
        String rangeValue = "bytes=" + position + "-" + endPosition;

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

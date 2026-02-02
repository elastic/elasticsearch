/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.http;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * StorageObject implementation using HTTP Range requests for efficient partial reads.
 * Uses standard Java HttpClient and InputStream - no custom stream classes needed.
 *
 * Supports:
 * - Full object reads via GET
 * - Range reads via HTTP Range header for columnar formats
 * - Metadata retrieval via HEAD requests
 */
public final class HttpStorageObject implements StorageObject {
    private final HttpClient client;
    private final StoragePath path;
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
        // Full object read - no Range header
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(path.toString()))
            .GET()
            .timeout(config.requestTimeout());

        // Add custom headers if configured
        config.customHeaders().forEach(requestBuilder::header);

        HttpRequest request = requestBuilder.build();

        try {
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() != 200) {
                throw new IOException("Failed to read object from " + path + ", HTTP status: " + response.statusCode());
            }

            return response.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted for " + path, e);
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }

        // Range read using HTTP Range header: "bytes=start-end" (inclusive)
        // HTTP Range uses inclusive end, so we need position + length - 1
        long endPosition = position + length - 1;
        String rangeHeader = String.format("bytes=%d-%d", position, endPosition);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(path.toString()))
            .header("Range", rangeHeader)
            .GET()
            .timeout(config.requestTimeout());

        // Add custom headers if configured
        config.customHeaders().forEach(requestBuilder::header);

        HttpRequest request = requestBuilder.build();

        try {
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

            // 206 = Partial Content (successful range request)
            // 200 = OK (server doesn't support ranges but returned full content)
            if (response.statusCode() == 206) {
                return response.body();
            } else if (response.statusCode() == 200) {
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
                throw new IOException("Range request failed for " + path + ", HTTP status: " + response.statusCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Range request interrupted for " + path, e);
        }
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

    /**
     * Fetches metadata via HEAD request and caches the results.
     */
    private void fetchMetadata() throws IOException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(path.toString()))
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .timeout(config.requestTimeout());

        // Add custom headers if configured
        config.customHeaders().forEach(requestBuilder::header);

        HttpRequest request = requestBuilder.build();

        try {
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());

            if (response.statusCode() == 200) {
                cachedExists = true;

                // Extract Content-Length
                cachedLength = response.headers()
                    .firstValueAsLong("Content-Length")
                    .orElseThrow(() -> new IOException("Server did not return Content-Length for " + path));

                // Extract Last-Modified
                cachedLastModified = response.headers().firstValue("Last-Modified").map(this::parseHttpDate).orElse(null);
            } else if (response.statusCode() == 404) {
                cachedExists = false;
                cachedLength = 0L;
                cachedLastModified = null;
            } else {
                throw new IOException("HEAD request failed for " + path + ", HTTP status: " + response.statusCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HEAD request interrupted for " + path, e);
        }
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

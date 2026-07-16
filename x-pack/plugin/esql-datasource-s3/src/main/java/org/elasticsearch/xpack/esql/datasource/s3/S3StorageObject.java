/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractMeteredStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.utils.ContentRangeParser;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.Executor;

/**
 * StorageObject implementation for S3 using AWS SDK v2.
 * Supports full and range reads, metadata retrieval, and optional native async via S3AsyncClient.
 */
public final class S3StorageObject extends AbstractMeteredStorageObject {
    private static final Logger logger = LogManager.getLogger(S3StorageObject.class);

    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final String bucket;
    private final String key;
    private final StoragePath path;

    private volatile Long cachedLength;
    private volatile Instant cachedLastModified;
    private volatile Boolean cachedExists;

    // Retries: the SDK RetryStrategy at the S3Client layer handles them (pinned to Standard in
    // S3StorageProvider#configureCommon). The provider-agnostic RetryPolicy + ResumingInputStream layer that
    // wraps this object adds cross-provider retry/resume on top.

    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path) {
        this(s3Client, null, bucket, key, path);
    }

    public S3StorageObject(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucket, String key, StoragePath path) {
        if (s3Client == null) {
            throw new IllegalArgumentException("s3Client cannot be null");
        }
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("bucket cannot be null or empty");
        }
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.bucket = bucket;
        this.key = key;
        this.path = path;
    }

    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path, long length) {
        this(s3Client, bucket, key, path);
        this.cachedLength = length;
    }

    public S3StorageObject(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucket, String key, StoragePath path, long length) {
        this(s3Client, s3AsyncClient, bucket, key, path);
        this.cachedLength = length;
    }

    public S3StorageObject(S3Client s3Client, String bucket, String key, StoragePath path, long length, Instant lastModified) {
        this(s3Client, bucket, key, path, length);
        this.cachedLastModified = lastModified;
    }

    public S3StorageObject(
        S3Client s3Client,
        S3AsyncClient s3AsyncClient,
        String bucket,
        String key,
        StoragePath path,
        long length,
        Instant lastModified
    ) {
        this(s3Client, s3AsyncClient, bucket, key, path, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        long startNanos = System.nanoTime();
        long bytes = 0L;
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
            ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);
            GetObjectResponse metadata = response.response();

            if (cachedLength == null) {
                cachedLength = metadata.contentLength();
            }
            if (cachedLastModified == null) {
                cachedLastModified = metadata.lastModified();
            }
            bytes = metadata.contentLength() != null ? metadata.contentLength() : 0L;
            // Wrap so a transient fault DURING the read surfaces as a typed ExternalUnavailableException the
            // resume loop can act on; the SDK throws a raw (unchecked) S3Exception/SdkException mid-body.
            return new TransientTypingInputStream(response, path);
        } catch (Exception e) {
            throw throwReadFailure("Failed to read object from", e);
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, bytes);
        }
    }

    /**
     * Maps a failure from the S3 client into the exception to surface to ES|QL. A retryable transport
     * status (5xx/429) becomes an {@link ExternalUnavailableException} (503 — the read may succeed on
     * retry); a missing object or any other failure becomes an {@link IOException}, which the external
     * source operator classifies as a client-class 400. Returns the exception (never throws) so both
     * the synchronous and async read paths can route it.
     */
    private Exception mapReadFailure(String context, Throwable cause) {
        if (cause instanceof S3Exception s3 && ExternalUnavailableException.isRetryableStatus(s3.statusCode())) {
            boolean throttling = ExternalUnavailableException.isThrottlingStatus(s3.statusCode());
            return new ExternalUnavailableException(
                throttling,
                cause,
                "S3 store unavailable reading [{}] (HTTP {})",
                path,
                s3.statusCode()
            );
        }
        if (cause instanceof NoSuchKeyException) {
            return new IOException("Object not found: " + path, cause);
        }
        return new IOException(context + " " + path, cause);
    }

    /**
     * Synchronous-path bridge for {@link #mapReadFailure}: rethrows the mapped exception. The return
     * type lets callers write {@code throw throwReadFailure(...)} so the compiler sees an exit.
     */
    private RuntimeException throwReadFailure(String context, Throwable cause) throws IOException {
        Exception mapped = mapReadFailure(context, cause);
        if (mapped instanceof RuntimeException re) {
            throw re;
        }
        throw (IOException) mapped;
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

        // READ_TO_END -> open-ended "bytes=position-" (no up-front length() lookup); otherwise a closed range.
        String rangeHeader = toEnd ? Strings.format("bytes=%d-", position) : Strings.format("bytes=%d-%d", position, position + length - 1);

        long startNanos = System.nanoTime();
        long requestedBytes = toEnd ? 0L : length;
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range(rangeHeader).build();
            ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);
            GetObjectResponse metadata = response.response();

            if (cachedLength == null) {
                Long total = ContentRangeParser.parseTotalLength(metadata.contentRange());
                if (total != null) {
                    cachedLength = total;
                }
            }
            if (cachedLastModified == null) {
                cachedLastModified = metadata.lastModified();
            }
            if (toEnd) {
                requestedBytes = metadata.contentLength() != null ? metadata.contentLength() : 0L;
            }
            return new TransientTypingInputStream(response, path);
        } catch (Exception e) {
            if (toEnd && e instanceof S3Exception s3e && s3e.statusCode() == 416) {
                // Open-ended read at/after the end of an (empty or shorter) object: nothing to read. The SPI
                // contract for an open-ended read past the end is an empty stream.
                return InputStream.nullInputStream();
            }
            throw throwReadFailure("Range request failed for", e);
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, requestedBytes);
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
    public void abortStream(InputStream stream) throws IOException {
        if (stream instanceof Abortable abortable) {
            abortable.abort();
        } else {
            logger.trace(
                () -> Strings.format(
                    "abortStream received non-Abortable stream [%s] for [%s]; falling back to close() which may drain the body",
                    stream.getClass().getName(),
                    path
                )
            );
            stream.close();
        }
    }

    @Override
    public StoragePath path() {
        return path;
    }

    private void fetchMetadata() throws IOException {
        try {
            // Suffix range: bytes=-1 returns the last byte + Content-Range with total size.
            // Avoids a separate HEAD request for file size discovery.
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=-1").build();
            try (var response = s3Client.getObject(request)) {
                // Drain the 1-byte body so the HTTP connection returns to the pool
                // instead of being aborted on close.
                response.readAllBytes();
                GetObjectResponse metadata = response.response();
                cachedExists = true;
                cachedLastModified = metadata.lastModified();
                Long total = ContentRangeParser.parseTotalLength(metadata.contentRange());
                if (total != null) {
                    cachedLength = total;
                    return;
                }
            }
            // Content-Range missing (unexpected for S3) — fall back to HEAD for length
            fetchMetadataViaHead();
        } catch (NoSuchKeyException e) {
            setNotFound();
        } catch (S3Exception e) {
            if (e.statusCode() == 416) {
                // 416 Range Not Satisfiable: object exists but is empty (0 bytes)
                cachedExists = true;
                cachedLength = 0L;
            } else if (e.statusCode() == 403) {
                // GET denied — try the existing bytes=0-0 fallback which extracts
                // size from Content-Range; HEAD uses the same s3:GetObject permission
                // so would also be denied.
                fetchMetadataViaRangeGet();
            } else {
                fetchMetadataViaHead();
            }
        }
    }

    private void fetchMetadataViaHead() throws IOException {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
            HeadObjectResponse response = s3Client.headObject(request);

            cachedExists = true;
            cachedLength = response.contentLength();
            cachedLastModified = response.lastModified();
        } catch (NoSuchKeyException e) {
            setNotFound();
        } catch (Exception e) {
            if (e instanceof S3Exception s3e && s3e.statusCode() == 403) {
                fetchMetadataViaRangeGet();
            } else {
                throw new IOException("HeadObject request failed for " + path, e);
            }
        }
    }

    private void fetchMetadataViaRangeGet() throws IOException {
        try {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=0-0").build();
            try (var response = s3Client.getObject(request)) {
                GetObjectResponse metadata = response.response();
                cachedExists = true;
                cachedLastModified = metadata.lastModified();
                Long total = ContentRangeParser.parseTotalLength(metadata.contentRange());
                if (total == null) {
                    throw new IOException(
                        "Failed to determine object size for " + path + ": Content-Range header missing from range GET response"
                    );
                }
                cachedLength = total;
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof NoSuchKeyException) {
                setNotFound();
            } else {
                throw new IOException("Failed to get metadata for " + path + " (HEAD denied, range GET also failed)", e);
            }
        }
    }

    private void setNotFound() {
        cachedExists = false;
        cachedLength = 0L;
        cachedLastModified = null;
    }

    public String bucket() {
        return bucket;
    }

    public String key() {
        return key;
    }

    @Override
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        if (s3AsyncClient == null) {
            super.readBytesAsync(position, length, factory, executor, listener);
            return;
        }

        if (position < 0) {
            listener.onFailure(new IllegalArgumentException("position must be non-negative, got: " + position));
            return;
        }
        if (length <= 0) {
            listener.onFailure(new IllegalArgumentException("length must be positive, got: " + length));
            return;
        }
        if (length > Integer.MAX_VALUE) {
            // The async path materializes the response into a single direct ByteBuffer; ranges larger than 2 GiB
            // are not supportable here. Callers needing larger reads must split the range or fall
            // back to the streaming sync path via newStream(position, length).
            listener.onFailure(new IllegalArgumentException("length must fit in an int for async reads, got: " + length));
            return;
        }

        long endPosition = position + length - 1;
        String rangeHeader = Strings.format("bytes=%d-%d", position, endPosition);

        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).range(rangeHeader).build();

        // Use a custom transformer instead of AsyncResponseTransformer.toBytes() so each chunk is
        // copied straight into a pre-sized direct ByteBuffer (single chunk-to-destination copy),
        // rather than the SDK's default BAOS-based pipeline which materializes the body 3+ times.
        // See KnownLengthAsyncResponseTransformer for the full rationale.
        long startNanos = System.nanoTime();
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            (int) length,
            factory
        );
        onReadComplete(s3AsyncClient.getObject(request, transformer), (buffer, throwable) -> {
            if (throwable != null) {
                counters.addRequest(System.nanoTime() - startNanos, 0L);
                Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;
                listener.onFailure(mapReadFailure("Failed to read object from", cause));
                return;
            }

            GetObjectResponse response = transformer.response();
            if (response != null) {
                if (cachedLastModified == null) {
                    cachedLastModified = response.lastModified();
                }
                if (cachedLength == null) {
                    Long total = ContentRangeParser.parseTotalLength(response.contentRange());
                    if (total != null) {
                        cachedLength = total;
                    }
                }
            }

            deliverRead(listener, buffer, startNanos);
        });
    }

    @Override
    public boolean supportsNativeAsync() {
        return s3AsyncClient != null;
    }

    @Override
    public String toString() {
        return "S3StorageObject{bucket=" + bucket + ", key=" + key + ", path=" + path + "}";
    }
}

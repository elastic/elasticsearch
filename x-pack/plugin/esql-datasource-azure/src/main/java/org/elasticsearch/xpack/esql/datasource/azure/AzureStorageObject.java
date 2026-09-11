/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobDownloadHeaders;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobInputStream;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractMeteredStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalObjectChangedException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.utils.ContentRangeParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * StorageObject implementation for Azure Blob Storage.
 * Supports full and range reads, and metadata retrieval with caching.
 */
public final class AzureStorageObject extends AbstractMeteredStorageObject {
    private final BlobClient blobClient;
    private final BlobAsyncClient blobAsyncClient;
    private final String container;
    private final String blobName;
    private final StoragePath path;

    private volatile Long cachedLength;
    private volatile Instant cachedLastModified;
    private volatile Boolean cachedExists;
    /** First strong ETag returned by a download; sent as If-Match on later ones and reported as {@link #contentGeneration()}. */
    private final AtomicReference<String> pinnedEtag = new AtomicReference<>();

    public AzureStorageObject(BlobClient blobClient, String container, String blobName, StoragePath path) {
        this(blobClient, null, container, blobName, path);
    }

    public AzureStorageObject(BlobClient blobClient, BlobAsyncClient blobAsyncClient, String container, String blobName, StoragePath path) {
        if (blobClient == null) {
            throw new IllegalArgumentException("blobClient cannot be null");
        }
        if (container == null || container.isEmpty()) {
            throw new IllegalArgumentException("container cannot be null or empty");
        }
        if (blobName == null) {
            throw new IllegalArgumentException("blobName cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        this.blobClient = blobClient;
        this.blobAsyncClient = blobAsyncClient;
        this.container = container;
        this.blobName = blobName;
        this.path = path;
    }

    public AzureStorageObject(BlobClient blobClient, String container, String blobName, StoragePath path, long length) {
        this(blobClient, null, container, blobName, path, length);
    }

    public AzureStorageObject(
        BlobClient blobClient,
        BlobAsyncClient blobAsyncClient,
        String container,
        String blobName,
        StoragePath path,
        long length
    ) {
        this(blobClient, blobAsyncClient, container, blobName, path);
        this.cachedLength = length;
    }

    public AzureStorageObject(
        BlobClient blobClient,
        String container,
        String blobName,
        StoragePath path,
        long length,
        Instant lastModified
    ) {
        this(blobClient, null, container, blobName, path, length, lastModified);
    }

    public AzureStorageObject(
        BlobClient blobClient,
        BlobAsyncClient blobAsyncClient,
        String container,
        String blobName,
        StoragePath path,
        long length,
        Instant lastModified
    ) {
        this(blobClient, blobAsyncClient, container, blobName, path, length);
        this.cachedLastModified = lastModified;
    }

    @Override
    public InputStream newStream() throws IOException {
        long startNanos = System.nanoTime();
        long bytes = 0L;
        try {
            BlobInputStream blobStream = validateOpenedBlob(blobClient.openInputStream(null, requestConditions()));
            if (cachedLength != null) {
                bytes = cachedLength;
            }
            return new AzureTransientTypingInputStream(blobStream, path);
        } catch (Exception e) {
            throw throwReadFailure("Failed to read object from", e);
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, bytes);
        }
    }

    /**
     * Maps a failure from the Azure blob client into the exception to surface to ES|QL. A retryable
     * transport status (5xx/429) becomes an {@link ExternalUnavailableException} (503 — the read may
     * succeed on retry); any other failure becomes an {@link IOException}, which the external source
     * operator classifies as a client-class 400. Returns (never throws) so both the synchronous and
     * async read paths can route it.
     */
    private Exception mapReadFailure(String context, Throwable cause) {
        if (ExceptionsHelper.unwrap(cause, ExternalObjectChangedException.class) instanceof ExternalObjectChangedException changed) {
            return changed;
        }
        if (cause instanceof BlobStorageException bse && ExternalUnavailableException.isRetryableStatus(bse.getStatusCode())) {
            boolean throttling = ExternalUnavailableException.isThrottlingStatus(bse.getStatusCode());
            long retryAfterMs = 0L;
            if (throttling && bse.getResponse() != null) {
                retryAfterMs = ExternalUnavailableException.parseRetryAfterMs(bse.getResponse().getHeaderValue("Retry-After"));
            }
            return new ExternalUnavailableException(
                throttling,
                retryAfterMs,
                cause,
                "Azure store unavailable reading [{}] (HTTP {})",
                path,
                bse.getStatusCode()
            );
        }
        if (cause instanceof BlobStorageException precondition && precondition.getStatusCode() == 412) {
            return new ExternalObjectChangedException(cause, "Object changed during read of [{}]", path);
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

        long startNanos = System.nanoTime();
        try {
            // READ_TO_END: the offset-only BlobRange reads from position to the end of the blob — no length() lookup.
            BlobRange range = toEnd ? new BlobRange(position) : new BlobRange(position, length);
            BlobInputStream blobStream = validateOpenedBlob(blobClient.openInputStream(range, requestConditions()));
            return new AzureTransientTypingInputStream(blobStream, path);
        } catch (Exception e) {
            if (toEnd && e instanceof BlobStorageException bse && bse.getStatusCode() == 416) {
                // Open-ended read at/after the end of an (empty or shorter) object: nothing to read. The SPI
                // contract for an open-ended read past the end is an empty stream.
                return InputStream.nullInputStream();
            }
            throw throwReadFailure("Range request failed for", e);
        } finally {
            counters.addRequest(System.nanoTime() - startNanos, toEnd ? 0L : length);
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
    public StoragePath path() {
        return path;
    }

    @Override
    public long knownLength() {
        return cachedLength != null ? cachedLength : READ_TO_END;
    }

    @Override
    public String contentGeneration() {
        return pinnedEtag.get();
    }

    private BlobRequestConditions requestConditions() {
        BlobRequestConditions conditions = new BlobRequestConditions();
        String etag = pinnedEtag.get();
        if (etag != null) {
            conditions.setIfMatch(etag);
        }
        return conditions;
    }

    /** Weak ETags ({@code W/"..."}) are not byte-for-byte identifiers, so they are never used as a pin. */
    private void observeEtag(String etag) {
        String current = pinnedEtag.get();
        if (etag == null || etag.isBlank() || etag.regionMatches(true, 0, "W/", 0, 2)) {
            if (current != null) {
                throw new ExternalObjectChangedException("Object generation could not be verified during read of [{}]", path);
            }
            return;
        }
        if (current == null) {
            if (pinnedEtag.compareAndSet(null, etag)) {
                return;
            }
            current = pinnedEtag.get();
        }
        if (current.equals(etag) == false) {
            throw new ExternalObjectChangedException("Object changed during read of [{}]", path);
        }
    }

    private BlobInputStream validateOpenedBlob(BlobInputStream blobStream) {
        try {
            BlobProperties properties = blobStream.getProperties();
            observeEtag(properties == null ? null : properties.getETag());
            if (properties != null) {
                cachedLength = properties.getBlobSize();
            }
            return blobStream;
        } catch (RuntimeException e) {
            try {
                blobStream.close();
            } catch (Exception closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    private void observeDownloadResponse(BlobDownloadAsyncResponse response) {
        if (response == null) {
            return;
        }
        observeDownloadHeaders(response.getDeserializedHeaders());
    }

    private void observeDownloadHeaders(BlobDownloadHeaders headers) {
        observeEtag(headers == null ? null : headers.getETag());
        if (headers == null) {
            return;
        }
        Long total = ContentRangeParser.parseTotalLength(headers.getContentRange());
        if (total != null) {
            cachedLength = total;
        }
    }

    private void fetchMetadata() throws IOException {
        try {
            var properties = blobClient.getProperties();
            cachedExists = true;
            // getProperties() transfers no blob bytes: it reports whatever version is current, which is
            // not necessarily the one reads are pinned to. It must neither establish the pin nor
            // overwrite the pinned version's size (already set by the download that pinned it).
            String etag = pinnedEtag.get();
            if (etag == null || etag.equals(properties.getETag())) {
                cachedLength = properties.getBlobSize();
            }
            cachedLastModified = properties.getLastModified() != null ? properties.getLastModified().toInstant() : null;
        } catch (Exception e) {
            if (e instanceof BlobStorageException bse && bse.getStatusCode() == 404) {
                setNotFound();
            } else if (e instanceof BlobStorageException bse && bse.getStatusCode() == 403) {
                fetchMetadataViaRangeGet();
            } else {
                throw new IOException("Failed to get metadata for " + path, e);
            }
        }
    }

    private void fetchMetadataViaRangeGet() throws IOException {
        try {
            var output = new ByteArrayOutputStream();
            // Unlike getProperties(), this transfers blob bytes under the same If-Match as the reads, so
            // it may establish the pin and its Content-Range total is the pinned version's size.
            var response = blobClient.downloadStreamWithResponse(
                output,
                new BlobRange(0, 1L),
                null,
                requestConditions(),
                false,
                null,
                null
            );
            var headers = response.getDeserializedHeaders();
            cachedExists = true;
            observeEtag(headers.getETag());
            Long total = ContentRangeParser.parseTotalLength(headers.getContentRange());
            if (total == null) {
                throw new IOException(
                    "Failed to determine object size for " + path + ": Content-Range header missing from range GET response"
                );
            }
            cachedLength = total;
            cachedLastModified = headers.getLastModified() != null ? headers.getLastModified().toInstant() : null;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof BlobStorageException bse && bse.getStatusCode() == 404) {
                setNotFound();
            } else if (e instanceof BlobStorageException bse && bse.getStatusCode() == 412) {
                // This download carried the read pin, so a 412 here is the same mid-query rewrite the read
                // path reports; keep the typing rather than flattening it to a client-class 400.
                throw throwReadFailure("Failed to get metadata for", e);
            } else {
                throw new IOException("Failed to get metadata for " + path + " (properties denied, range GET also failed)", e);
            }
        }
    }

    private void setNotFound() {
        cachedExists = false;
        cachedLength = 0L;
        cachedLastModified = null;
    }

    @Override
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        if (blobAsyncClient == null) {
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
            listener.onFailure(new IllegalArgumentException("length must fit in an int for async reads, got: " + length));
            return;
        }

        int len = Math.toIntExact(length);
        final DirectReadBuffer drb;
        try {
            drb = factory.allocateWritableWindow(len);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        BlobRange range = new BlobRange(position, length);
        long startNanos = System.nanoTime();
        final CompletableFuture<Void> future;
        try {
            future = blobAsyncClient.downloadWithResponse(range, null, requestConditions(), false)
                .doOnNext(this::observeDownloadResponse)
                .flatMapMany(response -> response.getValue())
                .reduce(drb.buffer(), (acc, chunk) -> {
                    if (chunk.remaining() > acc.remaining()) {
                        throw new IllegalStateException("Server returned more bytes than requested (" + length + ")");
                    }
                    acc.put(chunk);
                    return acc;
                })
                .doOnNext(ByteBuffer::flip)
                // Do not complete the SDK-retained future with an alias of drb's payload.
                .then()
                .toFuture();
        } catch (RuntimeException e) {
            // Assembly-time throw from Reactor operator construction. No request was issued,
            // so counters are not updated.
            drb.close();
            listener.onFailure(mapReadFailure("Failed to read bytes from", e));
            return;
        }
        onReadComplete(future, (ignored, error) -> {
            if (error != null) {
                counters.addRequest(System.nanoTime() - startNanos, 0L);
                // Release eagerly on the failure path so the breaker charge does not outlive
                // the failed request.
                drb.close();
                Throwable cause = error.getCause() != null ? error.getCause() : error;
                listener.onFailure(mapReadFailure("Failed to read bytes from", cause));
            } else {
                deliverRead(listener, drb, startNanos);
            }
        });
    }

    @Override
    public boolean supportsNativeAsync() {
        return blobAsyncClient != null;
    }

    @Override
    public boolean readBytesAsyncReleasesExecutor() {
        return blobAsyncClient != null;
    }

    // TODO: wire retry counts via an Azure SDK HttpPipelinePolicy interceptor; SDK-internal
    // retries are not yet visible to counters.addRetry().

    @Override
    public String toString() {
        return "AzureStorageObject{container=" + container + ", blobName=" + blobName + ", path=" + path + "}";
    }
}

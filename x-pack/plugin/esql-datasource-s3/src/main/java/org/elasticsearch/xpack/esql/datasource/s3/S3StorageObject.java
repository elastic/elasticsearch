/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import io.netty.channel.ChannelException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractMeteredStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalObjectChangedException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.utils.ContentRangeParser;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLException;

/**
 * StorageObject implementation for S3 using AWS SDK v2.
 * Supports full and range reads, metadata retrieval, and optional native async via S3AsyncClient.
 */
public final class S3StorageObject extends AbstractMeteredStorageObject {
    private static final Logger logger = LogManager.getLogger(S3StorageObject.class);

    // Real SDK chains here are 2-4 deep; this only stops a pathological one.
    private static final int MAX_CAUSE_DEPTH = 12;

    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final String bucket;
    private final String key;
    private final StoragePath path;

    private volatile Long cachedLength;
    private volatile Instant cachedLastModified;
    private volatile Boolean cachedExists;
    /** First strong ETag returned by a GET; sent as If-Match on later GETs and reported as {@link #contentGeneration()}. */
    private final AtomicReference<String> pinnedEtag = new AtomicReference<>();
    /** Some S3-compatible stores do not implement If-Match on GET; validate each response ETag instead. */
    private volatile boolean ifMatchUnsupported;

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
            GetObjectRequest.Builder request = GetObjectRequest.builder().bucket(bucket).key(key);
            ResponseInputStream<GetObjectResponse> response = getObject(request);
            GetObjectResponse metadata = response.response();
            observeResponse(metadata, 0L, false);
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
     * Maps a failure from the S3 client into the exception to surface to ES|QL. An already-typed
     * {@link ExternalUnavailableException} found anywhere in the cause chain is returned unchanged so its retry
     * and status signal is preserved. A retryable transport status (5xx/429) becomes an
     * {@link ExternalUnavailableException} (503 — the read may
     * succeed on retry). A closed HTTP client ({@code Connection pool shut down} / client-closed
     * {@link IllegalStateException}) is the same 503: the client is gone, not the object. An
     * {@link SdkClientException} whose <em>direct</em> cause is an {@link IOException},
     * {@link TimeoutException}, or Netty {@link ChannelException} is the same 503: the SDK never
     * got a response (Apache drop / Netty read timeout). Nested {@code SdkClientException}
     * (IMDS/STS), {@link UnknownHostException}, and {@link SSLException} stay client-class. Other
     * {@link IllegalStateException}s are returned as-is (HTTP 500 via classify) so a programming
     * error is not retried and is not disguised as a client 400. A circuit-breaker rejection is
     * returned as a {@link CircuitBreakingException} naming the path, wherever it sits in the cause
     * chain: the destination buffer for a native-async read is allocated inside the SDK's response
     * pipeline, so the SDK's retry stage wraps the trip in a status-neutral {@code SdkClientException} —
     * unwrapping it preserves the breaker's 429 so load shedding is not reported as a permanent
     * query error. A missing object, a credential failure, or any other failure becomes an
     * {@link IOException}, which the external source operator classifies as a client-class 400.
     * Returns the exception (never throws) so both the synchronous and async read paths can route it.
     */
    private Exception mapReadFailure(String context, Throwable cause) {
        if (cause instanceof ExternalObjectChangedException changed) {
            return changed;
        }
        CircuitBreakingException breakerTrip = unwrapBreakerTrip(cause, context, path);
        if (breakerTrip != null) {
            return breakerTrip;
        }
        ExternalUnavailableException unavailable = findUnavailable(cause);
        if (unavailable != null) {
            return unavailable;
        }
        if (cause instanceof S3Exception s3 && ExternalUnavailableException.isRetryableStatus(s3.statusCode())) {
            boolean throttling = ExternalUnavailableException.isThrottlingStatus(s3.statusCode());
            long retryAfterMs = 0L;
            if (throttling && s3.awsErrorDetails() != null && s3.awsErrorDetails().sdkHttpResponse() != null) {
                retryAfterMs = ExternalUnavailableException.parseRetryAfterMs(
                    s3.awsErrorDetails().sdkHttpResponse().firstMatchingHeader("Retry-After").orElse(null)
                );
            }
            return new ExternalUnavailableException(
                throttling,
                retryAfterMs,
                cause,
                "S3 store unavailable reading [{}] (HTTP {})",
                path,
                s3.statusCode()
            );
        }
        if (cause instanceof S3Exception precondition && precondition.statusCode() == 412) {
            return new ExternalObjectChangedException(cause, "Object changed during read of [{}]", path);
        }
        if (cause instanceof S3Exception denied && denied.statusCode() == 403) {
            // Follows the listing-403 wording in S3StorageProvider: name what was refused, then what to change.
            // The read path cannot say which credential is wrong -- S3 answers a bad key and an anonymous request
            // against an authenticated bucket with the same 403 -- so it names both remedies.
            return new IOException(
                "Access denied reading ["
                    + S3FailureDetail.redactPath(path)
                    + "] ("
                    + S3FailureDetail.of(denied)
                    + "). Verify the access_key and secret_key configured on the data source, "
                    + "or set auth=anonymous if the bucket is public.",
                cause
            );
        }
        if (cause instanceof NoSuchKeyException) {
            return new IOException("Object not found: " + S3FailureDetail.redactPath(path), cause);
        }
        if (isClosedClient(cause)) {
            return new ExternalUnavailableException(
                false,
                cause,
                "S3 client unavailable reading [{}]: {}",
                S3FailureDetail.redactPath(path),
                S3FailureDetail.of(cause)
            );
        }
        if (isSdkClientTransportFailure(cause)) {
            return new ExternalUnavailableException(false, cause, "S3 store unavailable reading [{}]: {}", S3FailureDetail.redactPath(path), S3FailureDetail.of(cause));
        }
        if (cause instanceof IllegalStateException ise) {
            return ise;
        }
        return new IOException(context + " " + S3FailureDetail.redactPath(path) + ": " + S3FailureDetail.of(cause), cause);
    }

    /**
     * The first {@link ExternalUnavailableException} in {@code cause}'s chain, or {@code null} if there is none.
     * The whole chain is walked rather than only the top type inspected because a failure our own code typed — the
     * body length mismatches raised by {@link KnownLengthAsyncResponseTransformer} — can come back from the SDK
     * wrapped in one or more of its own exceptions. A top-only check would miss those and let them fall through to
     * the client-class 400 arm, which is the give-up-without-retrying this mapping exists to prevent.
     */
    private static ExternalUnavailableException findUnavailable(Throwable cause) {
        Throwable current = cause;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH && current != null; depth++) {
            if (current instanceof ExternalUnavailableException eue) {
                return eue;
            }
            Throwable next = current.getCause();
            if (next == null || next == current) {
                break;
            }
            current = next;
        }
        return null;
    }

    private static boolean isClosedClient(Throwable cause) {
        Throwable current = cause;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH && current != null; depth++) {
            if (current instanceof IllegalStateException) {
                String message = current.getMessage();
                if (message != null) {
                    String lower = message.toLowerCase(Locale.ROOT);
                    if (lower.contains("pool shut down") || lower.contains("client is closed")) {
                        return true;
                    }
                }
            }
            Throwable next = current.getCause();
            if (next == null || next == current) {
                break;
            }
            current = next;
        }
        return false;
    }

    /**
     * Direct cause of the first {@link SdkClientException} is an {@link IOException},
     * {@link TimeoutException}, or Netty {@link ChannelException} (Apache no-response, Netty
     * {@code ReadTimeoutException}). Nested {@code SdkClientException} is the IMDS/STS credential
     * chain, not a dropped GET. {@link UnknownHostException} and {@link SSLException} stay
     * client-class.
     */
    static boolean isSdkClientTransportFailure(Throwable cause) {
        Throwable sdk = ExceptionsHelper.unwrap(cause, SdkClientException.class);
        if (sdk == null) {
            return false;
        }
        Throwable below = sdk.getCause();
        if (below == null) {
            return false;
        }
        if (ExceptionsHelper.unwrap(below, UnknownHostException.class) != null
            || ExceptionsHelper.unwrap(below, SSLException.class) != null) {
            return false;
        }
        return below instanceof IOException || below instanceof TimeoutException || below instanceof ChannelException;
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
    public long knownLength() {
        return cachedLength != null ? cachedLength : READ_TO_END;
    }

    @Override
    public String contentGeneration() {
        return pinnedEtag.get();
    }

    /**
     * Issues GET, sending If-Match of the first strong ETag on later opens. A store that answers
     * {@code NotImplemented} does not support If-Match on GET: one unconditioned retry is allowed,
     * but its response and every later response must carry the pinned ETag.
     */
    private ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest.Builder builder) {
        boolean sentIfMatch = applyIfMatch(builder);
        GetObjectRequest request = builder.build();
        try {
            return validateGeneration(s3Client.getObject(request));
        } catch (S3Exception e) {
            if (sentIfMatch && isIfMatchUnsupported(e)) {
                ifMatchUnsupported = true;
                logger.debug("S3 If-Match not implemented for [{}]; validating response ETags instead", path);
                return validateGeneration(s3Client.getObject(unpinned(request)));
            }
            throw e;
        }
    }

    private boolean applyIfMatch(GetObjectRequest.Builder builder) {
        String etag = pinnedEtag.get();
        if (ifMatchUnsupported || etag == null) {
            return false;
        }
        builder.ifMatch(etag);
        return true;
    }

    private static GetObjectRequest unpinned(GetObjectRequest pinned) {
        GetObjectRequest.Builder builder = GetObjectRequest.builder().bucket(pinned.bucket()).key(pinned.key());
        if (pinned.range() != null) {
            builder.range(pinned.range());
        }
        return builder.build();
    }

    /**
     * True only for the store-does-not-implement-If-Match answer. Deliberately not "any 400": a
     * malformed range, a bad request signature, or an invalid argument are also 400s, and unpinning
     * on those would silently drop the generation pin for the rest of the query and retry the same
     * request unpinned. {@code NotImplemented} is the S3 API's own "this server lacks the feature"
     * code; AWS itself answers a genuine If-Match mismatch with 412, handled in {@link #mapReadFailure}.
     */
    private static boolean isIfMatchUnsupported(Throwable cause) {
        if (cause instanceof S3Exception s3 && s3.awsErrorDetails() != null) {
            return "NotImplemented".equals(s3.awsErrorDetails().errorCode());
        }
        return false;
    }

    private void observeResponse(GetObjectResponse metadata, long position, boolean closedRange) {
        Long total = ContentRangeParser.parseTotalLength(metadata.contentRange());
        if (total != null) {
            cachedLength = total;
        } else if (closedRange == false && position == 0 && metadata.contentLength() != null) {
            cachedLength = metadata.contentLength();
        }
        if (cachedLastModified == null && metadata.lastModified() != null) {
            cachedLastModified = metadata.lastModified();
        }
    }

    private void observeEtag(String etag) {
        String current = pinnedEtag.get();
        if (etag == null || etag.isBlank() || isStrongEtag(etag) == false) {
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

    /**
     * Validates the response generation before exposing its body. This also closes the race between
     * concurrent first reads: exactly one ETag wins the pin and a response from another generation is aborted.
     */
    private ResponseInputStream<GetObjectResponse> validateGeneration(ResponseInputStream<GetObjectResponse> response) {
        try {
            observeEtag(response.response().eTag());
            return response;
        } catch (RuntimeException e) {
            response.abort();
            throw e;
        }
    }

    /** Weak ETags ({@code W/"..."}) are not byte-for-byte identifiers, so they are never used as a pin. */
    private static boolean isStrongEtag(String etag) {
        return etag.regionMatches(true, 0, "W/", 0, 2) == false;
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
            GetObjectRequest.Builder request = GetObjectRequest.builder().bucket(bucket).key(key).range(rangeHeader);
            ResponseInputStream<GetObjectResponse> response = getObject(request);
            GetObjectResponse metadata = response.response();
            observeResponse(metadata, position, toEnd == false);
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
            GetObjectRequest.Builder request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=-1");
            try (var response = getObject(request)) {
                // Drain the 1-byte body so the HTTP connection returns to the pool
                // instead of being aborted on close.
                response.readAllBytes();
                GetObjectResponse metadata = response.response();
                cachedExists = true;
                observeResponse(metadata, 0L, true);
                if (cachedLength != null) {
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
        } catch (Exception e) {
            throw throwReadFailure("Failed to read object metadata for", e);
        }
    }

    private void fetchMetadataViaHead() throws IOException {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
            HeadObjectResponse response = s3Client.headObject(request);

            cachedExists = true;
            // HEAD is not a GET: it reports whatever generation is current, which is not necessarily the
            // one reads are pinned to. It must neither establish the pin nor overwrite the pinned
            // generation's size (already set by the GET that pinned it).
            String etag = pinnedEtag.get();
            if (etag == null || etag.equals(response.eTag())) {
                cachedLength = response.contentLength();
            }
            cachedLastModified = response.lastModified();
        } catch (NoSuchKeyException e) {
            setNotFound();
        } catch (Exception e) {
            if (e instanceof S3Exception s3e && s3e.statusCode() == 403) {
                fetchMetadataViaRangeGet();
            } else {
                throw throwReadFailure("HeadObject request failed for", e);
            }
        }
    }

    private void fetchMetadataViaRangeGet() throws IOException {
        try {
            GetObjectRequest.Builder request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=0-0");
            try (var response = getObject(request)) {
                GetObjectResponse metadata = response.response();
                cachedExists = true;
                observeResponse(metadata, 0L, true);
                if (cachedLength == null) {
                    throw new IOException(
                        "Failed to determine object size for " + path + ": Content-Range header missing from range GET response"
                    );
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof NoSuchKeyException) {
                setNotFound();
            } else {
                throw throwReadFailure("Failed to get metadata for", e);
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
        startReadBytesAsync(position, length, factory, executor, listener);
    }

    @Override
    public Releasable startReadBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        if (s3AsyncClient == null) {
            // Must call super.readBytesAsync (the StorageObject default via AbstractMeteredStorageObject),
            // not super.startReadBytesAsync: this class's readBytesAsync delegates here, so the default
            // start would recurse until the stack overflows. StorageObject.super is illegal here because
            // this class does not implement StorageObject directly.
            super.readBytesAsync(position, length, factory, executor, listener);
            return () -> {};
        }

        if (position < 0) {
            listener.onFailure(new IllegalArgumentException("position must be non-negative, got: " + position));
            return () -> {};
        }
        if (length <= 0) {
            listener.onFailure(new IllegalArgumentException("length must be positive, got: " + length));
            return () -> {};
        }
        if (length > Integer.MAX_VALUE) {
            // The async path materializes the response into a single ByteBuffer; ranges larger than 2 GiB
            // are not supportable here. Callers needing larger reads must split the range or fall
            // back to the streaming sync path via newStream(position, length).
            listener.onFailure(new IllegalArgumentException("length must fit in an int for async reads, got: " + length));
            return () -> {};
        }

        long endPosition = position + length - 1;
        String rangeHeader = Strings.format("bytes=%d-%d", position, endPosition);

        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder().bucket(bucket).key(key).range(rangeHeader);
        boolean sentIfMatch = applyIfMatch(requestBuilder);
        GetObjectRequest request = requestBuilder.build();

        // Use a custom transformer instead of AsyncResponseTransformer.toBytes() so each chunk is
        // copied straight into a pre-sized destination ByteBuffer (single chunk-to-destination copy),
        // rather than the SDK's default BAOS-based pipeline which materializes the body 3+ times.
        // See KnownLengthAsyncResponseTransformer for the full rationale.
        long startNanos = System.nanoTime();
        AtomicReference<CompletableFuture<?>> live = new AtomicReference<>();
        issueAsyncGet(request, position, length, factory, listener, startNanos, sentIfMatch, live);
        return () -> {
            var current = live.get();
            if (current != null) {
                FutureUtils.cancel(current);
            }
        };
    }

    /**
     * Issues the async GET. A store that answers {@code NotImplemented} does not support If-Match on
     * GET: one unconditioned retry, with response-ETag validation, matching the sync {@link #getObject} path.
     */
    private void issueAsyncGet(
        GetObjectRequest request,
        long position,
        long length,
        DirectBufferFactory factory,
        ActionListener<DirectReadBuffer> listener,
        long startNanos,
        boolean allowIfMatchFallback,
        AtomicReference<CompletableFuture<?>> live
    ) {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            (int) length,
            factory,
            path
        );
        var sdkFuture = s3AsyncClient.getObject(request, transformer);
        live.set(sdkFuture);
        onReadComplete(sdkFuture, (buffer, throwable) -> {
            if (throwable != null) {
                Throwable cause = unwrapCompletionWrappers(throwable);
                if (allowIfMatchFallback && isIfMatchUnsupported(cause)) {
                    ifMatchUnsupported = true;
                    logger.debug("S3 If-Match not implemented for [{}]; validating response ETags instead", path);
                    issueAsyncGet(unpinned(request), position, length, factory, listener, startNanos, false, live);
                    return;
                }
                counters.addRequest(System.nanoTime() - startNanos, 0L);
                listener.onFailure(mapReadFailure("Failed to read object from", cause));
                return;
            }

            GetObjectResponse response = transformer.response();
            if (response != null) {
                try {
                    observeEtag(response.eTag());
                    observeResponse(response, position, true);
                } catch (ExternalObjectChangedException e) {
                    counters.addRequest(System.nanoTime() - startNanos, 0L);
                    buffer.close();
                    listener.onFailure(e);
                    return;
                }
            }

            deliverRead(listener, buffer, startNanos);
        });
    }

    /**
     * Peels the {@code CompletionException} / {@code ExecutionException} wrappers a {@code CompletableFuture} adds
     * around a failure, and only those. Peeling one level unconditionally instead would step past a failure that
     * carries a cause of its own — an {@link ExternalUnavailableException} wrapping a transport error, say — and hand
     * {@link #mapReadFailure} the inner exception, losing the type it was about to key on.
     */
    private static Throwable unwrapCompletionWrappers(Throwable throwable) {
        Throwable current = throwable;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH; depth++) {
            if (current instanceof CompletionException == false && current instanceof ExecutionException == false) {
                break;
            }
            Throwable next = current.getCause();
            if (next == null || next == current) {
                break;
            }
            current = next;
        }
        return current;
    }

    @Override
    public boolean supportsNativeAsync() {
        return s3AsyncClient != null;
    }

    @Override
    public boolean readBytesAsyncReleasesExecutor() {
        return s3AsyncClient != null;
    }

    @Override
    public String toString() {
        return "S3StorageObject{bucket=" + bucket + ", key=" + key + ", path=" + path + "}";
    }
}

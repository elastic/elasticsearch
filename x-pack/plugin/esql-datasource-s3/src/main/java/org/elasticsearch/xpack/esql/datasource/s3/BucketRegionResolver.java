/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Per-bucket region cache and per-region S3 client pairs for one {@link S3StorageProvider}.
 * <p>
 * A bucket's region is resolved once (single-flight HeadBucket) and then a region-pinned
 * sync/async pair is reused. The seed us-east-1 (or explicit-region) clients are both the
 * probe client and the pair for that seed region; extra pairs are built only for other
 * regions. Connection pools are per pair.
 * <p>
 * HeadBucket is blocking. Callers must not invoke {@link #clientsFor} on a Netty event-loop
 * thread.
 */
final class BucketRegionResolver {

    record ClientPair(S3Client sync, S3AsyncClient async) {}

    @FunctionalInterface
    interface PairFactory {
        ClientPair create(String region);
    }

    private static final Pattern REGION_ELEMENT = Pattern.compile("<Region>([^<]+)</Region>");
    private static final String BUCKET_REGION_HEADER = "x-amz-bucket-region";

    private final String seedRegion;
    @Nullable
    private final String explicitRegion;
    private final S3Client probeSync;
    private final S3AsyncClient probeAsync;
    private final long waitTimeoutNanos;
    private final PairFactory pairFactory;
    private final ConcurrentHashMap<String, CompletableFuture<String>> regions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ClientPair> pairs = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private boolean closed;

    BucketRegionResolver(
        String seedRegion,
        @Nullable String explicitRegion,
        S3Client probeSync,
        S3AsyncClient probeAsync,
        long waitTimeoutNanos,
        PairFactory pairFactory
    ) {
        this.seedRegion = seedRegion;
        this.explicitRegion = explicitRegion;
        this.probeSync = probeSync;
        this.probeAsync = probeAsync;
        this.waitTimeoutNanos = waitTimeoutNanos;
        this.pairFactory = pairFactory;
    }

    ClientPair clientsFor(String bucket) throws IOException {
        String region = regionFor(bucket);
        try {
            return pairFor(region);
        } catch (RuntimeException e) {
            rethrowProbeFailure(bucket, e);
            throw new AssertionError("unreachable");
        }
    }

    boolean hasResolvedRegion(String bucket) {
        if (explicitRegion != null) {
            return true;
        }
        CompletableFuture<String> cached = regions.get(bucket);
        return cached != null && cached.isDone() && cached.isCompletedExceptionally() == false;
    }

    /**
     * Drops a cached region when a later request observes a different one, so a stale
     * invalidation cannot evict a newer answer.
     */
    void invalidate(String bucket, @Nullable String observedRegion) {
        CompletableFuture<String> cached = regions.get(bucket);
        if (cached == null || cached.isDone() == false || cached.isCompletedExceptionally()) {
            return;
        }
        String current;
        try {
            current = cached.getNow(null);
        } catch (Exception e) {
            return;
        }
        if (current == null) {
            return;
        }
        if (observedRegion != null && observedRegion.equals(current)) {
            return;
        }
        regions.remove(bucket, cached);
    }

    void rememberRegion(String bucket, String region) {
        regions.putIfAbsent(bucket, CompletableFuture.completedFuture(region));
    }

    void close() {
        List<ClientPair> extra;
        synchronized (lock) {
            closed = true;
            extra = new ArrayList<>(pairs.values());
            pairs.clear();
        }
        for (CompletableFuture<String> pending : regions.values()) {
            pending.completeExceptionally(new IllegalStateException("S3 client is closed"));
        }
        for (ClientPair pair : extra) {
            closeQuietly(pair.sync());
            closeQuietly(pair.async());
        }
    }

    private String regionFor(String bucket) throws IOException {
        if (explicitRegion != null) {
            return explicitRegion;
        }
        CompletableFuture<String> created = new CompletableFuture<>();
        CompletableFuture<String> existing = regions.putIfAbsent(bucket, created);
        if (existing == null) {
            try {
                String region = probe(bucket);
                created.complete(region);
                return region;
            } catch (Exception e) {
                regions.remove(bucket, created);
                created.completeExceptionally(e);
                rethrowProbeFailure(bucket, e);
                throw new AssertionError("unreachable");
            }
        }
        try {
            return existing.get(waitTimeoutNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            StoragePath path = StoragePath.of("s3://" + bucket);
            throw new ExternalUnavailableException(
                Condition.STORE_UNAVAILABLE,
                path,
                "timed out waiting for bucket region",
                "",
                false,
                0L,
                e
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            rethrowProbeFailure(bucket, e);
            throw new AssertionError("unreachable");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            rethrowProbeFailure(bucket, cause);
            throw new AssertionError("unreachable");
        }
    }

    private String probe(String bucket) throws IOException {
        try {
            HeadBucketResponse response = probeSync.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            String region = response.bucketRegion();
            if (region == null || region.isEmpty()) {
                region = response.sdkHttpResponse().firstMatchingHeader(BUCKET_REGION_HEADER).orElse(null);
            }
            return region == null || region.isEmpty() ? seedRegion : region;
        } catch (NoSuchBucketException e) {
            throw e;
        } catch (S3Exception e) {
            if (isNoSuchBucket(e)) {
                throw e;
            }
            if (isRetryableProbeFailure(e)) {
                throw e;
            }
            String region = observedRegion(e);
            return region == null || region.isEmpty() ? seedRegion : region;
        } catch (SdkClientException e) {
            throw e;
        }
    }

    private ClientPair pairFor(String region) {
        if (seedRegion.equals(region)) {
            ensureOpen();
            return new ClientPair(probeSync, probeAsync);
        }
        ClientPair cached = pairs.get(region);
        if (cached != null) {
            return cached;
        }
        synchronized (lock) {
            ensureOpenLocked();
            cached = pairs.get(region);
            if (cached != null) {
                return cached;
            }
            ClientPair created = pairFactory.create(region);
            if (closed) {
                closeQuietly(created.sync());
                closeQuietly(created.async());
                throw closedException();
            }
            pairs.put(region, created);
            return created;
        }
    }

    private void ensureOpen() {
        synchronized (lock) {
            ensureOpenLocked();
        }
    }

    private void ensureOpenLocked() {
        if (closed) {
            throw closedException();
        }
    }

    private static IllegalStateException closedException() {
        return new IllegalStateException("S3 client is closed");
    }

    private static void rethrowProbeFailure(String bucket, Throwable cause) throws IOException {
        if (cause instanceof IOException ioe) {
            throw ioe;
        }
        if (cause instanceof ExternalException runtime) {
            throw runtime;
        }
        StoragePath path = StoragePath.of("s3://" + bucket);
        if (cause instanceof Exception exception) {
            ExternalUnavailableException unavailable = S3StorageProvider.mapResolveFailure(path, exception);
            if (unavailable != null) {
                throw unavailable;
            }
        }
        S3Exception s3 = s3Exception(cause);
        if (s3 != null && isNoSuchBucket(s3)) {
            // A missing bucket is a config error, not a gone object: OBJECT_NOT_FOUND would start
            // stale-listing recovery that cannot create the bucket.
            throw new ExternalClientException(
                Condition.METADATA_UNAVAILABLE,
                path,
                "NoSuchBucket",
                "Check that the bucket exists",
                cause instanceof Exception e ? e : null
            );
        }
        if (cause instanceof IllegalStateException closed && closed.getMessage() != null && closed.getMessage().contains("closed")) {
            throw new ExternalUnavailableException(Condition.STORE_UNAVAILABLE, path, "S3 client is closed", "", false, 0L, closed);
        }
        throw new IOException("Failed to resolve region for bucket [" + bucket + "]", cause);
    }

    private static boolean isRetryableProbeFailure(S3Exception e) {
        return ExternalUnavailableException.isRetryableStatus(e.statusCode());
    }

    static boolean isNoSuchBucket(S3Exception e) {
        if (e instanceof NoSuchBucketException) {
            return true;
        }
        if (e.statusCode() != 404) {
            return false;
        }
        return e.awsErrorDetails() != null && "NoSuchBucket".equals(e.awsErrorDetails().errorCode());
    }

    static boolean isWrongRegion(Throwable cause) {
        S3Exception s3 = s3Exception(cause);
        if (s3 == null) {
            return false;
        }
        if (s3.statusCode() == 301) {
            return true;
        }
        return S3StorageProvider.isAuthorizationHeaderMalformed(s3);
    }

    @Nullable
    static String observedRegion(Throwable cause) {
        S3Exception s3 = s3Exception(cause);
        if (s3 == null) {
            return null;
        }
        if (s3.awsErrorDetails() != null && s3.awsErrorDetails().sdkHttpResponse() != null) {
            String header = s3.awsErrorDetails().sdkHttpResponse().firstMatchingHeader(BUCKET_REGION_HEADER).orElse(null);
            if (header != null && header.isEmpty() == false) {
                return header;
            }
        }
        String fromXml = regionFromXml(xmlPayload(s3));
        if (fromXml != null) {
            return fromXml;
        }
        String xmlSource = s3.awsErrorDetails() != null ? s3.awsErrorDetails().errorMessage() : s3.getMessage();
        if (xmlSource == null) {
            xmlSource = s3.toString();
        }
        return regionFromXml(xmlSource);
    }

    @Nullable
    private static String xmlPayload(S3Exception s3) {
        if (s3.awsErrorDetails() == null) {
            return null;
        }
        SdkBytes raw = s3.awsErrorDetails().rawResponse();
        return raw == null ? null : raw.asUtf8String();
    }

    @Nullable
    private static String regionFromXml(String xmlSource) {
        if (xmlSource == null || xmlSource.isEmpty()) {
            return null;
        }
        Matcher matcher = REGION_ELEMENT.matcher(xmlSource);
        return matcher.find() ? matcher.group(1) : null;
    }

    @Nullable
    static S3Exception s3Exception(Throwable cause) {
        Throwable current = cause;
        for (int depth = 0; depth < 12 && current != null; depth++) {
            if (current instanceof S3Exception s3) {
                return s3;
            }
            Throwable next = current.getCause();
            if (next == null || next == current) {
                break;
            }
            current = next;
        }
        return null;
    }

    static String wrongRegionRemedy(String bucket, @Nullable String region) {
        String named = region == null || region.isEmpty() ? "unknown" : region;
        return "bucket [" + bucket + "] is in [" + named + "]; set region on the dataset";
    }

    private static void closeQuietly(SdkAutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (RuntimeException ignored) {
            // Best-effort: a failed close must not hide the rest.
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;

public class BlobStoreCacheService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(BlobStoreCacheService.class);

    public static final int DEFAULT_CACHED_BLOB_SIZE = ByteSizeUnit.KB.toIntBytes(1);
    private static final Cache<String, String> LOG_EXCEEDING_FILES_CACHE = CacheBuilder.<String, String>builder()
        .setExpireAfterAccess(TimeValue.timeValueMinutes(60L))
        .build();

    static final int MAX_IN_FLIGHT_CACHE_FILLS = Integer.MAX_VALUE;

    private final Semaphore inFlightCacheFills;
    private final AtomicBoolean closed;
    private final Client client;
    private final String index;

    public BlobStoreCacheService(Client client, String index) {
        this.client = new OriginSettingClient(client, SEARCHABLE_SNAPSHOTS_ORIGIN);
        this.inFlightCacheFills = new Semaphore(MAX_IN_FLIGHT_CACHE_FILLS);
        this.closed = new AtomicBoolean(false);
        this.index = index;
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        if (closed.compareAndSet(false, true)) {
            logger.debug("blob cache service is stopped");
        }
    }

    // public for tests
    public boolean waitForInFlightCacheFillsToComplete(long timeout, TimeUnit unit) {
        boolean acquired = false;
        try {
            logger.debug("waiting for in-flight blob cache fills to complete");
            acquired = inFlightCacheFills.tryAcquire(MAX_IN_FLIGHT_CACHE_FILLS, timeout, unit);
        } catch (InterruptedException e) {
            logger.warn("interrupted while waiting for in-flight blob cache fills to complete", e);
            Thread.currentThread().interrupt();
        } finally {
            if (acquired) {
                inFlightCacheFills.release(MAX_IN_FLIGHT_CACHE_FILLS);
            }
        }
        return acquired;
    }

    // pkg private for tests
    int getInFlightCacheFills() {
        return MAX_IN_FLIGHT_CACHE_FILLS - inFlightCacheFills.availablePermits();
    }

    @Override
    protected void doClose() {}

    public CachedBlob get(
        final String repository,
        final SnapshotId snapshotId,
        final IndexId indexId,
        final ShardId shardId,
        final String name,
        final ByteRange range
    ) {
        assert Thread.currentThread().getName().contains('[' + ThreadPool.Names.SYSTEM_READ + ']') == false
            : "must not block [" + Thread.currentThread().getName() + "] for a cache read";

        final PlainActionFuture<CachedBlob> future = new PlainActionFuture<>();
        getAsync(repository, snapshotId, indexId, shardId, name, range, future);
        try {
            return future.actionGet(5, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    () -> format(
                        "get from cache index timed out after [5s], retrieving from blob store instead [id=%s]",
                        generateId(repository, snapshotId, indexId, shardId, name, range)
                    ),
                    e
                );
            } else {
                logger.warn("get from cache index timed out after [5s], retrieving from blob store instead");
            }
            return CachedBlob.CACHE_NOT_READY;
        }
    }

    final void getAsync(
        final String repository,
        final SnapshotId snapshotId,
        final IndexId indexId,
        final ShardId shardId,
        final String name,
        final ByteRange range,
        final ActionListener<CachedBlob> listener
    ) {
        if (closed.get()) {
            logger.debug("failed to retrieve cached blob from system index [{}], service is closed", index);
            listener.onResponse(CachedBlob.CACHE_NOT_READY);
            return;
        }
        final GetRequest request = new GetRequest(index).id(generateId(repository, snapshotId, indexId, shardId, name, range));
        innerGet(request, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse response) {
                if (response.isExists()) {
                    logger.debug("cache hit : [{}]", request.id());
                    assert response.isSourceEmpty() == false;

                    final CachedBlob cachedBlob = CachedBlob.fromSource(response.getSource());
                    assert assertDocId(response, repository, snapshotId, indexId, shardId, name, range);
                    if (cachedBlob.from() != range.start() || cachedBlob.to() != range.end()) {
                        // expected range in cache might differ with the returned cached blob; this can happen if the range to put in cache
                        // is changed between versions or through the index setting. In this case we assume it is a cache miss to force the
                        // blob to be cached again
                        listener.onResponse(CachedBlob.CACHE_MISS);
                    } else {
                        listener.onResponse(cachedBlob);
                    }
                } else {
                    logger.debug("cache miss: [{}]", request.id());
                    listener.onResponse(CachedBlob.CACHE_MISS);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // In case the blob cache system index is unavailable, we indicate it's not ready and move on. We do not fail the request:
                // a failure here is not fatal since the data exists in the blob store, so we can simply indicate the cache is not ready.
                if (isExpectedCacheGetException(e)) {
                    logger.debug(() -> "failed to retrieve cached blob from system index [" + index + "]", e);
                } else {
                    logger.warn(() -> "failed to retrieve cached blob from system index [" + index + "]", e);
                    assert false : e;
                }
                listener.onResponse(CachedBlob.CACHE_NOT_READY);
            }
        });
    }

    protected void innerGet(final GetRequest request, final ActionListener<GetResponse> listener) {
        client.get(request, listener);
    }

    private static boolean assertDocId(
        final GetResponse response,
        final String repository,
        final SnapshotId snapshotId,
        final IndexId indexId,
        final ShardId shardId,
        final String name,
        final ByteRange range
    ) {
        final String expectedId = generateId(repository, snapshotId, indexId, shardId, name, range);
        assert response.getId().equals(expectedId)
            : "Expected a cached blob document with id [" + expectedId + "] but got [" + response.getId() + ']';
        return true;
    }

    private static boolean isExpectedCacheGetException(Exception e) {
        if (TransportActions.isShardNotAvailableException(e)
            || e instanceof ConnectTransportException
            || e instanceof ClusterBlockException) {
            return true;
        }
        final Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof NodeClosedException || cause instanceof ConnectTransportException;
    }

    public final void putAsync(
        final String repository,
        final SnapshotId snapshotId,
        final IndexId indexId,
        final ShardId shardId,
        final String name,
        final ByteRange range,
        final BytesReference bytes,
        final long timeInEpochMillis,
        final ActionListener<Void> listener
    ) {
        if (closed.get()) {
            listener.onFailure(new IllegalStateException("Blob cache service is closed"));
            return;
        }
        final String id = generateId(repository, snapshotId, indexId, shardId, name, range);
        try {
            final CachedBlob cachedBlob = new CachedBlob(
                Instant.ofEpochMilli(timeInEpochMillis),
                repository,
                name,
                generatePath(snapshotId, indexId, shardId),
                bytes,
                range.start()
            );
            final IndexRequest request = new IndexRequest(index).id(id);
            try (XContentBuilder builder = jsonBuilder()) {
                request.source(cachedBlob.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }

            final RunOnce release = new RunOnce(() -> {
                final int availablePermits = inFlightCacheFills.availablePermits();
                assert availablePermits > 0 : "in-flight available permits should be greater than 0 but got: " + availablePermits;
                inFlightCacheFills.release();
            });

            boolean submitted = false;
            inFlightCacheFills.acquire();
            try {
                final ActionListener<Void> wrappedListener = ActionListener.runAfter(listener, release);
                innerPut(request, new ActionListener<>() {
                    @Override
                    public void onResponse(DocWriteResponse indexResponse) {
                        logger.trace("cache fill ({}): [{}]", indexResponse.status(), request.id());
                        wrappedListener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(() -> "failure in cache fill: [" + request.id() + "]", e);
                        wrappedListener.onFailure(e);
                    }
                });
                submitted = true;
            } finally {
                if (submitted == false) {
                    release.run();
                }
            }
        } catch (Exception e) {
            logger.warn(() -> "cache fill failure: [" + id + "]", e);
            listener.onFailure(e);
        }
    }

    protected void innerPut(final IndexRequest request, final ActionListener<DocWriteResponse> listener) {
        client.index(request, listener);
    }

    protected static String generateId(
        final String repository,
        final SnapshotId snapshotId,
        final IndexId indexId,
        final ShardId shardId,
        final String name,
        final ByteRange range
    ) {
        return String.join("/", repository, snapshotId.getUUID(), indexId.getId(), String.valueOf(shardId.id()), name, "@" + range.start());
    }

    protected static String generatePath(final SnapshotId snapshotId, final IndexId indexId, final ShardId shardId) {
        return String.join("/", snapshotId.getUUID(), indexId.getId(), String.valueOf(shardId.id()));
    }

    /**
     * Computes the {@link ByteRange} corresponding to the header of a Lucene file. This range can vary depending of the type of the file
     * which is indicated by the file's extension. The returned byte range can never be larger than the file's length but it can be smaller.
     *
     * For files that are declared as metadata files in {@link LuceneFilesExtensions}, the header can be as large as the specified
     * maximum metadata length parameter {@code maxMetadataLength}. Non-metadata files have a fixed length header of maximum 1KB.
     *
     * @param shardId the {@link ShardId} the file belongs to
     * @param fileName the name of the file
     * @param fileLength the length of the file
     * @param maxMetadataLength the maximum accepted length for metadata files
     *
     * @return the header {@link ByteRange}
     */
    public ByteRange computeBlobCacheByteRange(ShardId shardId, String fileName, long fileLength, ByteSizeValue maxMetadataLength) {
        final LuceneFilesExtensions fileExtension = LuceneFilesExtensions.fromExtension(IndexFileNames.getExtension(fileName));

        if (fileExtension != null && fileExtension.isMetadata()) {
            final long maxAllowedLengthInBytes = maxMetadataLength.getBytes();
            if (fileLength > maxAllowedLengthInBytes) {
                logExceedingFile(shardId, fileExtension, fileLength, maxMetadataLength);
            }
            return ByteRange.of(0L, Math.min(fileLength, maxAllowedLengthInBytes));
        }
        return ByteRange.of(0L, Math.min(fileLength, DEFAULT_CACHED_BLOB_SIZE));
    }

    private static void logExceedingFile(ShardId shardId, LuceneFilesExtensions extension, long length, ByteSizeValue maxAllowedLength) {
        if (logger.isInfoEnabled()) {
            try {
                // Use of a cache to prevent too many log traces per hour
                LOG_EXCEEDING_FILES_CACHE.computeIfAbsent(extension.getExtension(), key -> {
                    logger.info(
                        "{} file with extension [{}] is larger ([{}]) than the max. length allowed [{}] "
                            + "to cache metadata files in blob cache",
                        shardId,
                        extension,
                        length,
                        maxAllowedLength
                    );
                    return key;
                });
            } catch (ExecutionException e) {
                logger.warn(
                    () -> format(
                        "%s failed to log information about exceeding file type [%s] with length [%s]",
                        shardId,
                        extension,
                        length
                    ),
                    e
                );
            }
        }
    }
}

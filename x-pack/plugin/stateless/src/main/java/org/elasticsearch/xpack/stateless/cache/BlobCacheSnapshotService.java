/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.CacheManifest;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessThreadPools;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.LazyRangeMissingHandler;
import org.elasticsearch.xpack.stateless.cache.reader.SequentialRangeMissingHandler;
import org.elasticsearch.xpack.stateless.commits.BccBlob;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;

/**
 * Records the state of this node's shared blob cache and later restores the cache to a recorded state.
 * <p>
 * This lets a benchmark start every iteration from exactly the same cache, whatever the previous
 * iteration read or evicted: the driver prepares the cache once, calls {@link #snapshot}, and then
 * calls {@link #restore} before each iteration. A restore removes cache entries not mentioned in
 * the manifest and re-downloads entries that may have been evicted since the snapshot was taken.
 * <p>
 * Snapshot manifests are kept in memory on the node, and are lost on restart.
 * <p>
 * Both snapshot and restore expect that the cache is static while they execute - no indexing,
 * merging or querying should be running.
 */
public class BlobCacheSnapshotService {

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> {
        assert ThreadPool.assertCurrentThreadPool(
            StatelessThreadPools.PREWARM_THREAD_POOL,
            StatelessThreadPools.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        ) : "writeBuffer should only be used in the prewarm or fill vbcc thread pool but used in " + Thread.currentThread().getName();
        return ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE);
    });

    /**
     * @param shards  the number of shards whose cached regions were recorded
     * @param regions the number of regions recorded
     * @param bytes   the number of cached bytes recorded
     */
    public record SnapshotSummary(int shards, int regions, long bytes) {}

    /**
     * @param regionsEvicted    the number of regions evicted because the snapshot does not describe them
     * @param rangesDownloaded  the number of snapshot ranges for which data was downloaded
     * @param bytesCopied       the number of bytes copied from the object store into the cache
     * @param shardsSkipped     the number of shards in the snapshot that are no longer on this node
     */
    public record RestoreSummary(int regionsEvicted, int rangesDownloaded, long bytesCopied, int shardsSkipped) {}

    private final StatelessSharedBlobCacheService cacheService;
    private final Supplier<Map<ShardId, BlobStoreCacheDirectory>> localShardDirectories;
    private final ThreadPool threadPool;
    private final Map<String, CacheManifest<FileCacheKey>> snapshots = new ConcurrentHashMap<>();

    public BlobCacheSnapshotService(StatelessSharedBlobCacheService cacheService, IndicesService indicesService, ThreadPool threadPool) {
        this(cacheService, () -> localShardDirectories(indicesService), threadPool);
    }

    BlobCacheSnapshotService(
        StatelessSharedBlobCacheService cacheService,
        Supplier<Map<ShardId, BlobStoreCacheDirectory>> localShardDirectories,
        ThreadPool threadPool
    ) {
        this.cacheService = cacheService;
        this.localShardDirectories = localShardDirectories;
        this.threadPool = threadPool;
    }

    /**
     * Records the populated ranges of the regions referenced by the current commits of this node's shards, and keeps the record under
     * {@code name}. An earlier snapshot with the same name is replaced. Regions of blobs that are no longer referenced by a live commit
     * are skipped, as a restore could not download them.
     *
     * @throws IllegalStateException if a recorded blob is not uploaded to the object store yet, so a restore could not download it
     */
    public SnapshotSummary snapshot(String name) {
        final CacheManifest.Builder<FileCacheKey> builder = CacheManifest.builder();
        final Set<String> notUploaded = new TreeSet<>();
        int shards = 0;
        for (var shard : localShardDirectories.get().entrySet()) {
            final ShardId shardId = shard.getKey();
            final BlobStoreCacheDirectory directory = shard.getValue();
            final var isObsolete = directory.obsoleteRegionPredicate();
            shards++;
            for (var keyRegions : cacheService.captureManifest(key -> key.shardId().equals(shardId)).regions().entrySet()) {
                final FileCacheKey key = keyRegions.getKey();
                boolean referenced = false;
                for (var regionEntry : keyRegions.getValue().entrySet()) {
                    if (isObsolete.test(key, regionEntry.getKey()) == false) {
                        final var region = regionEntry.getValue();
                        builder.add(key, regionEntry.getKey(), region.timestampMillis(), region.ranges());
                        referenced = true;
                    }
                }
                if (referenced && isUploaded(directory, key) == false) {
                    notUploaded.add(key.fileName());
                }
            }
        }
        if (notUploaded.isEmpty() == false) {
            throw new IllegalStateException(
                "cannot snapshot the blob cache: ["
                    + notUploaded.size()
                    + "] blobs are not uploaded to the object store yet, for example "
                    + notUploaded.iterator().next()
                    + ". Flush all indices and wait for the uploads before taking a snapshot"
            );
        }
        final CacheManifest<FileCacheKey> manifest = builder.build();
        snapshots.put(name, manifest);
        return new SnapshotSummary(shards, manifest.regionCount(), manifest.totalBytes());
    }

    /**
     * Puts the cache back into the state recorded under {@code name}: evicts every region that holds data not described by
     * the snapshot, then downloads any snapshot ranges that are missing. The listener is completed once all of the data is in the cache.
     * <p>
     * The listener fails if a blob cannot be downloaded, for example because it was deleted after the snapshot was taken, or if some of
     * the data is still missing at the end. In that case the cache is only partly restored. Shards in the snapshot that are no longer on
     * this node are skipped.
     */
    public void restore(String name, ActionListener<RestoreSummary> listener) {
        final CacheManifest<FileCacheKey> manifest = snapshots.get(name);
        if (manifest == null) {
            listener.onFailure(new ResourceNotFoundException("no blob cache snapshot named [" + name + "]"));
            return;
        }
        // the directories only create blob readers for warming on a generic thread
        threadPool.generic()
            .execute(
                ActionRunnable.wrap(
                    listener,
                    restoreListener -> new Restore(name, manifest, cacheService.evictNotCoveredBy(manifest), restoreListener).run()
                )
            );
    }

    // Blobs of indexing shards are only cached after they are uploaded, and their directory only references uploaded commits.
    private static boolean isUploaded(BlobStoreCacheDirectory directory, FileCacheKey key) {
        if (directory instanceof SearchDirectory searchDirectory) {
            return searchDirectory.isBccUploaded(new BccBlob(key.fileName(), key.primaryTerm()).termAndGeneration());
        }
        return true;
    }

    private static Map<ShardId, BlobStoreCacheDirectory> localShardDirectories(IndicesService indicesService) {
        final Map<ShardId, BlobStoreCacheDirectory> directories = new HashMap<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                try {
                    directories.put(shard.shardId(), BlobStoreCacheDirectory.unwrapDirectory(shard.store().directory()));
                } catch (AlreadyClosedException e) {
                    // the shard is closing, so its cache regions are about to be evicted
                }
            }
        }
        return directories;
    }

    private class Restore {
        private final String name;
        private final CacheManifest<FileCacheKey> manifest;
        private final int regionsEvicted;
        private final ActionListener<RestoreSummary> listener;
        private final Set<ShardId> skippedShards = new HashSet<>();
        private final AtomicReference<Exception> firstFailure = new AtomicReference<>();
        private final AtomicInteger failedRanges = new AtomicInteger();
        private final AtomicInteger rangesDownloaded = new AtomicInteger();
        private final LongAdder bytesCopied = new LongAdder();

        Restore(String name, CacheManifest<FileCacheKey> manifest, int regionsEvicted, ActionListener<RestoreSummary> listener) {
            this.name = name;
            this.manifest = manifest;
            this.regionsEvicted = regionsEvicted;
            this.listener = listener;
        }

        void run() {
            final Map<ShardId, BlobStoreCacheDirectory> directories = localShardDirectories.get();
            final Executor fetchExecutor = threadPool.executor(StatelessThreadPools.PREWARM_THREAD_POOL);
            try (var refs = new RefCountingRunnable(this::finish)) {
                for (var keyRegions : manifest.regions().entrySet()) {
                    final FileCacheKey key = keyRegions.getKey();
                    final BlobStoreCacheDirectory directory = directories.get(key.shardId());
                    if (directory == null) {
                        skippedShards.add(key.shardId());
                        continue;
                    }
                    final CacheBlobReader reader;
                    try {
                        reader = directory.getCacheBlobReaderForWarming(new BccBlob(key.fileName(), key.primaryTerm()));
                    } catch (Exception e) {
                        recordFailure(e);
                        continue;
                    }
                    for (var regionEntry : keyRegions.getValue().entrySet()) {
                        final int region = regionEntry.getKey();
                        final long regionStart = cacheService.getRegionStart(region);
                        for (ByteRange relativeRange : regionEntry.getValue().ranges()) {
                            final ByteRange range = relativeRange.shift(regionStart);
                            cacheService.fetchRange(
                                key,
                                region,
                                range,
                                (long) (region + 1) * cacheService.getRegionSize(),
                                new LazyRangeMissingHandler<>(
                                    () -> new SequentialRangeMissingHandler(
                                        this,
                                        key.fileName(),
                                        range,
                                        reader,
                                        () -> writeBuffer.get().clear(),
                                        bytes -> bytesCopied.add(bytes),
                                        StatelessThreadPools.PREWARM_THREAD_POOL
                                    )
                                ),
                                fetchExecutor,
                                false,
                                regionEntry.getValue().timestampMillis(),
                                true,
                                ActionListener.releaseAfter(ActionListener.wrap(downloaded -> {
                                    if (downloaded) {
                                        rangesDownloaded.incrementAndGet();
                                    }
                                }, this::recordFailure), refs.acquire())
                            );
                        }
                    }
                }
            }
        }

        private void recordFailure(Exception e) {
            failedRanges.incrementAndGet();
            firstFailure.accumulateAndGet(e, (first, next) -> first == null ? next : ExceptionsHelper.useOrSuppress(first, next));
        }

        private void finish() {
            final int missingRegions = countMissingRegions();
            if (failedRanges.get() > 0 || missingRegions > 0) {
                listener.onFailure(
                    new ElasticsearchException(
                        "blob cache restore ["
                            + name
                            + "] is incomplete: ["
                            + failedRanges.get()
                            + "] ranges failed to download and ["
                            + missingRegions
                            + "] regions are still missing data. The cache is only partly restored",
                        firstFailure.get()
                    )
                );
            } else {
                listener.onResponse(new RestoreSummary(regionsEvicted, rangesDownloaded.get(), bytesCopied.sum(), skippedShards.size()));
            }
        }

        private int countMissingRegions() {
            final CacheManifest<FileCacheKey> current = cacheService.captureManifest(key -> true);
            int missing = 0;
            for (var keyRegions : manifest.regions().entrySet()) {
                if (skippedShards.contains(keyRegions.getKey().shardId())) {
                    continue;
                }
                for (var regionEntry : keyRegions.getValue().entrySet()) {
                    final var present = current.get(keyRegions.getKey(), regionEntry.getKey());
                    if (present == null || present.covers(regionEntry.getValue().ranges()) == false) {
                        missing++;
                    }
                }
            }
            return missing;
        }
    }
}

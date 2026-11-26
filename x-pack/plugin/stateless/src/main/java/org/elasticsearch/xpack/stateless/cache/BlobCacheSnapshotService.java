/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.CacheManifest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessThreadPools;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.LazyRangeMissingHandler;
import org.elasticsearch.xpack.stateless.cache.reader.SequentialRangeMissingHandler;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BccBlob;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiPredicate;
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

    /**
     * How many failures a failed restore keeps as suppressed exceptions, as one missing blob fails every range that is read from it.
     */
    private static final int MAX_REPORTED_FAILURES = 5;

    private final StatelessSharedBlobCacheService cacheService;
    private final Supplier<Map<ShardId, BlobStoreCacheDirectory>> localShardDirectories;
    private final BiPredicate<ShardId, PrimaryTermAndGeneration> indexingBccUploaded;
    private final ThreadPool threadPool;
    private final Map<String, CacheManifest<FileCacheKey>> snapshots = new ConcurrentHashMap<>();

    /**
     * @param commitService the commit service that knows what indexing shards have uploaded, or {@code null} on a node without the index
     *                      role
     */
    public BlobCacheSnapshotService(
        StatelessSharedBlobCacheService cacheService,
        IndicesService indicesService,
        ThreadPool threadPool,
        @Nullable StatelessCommitService commitService
    ) {
        this(
            cacheService,
            () -> localShardDirectories(indicesService),
            (shardId, bcc) -> isUploaded(commitService, shardId, bcc),
            threadPool
        );
    }

    BlobCacheSnapshotService(
        StatelessSharedBlobCacheService cacheService,
        Supplier<Map<ShardId, BlobStoreCacheDirectory>> localShardDirectories,
        BiPredicate<ShardId, PrimaryTermAndGeneration> indexingBccUploaded,
        ThreadPool threadPool
    ) {
        this.cacheService = cacheService;
        this.localShardDirectories = localShardDirectories;
        this.indexingBccUploaded = indexingBccUploaded;
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
        final Map<ShardId, BlobStoreCacheDirectory> directories = localShardDirectories.get();
        final Map<ShardId, BiPredicate<FileCacheKey, Integer>> obsoleteRegionPredicates = new HashMap<>();
        final CacheManifest.Builder<FileCacheKey> builder = CacheManifest.builder();
        final Set<String> notUploaded = new TreeSet<>();
        final Set<ShardId> recordedShards = new HashSet<>();
        // the cache is scanned once, however many shards the node has
        for (var keyRegions : cacheService.captureManifest(key -> directories.containsKey(key.shardId())).regions().entrySet()) {
            final FileCacheKey key = keyRegions.getKey();
            final BlobStoreCacheDirectory directory = directories.get(key.shardId());
            final var isObsolete = obsoleteRegionPredicates.computeIfAbsent(key.shardId(), shardId -> directory.obsoleteRegionPredicate());
            boolean referenced = false;
            for (var regionEntry : keyRegions.getValue().entrySet()) {
                if (isObsolete.test(key, regionEntry.getKey()) == false) {
                    final var region = regionEntry.getValue();
                    builder.add(key, regionEntry.getKey(), region.timestampMillis(), region.ranges());
                    referenced = true;
                }
            }
            if (referenced) {
                recordedShards.add(key.shardId());
                if (isUploaded(directory, key) == false) {
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
        return new SnapshotSummary(recordedShards.size(), manifest.regionCount(), manifest.totalBytes());
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

    // An indexing shard warms its cache from a batched compound commit before it is uploaded, so the cache can hold blobs that a restore
    // would not find in the object store.
    private boolean isUploaded(BlobStoreCacheDirectory directory, FileCacheKey key) {
        final PrimaryTermAndGeneration bcc = new BccBlob(key.fileName(), key.primaryTerm()).termAndGeneration();
        if (directory instanceof SearchDirectory searchDirectory) {
            return searchDirectory.isBccUploaded(bcc);
        }
        return indexingBccUploaded.test(key.shardId(), bcc);
    }

    private static boolean isUploaded(@Nullable StatelessCommitService commitService, ShardId shardId, PrimaryTermAndGeneration bcc) {
        if (commitService == null) {
            return false;
        }
        try {
            final BatchedCompoundCommit latestUploaded = commitService.getLatestUploadedBcc(shardId);
            return latestUploaded != null && bcc.generation() <= latestUploaded.primaryTermAndGeneration().generation();
        } catch (AlreadyClosedException e) {
            return false;
        }
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
        private final AtomicInteger failedRanges = new AtomicInteger();
        private Exception firstFailure; // guarded by this
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
                                // The snapshot came from this cache and everything it does not describe was evicted, so there is room for
                                // every region it needs. Without force, fetching more ranges into a region that took the last free slot
                                // would be skipped, or would evict a region that the snapshot needs.
                                true,
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
            final int failures = failedRanges.incrementAndGet();
            synchronized (this) {
                if (firstFailure == null) {
                    firstFailure = e;
                } else if (failures <= MAX_REPORTED_FAILURES) {
                    firstFailure.addSuppressed(e);
                }
            }
        }

        private synchronized Exception firstFailure() {
            return firstFailure;
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
                        firstFailure()
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
                    final var wanted = regionEntry.getValue();
                    if (present == null
                        || present.covers(wanted.ranges()) == false
                        || present.timestampMillis() != wanted.timestampMillis()) {
                        missing++;
                    }
                }
            }
            return missing;
        }
    }
}

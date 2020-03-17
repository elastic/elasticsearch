/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.BaseSearchableSnapshotDirectory;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * {@link CacheDirectory} uses a {@link CacheService} to cache Lucene files provided by another {@link Directory}.
 */
public class CacheDirectory extends BaseSearchableSnapshotDirectory {

    private final Map<String, IndexInputStats> stats;
    private final CacheService cacheService;
    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final ShardId shardId;
    private final Path cacheDir;
    private final LongSupplier currentTimeNanosSupplier;

    public CacheDirectory(final Supplier<BlobStoreIndexShardSnapshot> snapshot, final Supplier<BlobContainer> blobContainer,
                          CacheService cacheService, Path cacheDir, SnapshotId snapshotId, IndexId indexId, ShardId shardId,
                          LongSupplier currentTimeNanosSupplier)
        throws IOException {
        super(blobContainer, snapshot);
        this.stats = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.cacheService = Objects.requireNonNull(cacheService);
        this.cacheDir = Files.createDirectories(cacheDir);
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indexId = Objects.requireNonNull(indexId);
        this.shardId = Objects.requireNonNull(shardId);
        this.currentTimeNanosSupplier = Objects.requireNonNull(currentTimeNanosSupplier);
    }

    CacheKey createCacheKey(String fileName) {
        return new CacheKey(snapshotId, indexId, shardId, fileName);
    }

    CacheFile getCacheFile(CacheKey cacheKey, long fileLength) throws Exception {
        return cacheService.get(cacheKey, fileLength, cacheDir);
    }

    public SnapshotId getSnapshotId() {
        return snapshotId;
    }

    public IndexId getIndexId() {
        return indexId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public Map<String, IndexInputStats> getStats() {
        return Collections.unmodifiableMap(stats);
    }

    // pkg private for tests
    @Nullable
    IndexInputStats getStats(String name) {
        return stats.get(name);
    }

    // pkg private so tests can override
    IndexInputStats createIndexInputStats(final long fileLength) {
        return new IndexInputStats(fileLength);
    }

    long statsCurrentTimeNanos() {
        return currentTimeNanosSupplier.getAsLong();
    }

    @Override
    protected void innerClose() {
        // Ideally we could let the cache evict/remove cached files by itself after the
        // directory has been closed.
        clearCache();
    }

    public void clearCache() {
        cacheService.removeFromCache(cacheKey -> cacheKey.belongsTo(snapshotId, indexId, shardId));
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();
        final FileInfo fileInfo = fileInfo(name);
        final IndexInputStats inputStats = stats.computeIfAbsent(name, n -> createIndexInputStats(fileInfo.length()));
        return new CacheBufferedIndexInput(this, fileInfo, context, inputStats);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;

import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;

/**
 * This {@link IndexStorePlugin.IndexFoldersDeletionListener} is called when an index folder or a shard folder is deleted from the disk. If
 * the index (or the shard) is a backed by a snapshot this listener notifies the {@link CacheService} that the cache files associated to the
 * shard(s) must be evicted.
 */
public class SearchableSnapshotIndexFoldersDeletionListener implements IndexStorePlugin.IndexFoldersDeletionListener {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotIndexEventListener.class);

    private final Supplier<CacheService> cacheServiceSupplier;
    private final Supplier<SharedBlobCacheService<CacheKey>> frozenCacheServiceSupplier;

    public SearchableSnapshotIndexFoldersDeletionListener(
        Supplier<CacheService> cacheServiceSupplier,
        Supplier<SharedBlobCacheService<CacheKey>> frozenCacheServiceSupplier
    ) {
        this.cacheServiceSupplier = Objects.requireNonNull(cacheServiceSupplier);
        this.frozenCacheServiceSupplier = Objects.requireNonNull(frozenCacheServiceSupplier);
    }

    @Override
    public void beforeIndexFoldersDeleted(
        Index index,
        IndexSettings indexSettings,
        Path[] indexPaths,
        IndexRemovalReason indexRemovalReason
    ) {
        if (indexSettings.getIndexMetadata().isSearchableSnapshot()) {
            for (int shard = 0; shard < indexSettings.getNumberOfShards(); shard++) {
                markShardAsEvictedInCache(new ShardId(index, shard), indexSettings, indexRemovalReason);
            }
        }
    }

    @Override
    public void beforeShardFoldersDeleted(
        ShardId shardId,
        IndexSettings indexSettings,
        Path[] shardPaths,
        IndexRemovalReason indexRemovalReason
    ) {
        if (indexSettings.getIndexMetadata().isSearchableSnapshot()) {
            markShardAsEvictedInCache(shardId, indexSettings, indexRemovalReason);
        }
    }

    private void markShardAsEvictedInCache(ShardId shardId, IndexSettings indexSettings, IndexRemovalReason indexRemovalReason) {
        final CacheService cacheService = this.cacheServiceSupplier.get();
        assert cacheService != null : "cache service not initialized";

        logger.debug("{} marking shard as evicted in searchable snapshots cache (reason: cache files deleted from disk)", shardId);
        cacheService.markShardAsEvictedInCache(
            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings()),
            SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings.getSettings()),
            shardId
        );

        // Only partial searchable snapshots use the shared blob cache.
        if (indexSettings.getIndexMetadata().isPartialSearchableSnapshot()) {
            switch (indexRemovalReason) {
                // The index was deleted, it's not coming back - we can evict asynchronously
                case DELETED -> {
                    final SharedBlobCacheService<CacheKey> sharedBlobCacheService =
                        SearchableSnapshotIndexFoldersDeletionListener.this.frozenCacheServiceSupplier.get();
                    assert sharedBlobCacheService != null : "frozen cache service not initialized";
                    sharedBlobCacheService.forceEvictAsync(SearchableSnapshots.forceEvictPredicate(shardId, indexSettings.getSettings()));
                }
                // An error occurred - we should eagerly clear the state
                case FAILURE -> {
                    final SharedBlobCacheService<CacheKey> sharedBlobCacheService =
                        SearchableSnapshotIndexFoldersDeletionListener.this.frozenCacheServiceSupplier.get();
                    assert sharedBlobCacheService != null : "frozen cache service not initialized";
                    sharedBlobCacheService.forceEvict(SearchableSnapshots.forceEvictPredicate(shardId, indexSettings.getSettings()));
                }
                // Any other reason - we let the cache entries expire naturally
            }
        }
    }
}

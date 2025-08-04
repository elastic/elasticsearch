/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ByteSizeCachingDirectory;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory.unwrapDirectory;

public class SearchableSnapshotIndexEventListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotIndexEventListener.class);

    private final @Nullable CacheService cacheService;
    private final @Nullable SharedBlobCacheService<CacheKey> sharedBlobCacheService;

    public SearchableSnapshotIndexEventListener(
        Settings settings,
        @Nullable CacheService cacheService,
        @Nullable SharedBlobCacheService<CacheKey> sharedBlobCacheService
    ) {
        assert cacheService != null || DiscoveryNode.canContainData(settings) == false;
        this.cacheService = cacheService;
        this.sharedBlobCacheService = sharedBlobCacheService;
    }

    /**
     * Called before a searchable snapshot {@link IndexShard} starts to recover. This event is used to trigger the loading of the shard
     * snapshot information that contains the list of shard's Lucene files.
     *
     * @param indexShard    the shard that is about to recover
     * @param indexSettings the shard's index settings
     */
    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        ensureSnapshotIsLoaded(indexShard);
        var sizeCachingDirectory = ByteSizeCachingDirectory.unwrapDirectory(indexShard.store().directory());
        if (sizeCachingDirectory != null) {
            // Marks the cached estimation of the directory size as stale in ByteSizeCachingDirectory since we just loaded the snapshot
            // files list into the searchable snapshot directory.
            sizeCachingDirectory.markEstimatedSizeAsStale();
        }
        listener.onResponse(null);
    }

    private static void ensureSnapshotIsLoaded(IndexShard indexShard) {
        final var store = indexShard.store();
        final SearchableSnapshotDirectory directory = unwrapDirectory(store.directory());
        assert directory != null;
        final ListenableFuture<Void> preWarmListener = new ListenableFuture<>();
        final boolean success = directory.loadSnapshot(indexShard.recoveryState(), store::isClosing, preWarmListener);
        final ShardRouting shardRouting = indexShard.routingEntry();
        if (success && shardRouting.isRelocationTarget()) {
            final Runnable preWarmCondition = indexShard.addCleanFilesDependency();
            preWarmListener.addListener(ActionListener.wrap(v -> preWarmCondition.run(), e -> {
                logger.warn(
                    () -> format(
                        "pre-warm operation failed for [%s] while it was the target of primary relocation [%s]",
                        shardRouting.shardId(),
                        shardRouting
                    ),
                    e
                );
                preWarmCondition.run();
            }));
        }
        assert directory.listAll().length > 0 : "expecting directory listing to be non-empty";
        assert success || indexShard.routingEntry().recoverySource().getType() == RecoverySource.Type.PEER
            : "loading snapshot must not be called twice unless we are retrying a peer recovery";
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        if (shouldEvictCacheFiles(reason)) {
            if (indexService.getMetadata().isSearchableSnapshot()) {
                final IndexSettings indexSettings = indexService.getIndexSettings();
                for (IndexShard indexShard : indexService) {
                    final ShardId shardId = indexShard.shardId();

                    logger.debug("{} marking shard as evicted in searchable snapshots cache (reason: {})", shardId, reason);
                    if (cacheService != null) {
                        cacheService.markShardAsEvictedInCache(
                            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings()),
                            SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings.getSettings()),
                            shardId
                        );
                    }
                    if (indexSettings.getIndexMetadata().isPartialSearchableSnapshot() && sharedBlobCacheService != null) {
                        switch (reason) {
                            // This index was deleted, it's not coming back - we can evict asynchronously
                            case DELETED -> sharedBlobCacheService.forceEvictAsync(
                                SearchableSnapshots.forceEvictPredicate(shardId, indexSettings.getSettings())
                            );
                            // A failure occurred - we should eagerly clear the state
                            case FAILURE -> sharedBlobCacheService.forceEvict(
                                SearchableSnapshots.forceEvictPredicate(shardId, indexSettings.getSettings())
                            );
                            // Any other reason - we let the cache entries expire naturally
                        }
                    }
                }
            }
        }
    }

    private static boolean shouldEvictCacheFiles(IndexRemovalReason reason) {
        return reason == IndexRemovalReason.DELETED
            || reason == IndexRemovalReason.NO_LONGER_ASSIGNED
            || reason == IndexRemovalReason.FAILURE;
    }
}

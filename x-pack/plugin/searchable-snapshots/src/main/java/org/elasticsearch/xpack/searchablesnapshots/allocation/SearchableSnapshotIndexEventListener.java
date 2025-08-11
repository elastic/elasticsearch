/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.nio.file.Path;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.isSearchableSnapshotStore;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory.unwrapDirectory;

public class SearchableSnapshotIndexEventListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotIndexEventListener.class);

    private final @Nullable CacheService cacheService;
    private final @Nullable FrozenCacheService frozenCacheService;

    public SearchableSnapshotIndexEventListener(
        Settings settings,
        @Nullable CacheService cacheService,
        @Nullable FrozenCacheService frozenCacheService
    ) {
        assert cacheService != null || DiscoveryNode.canContainData(settings) == false;
        this.cacheService = cacheService;
        this.frozenCacheService = frozenCacheService;
    }

    /**
     * Called before a searchable snapshot {@link IndexShard} starts to recover. This event is used to trigger the loading of the shard
     * snapshot information that contains the list of shard's Lucene files.
     *
     * @param indexShard    the shard that is about to recover
     * @param indexSettings the shard's index settings
     */
    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings) {
        assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
        ensureSnapshotIsLoaded(indexShard);
        associateNewEmptyTranslogWithIndex(indexShard);
    }

    private static void ensureSnapshotIsLoaded(IndexShard indexShard) {
        final Store store = indexShard.store();
        final SearchableSnapshotDirectory directory = unwrapDirectory(store.directory());
        assert directory != null;
        final StepListener<Void> preWarmListener = new StepListener<>();
        final boolean success = directory.loadSnapshot(indexShard.recoveryState(), store::isClosing, preWarmListener);
        final ShardRouting shardRouting = indexShard.routingEntry();
        if (success && shardRouting.isRelocationTarget()) {
            final Runnable preWarmCondition = indexShard.addCleanFilesDependency();
            preWarmListener.whenComplete(v -> preWarmCondition.run(), e -> {
                logger.warn(
                    new ParameterizedMessage(
                        "pre-warm operation failed for [{}] while it was the target of primary relocation [{}]",
                        shardRouting.shardId(),
                        shardRouting
                    ),
                    e
                );
                preWarmCondition.run();
            });
        }
        assert directory.listAll().length > 0 : "expecting directory listing to be non-empty";
        assert success || indexShard.routingEntry().recoverySource().getType() == RecoverySource.Type.PEER
            : "loading snapshot must not be called twice unless we are retrying a peer recovery";
    }

    private static void associateNewEmptyTranslogWithIndex(IndexShard indexShard) {
        final ShardId shardId = indexShard.shardId();
        assert isSearchableSnapshotStore(indexShard.indexSettings().getSettings()) : "Expected a searchable snapshot shard " + shardId;
        try {
            final SegmentInfos segmentInfos = indexShard.store().readLastCommittedSegmentsInfo();
            final long localCheckpoint = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
            final long primaryTerm = indexShard.getPendingPrimaryTerm();
            final String translogUUID = segmentInfos.userData.get(Translog.TRANSLOG_UUID_KEY);
            final Path translogLocation = indexShard.shardPath().resolveTranslog();
            Translog.createEmptyTranslog(translogLocation, shardId, localCheckpoint, primaryTerm, translogUUID, null);
        } catch (Exception e) {
            throw new TranslogException(shardId, "failed to associate a new translog", e);
        }
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        if (shouldEvictCacheFiles(reason)) {
            final IndexSettings indexSettings = indexService.getIndexSettings();
            if (isSearchableSnapshotStore(indexSettings.getSettings())) {
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
                    if (frozenCacheService != null) {
                        frozenCacheService.markShardAsEvictedInCache(
                            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings()),
                            SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings.getSettings()),
                            shardId
                        );
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

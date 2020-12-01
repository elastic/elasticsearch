/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.nio.file.Path;

import static org.elasticsearch.index.store.SearchableSnapshotDirectory.unwrapDirectory;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.isSearchableSnapshotStore;

public class SearchableSnapshotIndexEventListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotIndexEventListener.class);
    private final CacheService cacheService;

    public SearchableSnapshotIndexEventListener(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings) {
        assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
        ensureSnapshotIsLoaded(indexShard);
        associateNewEmptyTranslogWithIndex(indexShard);
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        if (reason == IndexRemovalReason.DELETED || reason == IndexRemovalReason.FAILURE) {
            if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexService.getIndexSettings().getSettings())) {
                for (IndexShard indexShard : indexService) {
                    try {
                        final SearchableSnapshotDirectory directory = unwrapDirectory(indexShard.store().directory());
                        assert directory != null : "expect a searchable snapshot directory instance";
                        directory.clearCacheOnClose();
                    } catch (Exception e) {
                        logger.warn("failed to close shard", e);
                    }
                }
            }
        }
    }

    @Override
    public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        if (SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings)) {
            final SnapshotId snapshotId = new SnapshotId(
                SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
            );
            final IndexId indexId = new IndexId(
                SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings),
                SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
            );
            // cacheService.removeFromCache(cacheKey -> cacheKey.belongsTo(snapshotId, indexId, shardId));
        }
    }

    private static void ensureSnapshotIsLoaded(IndexShard indexShard) {
        final SearchableSnapshotDirectory directory = unwrapDirectory(indexShard.store().directory());
        assert directory != null;

        final boolean success = directory.loadSnapshot(indexShard.recoveryState());
        assert directory.listAll().length > 0 : "expecting directory listing to be non-empty";
        assert success
            || indexShard.routingEntry()
                .recoverySource()
                .getType() == RecoverySource.Type.PEER : "loading snapshot must not be called twice unless we are retrying a peer recovery";
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
}

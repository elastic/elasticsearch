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
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;

import static org.elasticsearch.index.store.SearchableSnapshotDirectory.unwrapDirectory;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.isSearchableSnapshotStore;

public class SearchableSnapshotIndexEventListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotIndexEventListener.class);

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

    /**
     * Called before closing an {@link IndexService}. This event is used to toggle the "clear cache on closing" flag of the existing
     * searchable snapshot directory instance. This way the directory will clean up its cache entries for us.
     *
     * @param indexService The index service
     * @param reason       the reason for index removal
     */
    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        if (reason == IndexRemovalReason.DELETED
            || reason == IndexRemovalReason.NO_LONGER_ASSIGNED
            || reason == IndexRemovalReason.FAILURE) {
            if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexService.getIndexSettings().getSettings())) {
                for (IndexShard indexShard : indexService) {
                    try {
                        final SearchableSnapshotDirectory directory = unwrapDirectory(indexShard.store().directory());
                        assert directory != null : "expect a searchable snapshot directory instance";
                        directory.clearCacheOnClose();
                    } catch (Exception e) {
                        logger.warn("something went wrong when setting clear-on-close flag", e);
                    }
                }
            }
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

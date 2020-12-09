/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.isSearchableSnapshotStore;

public class SearchableSnapshotIndexEventListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotIndexEventListener.class);

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings) {
        assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
        ensureSnapshotIsLoaded(indexShard);
        associateNewEmptyTranslogWithIndex(indexShard);
    }

    private static void ensureSnapshotIsLoaded(IndexShard indexShard) {
        final SearchableSnapshotDirectory directory = SearchableSnapshotDirectory.unwrapDirectory(indexShard.store().directory());
        assert directory != null;
        final StepListener<Void> preWarmListener = new StepListener<>();
        final boolean success = directory.loadSnapshot(indexShard.recoveryState(), preWarmListener);
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

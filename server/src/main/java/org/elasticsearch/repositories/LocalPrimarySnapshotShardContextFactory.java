/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;

import static org.elasticsearch.repositories.SnapshotShardContextHelper.acquireSnapshotIndexCommit;
import static org.elasticsearch.repositories.SnapshotShardContextHelper.closeSnapshotIndexCommit;

/**
 * A factory implementation for creating {@link LocalPrimarySnapshotShardContext} instance from the primary shard
 * running on the local node.
 */
public class LocalPrimarySnapshotShardContextFactory implements SnapshotShardContextFactory {

    private static final Logger logger = LogManager.getLogger(LocalPrimarySnapshotShardContextFactory.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    public LocalPrimarySnapshotShardContextFactory(ClusterService clusterService, IndicesService indicesService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    public SubscribableListener<SnapshotShardContext> asyncCreate(
        ShardId shardId,
        Snapshot snapshot,
        IndexId indexId,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) throws IOException {
        final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        final var snapshotIndexCommitAndShardStateId = acquireSnapshotIndexCommit(
            clusterService,
            indexShard,
            snapshot,
            supportsRelocationDuringSnapshot(),
            snapshotStatus
        );
        try {
            return SubscribableListener.newSucceeded(
                new LocalPrimarySnapshotShardContext(
                    indexShard.store(),
                    indexShard.mapperService(),
                    snapshot.getSnapshotId(),
                    indexId,
                    snapshotIndexCommitAndShardStateId.snapshotIndexCommit(),
                    snapshotIndexCommitAndShardStateId.shardStateId(),
                    snapshotStatus,
                    repositoryMetaVersion,
                    snapshotStartTime,
                    listener
                )
            );
        } catch (Exception e) {
            closeSnapshotIndexCommit(snapshotIndexCommitAndShardStateId.snapshotIndexCommit(), shardId, snapshot);
            throw e;
        }
    }

}

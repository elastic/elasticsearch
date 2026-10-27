/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

/**
 * Builds cluster states in which an index is being restored from a snapshot, for unit tests of the code that reports such a shard as a
 * {@link ShardRestoringException}. The real classes are used rather than mocks, so the states are the ones a restore produces: the
 * primary is initializing from a {@link RecoverySource.SnapshotRecoverySource} and a started {@link RestoreInProgress} entry lists it.
 */
public final class RestoringShardTestState {

    private RestoringShardTestState() {}

    /**
     * @param state       the cluster state, which has the restoring index in addition to what {@code base} had
     * @param shardId     the one shard of the restoring index
     * @param restoreUuid the UUID of the restore, which a {@link ShardRestoringException} carries as its recovery ID
     */
    public record Restoring(ClusterState state, ShardId shardId, String restoreUuid) {}

    /**
     * Adds to {@code base} an index with one shard whose primary is initializing from a snapshot, so it has no copy that can be searched,
     * with a restore that reports its shards if {@code reportShardRestoring} is set. The state can be built on again to restore more
     * indices. {@code base} must have the project {@code projectId}.
     */
    public static Restoring restoringIndex(
        ClusterState base,
        ProjectId projectId,
        String indexName,
        String restoreUuid,
        boolean reportShardRestoring
    ) {
        final String indexUuid = ESTestCase.randomUUID();
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(ESTestCase.indexSettings(IndexVersion.current(), indexUuid, 1, 0))
            .build();
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final Snapshot snapshot = new Snapshot(
            projectId,
            ESTestCase.randomIdentifier(),
            new SnapshotId(ESTestCase.randomIdentifier(), ESTestCase.randomUUID())
        );
        final ShardRouting primary = TestShardRouting.shardRoutingBuilder(shardId, "node", true, ShardRoutingState.INITIALIZING)
            .withRecoverySource(
                new RecoverySource.SnapshotRecoverySource(restoreUuid, snapshot, IndexVersion.current(), new IndexId(indexName, indexUuid))
            )
            .build();
        final RestoreInProgress restoreInProgress = new RestoreInProgress.Builder(RestoreInProgress.get(base)).add(
            new RestoreInProgress.Entry(
                restoreUuid,
                snapshot,
                RestoreInProgress.State.STARTED,
                false,
                List.of(indexName),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus("node")),
                reportShardRestoring
            )
        ).build();
        final ClusterState state = ClusterState.builder(base)
            .putProjectMetadata(ProjectMetadata.builder(base.metadata().getProject(projectId)).put(indexMetadata, false))
            .putRoutingTable(
                projectId,
                RoutingTable.builder(base.routingTable(projectId))
                    .add(IndexRoutingTable.builder(shardId.getIndex()).addShard(primary))
                    .build()
            )
            .putCustom(RestoreInProgress.TYPE, restoreInProgress)
            .build();
        return new Restoring(state, shardId, restoreUuid);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

public enum IndicesClusterStateServiceTestUtils {
    ;

    // exposed outside package for tests
    public static IndicesClusterStateService newIndicesClusterStateService(
        final Settings settings,
        final IndicesClusterStateService.AllocatedIndices<
            ? extends IndicesClusterStateService.Shard,
            ? extends IndicesClusterStateService.AllocatedIndex<? extends IndicesClusterStateService.Shard>> indicesService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final Executor applyExecutor,
        final PeerRecoveryTargetService recoveryTargetService,
        final ShardStateAction shardStateAction,
        final RepositoriesService repositoriesService,
        final SearchService searchService,
        final PeerRecoverySourceService peerRecoverySourceService,
        final SnapshotShardsService snapshotShardsService,
        final PrimaryReplicaSyncer primaryReplicaSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final NodeClient client
    ) {
        return new IndicesClusterStateService(
            settings,
            indicesService,
            clusterService,
            threadPool,
            applyExecutor,
            recoveryTargetService,
            shardStateAction,
            repositoriesService,
            searchService,
            peerRecoverySourceService,
            snapshotShardsService,
            primaryReplicaSyncer,
            retentionLeaseSyncer,
            client
        );
    }
}

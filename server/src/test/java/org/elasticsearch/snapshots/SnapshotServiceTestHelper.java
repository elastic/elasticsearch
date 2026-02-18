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
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryShardId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;

/**
 * Shared test helpers for snapshot unit tests.
 */
class SnapshotServiceTestHelper {

    private SnapshotServiceTestHelper() {}

    static String uuid() {
        return UUIDs.randomBase64UUID(random());
    }

    static Snapshot snapshot(String repoName, String name) {
        return new Snapshot(ProjectId.DEFAULT, repoName, new SnapshotId(name, uuid()));
    }

    static SnapshotsInProgress.Entry snapshotEntry(
        Snapshot snapshot,
        Map<String, IndexId> indexIds,
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards
    ) {
        return SnapshotsInProgress.startedEntry(
            snapshot,
            random().nextBoolean(),
            random().nextBoolean(),
            indexIds,
            Collections.emptyList(),
            1L,
            randomNonNegativeLong(),
            shards,
            Collections.emptyMap(),
            IndexVersion.current(),
            Collections.emptyList()
        );
    }

    static SnapshotsInProgress.Entry cloneEntry(
        Snapshot snapshot,
        SnapshotId source,
        Map<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> clones
    ) {
        final Map<String, IndexId> indexIds = clones.keySet()
            .stream()
            .map(RepositoryShardId::index)
            .distinct()
            .collect(Collectors.toMap(IndexId::getName, Function.identity()));
        return SnapshotsInProgress.startClone(snapshot, source, indexIds, 1L, randomNonNegativeLong(), IndexVersion.current())
            .withClones(clones);
    }

    static ClusterState stateWithSnapshots(ClusterState state, String repository, SnapshotsInProgress.Entry... entries) {
        return ClusterState.builder(state)
            .version(state.version() + 1L)
            .putCustom(
                SnapshotsInProgress.TYPE,
                SnapshotsInProgress.get(state).createCopyWithUpdatedEntriesForRepo(repository, Arrays.asList(entries))
            )
            .build();
    }

    static ClusterState stateWithSnapshots(String repository, SnapshotsInProgress.Entry... entries) {
        return stateWithSnapshots(
            ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes(uuid())).build(),
            repository,
            entries
        );
    }

    static ClusterState stateWithUnassignedIndices(String... indexNames) {
        final Metadata.Builder metaBuilder = Metadata.builder(Metadata.EMPTY_METADATA);
        for (String index : indexNames) {
            metaBuilder.put(
                IndexMetadata.builder(index)
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );
        }
        final RoutingTable.Builder routingTable = RoutingTable.builder();
        for (String index : indexNames) {
            final Index idx = metaBuilder.get(index).getIndex();
            final ShardId shardId = new ShardId(idx, 0);
            routingTable.add(
                IndexRoutingTable.builder(idx)
                    .addIndexShard(
                        IndexShardRoutingTable.builder(shardId)
                            .addShard(
                                ShardRouting.newUnassigned(
                                    shardId,
                                    true,
                                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
                                    ShardRouting.Role.DEFAULT
                                )
                            )
                    )
            );
        }
        return ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metaBuilder).routingTable(routingTable.build()).build();
    }

    static DiscoveryNodes discoveryNodes(String localNodeId) {
        return DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder(localNodeId).roles(new HashSet<>(DiscoveryNodeRole.roles())).build())
            .localNodeId(localNodeId)
            .build();
    }

    private static long randomNonNegativeLong() {
        long l = random().nextLong();
        return l == Long.MIN_VALUE ? 0 : Math.abs(l);
    }
}

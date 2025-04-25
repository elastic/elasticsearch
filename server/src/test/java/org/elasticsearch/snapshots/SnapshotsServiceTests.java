/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

public class SnapshotsServiceTests extends ESTestCase {

    public void testNoopShardStateUpdates() throws Exception {
        final String repoName = "test-repo";
        final Snapshot snapshot = snapshot(repoName, "snapshot-1");
        final SnapshotsInProgress.Entry snapshotNoShards = snapshotEntry(snapshot, Collections.emptyMap(), Collections.emptyMap());

        final String indexName1 = "index-1";
        final ShardId shardId1 = new ShardId(index(indexName1), 0);
        {
            final ClusterState state = stateWithSnapshots(repoName, snapshotNoShards);
            final SnapshotsService.ShardSnapshotUpdate shardCompletion = successUpdate(snapshot, shardId1, uuid());
            assertIsNoop(state, shardCompletion);
        }
        {
            final IndexId indexId = indexId(indexName1);
            final ClusterState state = stateWithSnapshots(
                repoName,
                snapshotEntry(snapshot, Map.of(indexId.getName(), indexId), Map.of(shardId1, initShardStatus(uuid())))
            );
            final SnapshotsService.ShardSnapshotUpdate shardCompletion = successUpdate(
                snapshot("other-repo", snapshot.getSnapshotId().getName()),
                shardId1,
                uuid()
            );
            assertIsNoop(state, shardCompletion);
        }
    }

    public void testUpdateSnapshotToSuccess() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sn1 = snapshot(repoName, "snapshot-1");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final ShardId shardId1 = new ShardId(index(indexName1), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(
            sn1,
            Collections.singletonMap(indexId1.getName(), indexId1),
            Map.of(shardId1, initShardStatus(dataNodeId))
        );

        assertThat(snapshotSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(sn1, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(repoName, snapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.forRepo(repoName).get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.SUCCESS));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateSnapshotMultipleShards() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sn1 = snapshot(repoName, "snapshot-1");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final Index routingIndex1 = index(indexName1);
        final ShardId shardId1 = new ShardId(routingIndex1, 0);
        final ShardId shardId2 = new ShardId(routingIndex1, 1);
        final SnapshotsInProgress.ShardSnapshotStatus shardInitStatus = initShardStatus(dataNodeId);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(
            sn1,
            Collections.singletonMap(indexId1.getName(), indexId1),
            Map.of(shardId1, shardInitStatus, shardId2, shardInitStatus)
        );

        assertThat(snapshotSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(sn1, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(repoName, snapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.forRepo(repoName).get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.STARTED));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateCloneToSuccess() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final SnapshotsInProgress.Entry cloneSingleShard = cloneEntry(
            targetSnapshot,
            sourceSnapshot.getSnapshotId(),
            Map.of(shardId1, initShardStatus(dataNodeId))
        );

        assertThat(cloneSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(targetSnapshot, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(repoName, cloneSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.forRepo(repoName).get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.SUCCESS));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateCloneMultipleShards() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final RepositoryShardId shardId2 = new RepositoryShardId(indexId1, 1);
        final SnapshotsInProgress.ShardSnapshotStatus shardInitStatus = initShardStatus(dataNodeId);
        final SnapshotsInProgress.Entry cloneMultipleShards = cloneEntry(
            targetSnapshot,
            sourceSnapshot.getSnapshotId(),
            Map.of(shardId1, shardInitStatus, shardId2, shardInitStatus)
        );

        assertThat(cloneMultipleShards.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(targetSnapshot, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(repoName, cloneMultipleShards), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.forRepo(repoName).get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.STARTED));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testCompletedCloneStartsSnapshot() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final SnapshotsInProgress.ShardSnapshotStatus shardInitStatus = initShardStatus(dataNodeId);
        final SnapshotsInProgress.Entry cloneSingleShard = cloneEntry(
            targetSnapshot,
            sourceSnapshot.getSnapshotId(),
            Map.of(shardId1, shardInitStatus)
        );

        final ClusterState stateWithIndex = stateWithUnassignedIndices(indexName1);
        final Snapshot plainSnapshot = snapshot(repoName, "test-snapshot");
        final ShardId routingShardId1 = new ShardId(stateWithIndex.metadata().getProject().index(indexName1).getIndex(), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(
            plainSnapshot,
            Collections.singletonMap(indexId1.getName(), indexId1),
            Map.of(routingShardId1, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );

        assertThat(cloneSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        // 1. case: shard that just finished cloning is unassigned -> shard snapshot should go to MISSING state
        final ClusterState stateWithUnassignedRoutingShard = stateWithSnapshots(
            stateWithIndex,
            repoName,
            cloneSingleShard,
            snapshotSingleShard
        );
        final SnapshotsService.ShardSnapshotUpdate completeShardClone = successUpdate(targetSnapshot, shardId1, uuid());
        {
            final ClusterState updatedClusterState = applyUpdates(stateWithUnassignedRoutingShard, completeShardClone);
            final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
            final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.forRepo(repoName).get(0);
            assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
            final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.forRepo(repoName).get(1);
            assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.SUCCESS));
            assertThat(startedSnapshot.shards().get(routingShardId1).state(), is(SnapshotsInProgress.ShardState.MISSING));
            assertIsNoop(updatedClusterState, completeShardClone);
        }

        // 2. case: shard that just finished cloning is assigned correctly -> shard snapshot should go to INIT state
        final ClusterState stateWithAssignedRoutingShard = ClusterState.builder(stateWithUnassignedRoutingShard)
            .routingTable(
                RoutingTable.builder(stateWithUnassignedRoutingShard.routingTable())
                    .add(
                        IndexRoutingTable.builder(routingShardId1.getIndex())
                            .addIndexShard(
                                IndexShardRoutingTable.builder(routingShardId1)
                                    .addShard(
                                        TestShardRouting.newShardRouting(routingShardId1, dataNodeId, true, ShardRoutingState.STARTED)
                                    )
                            )
                    )
                    .build()
            )
            .build();
        {
            final ClusterState updatedClusterState = applyUpdates(stateWithAssignedRoutingShard, completeShardClone);
            final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
            final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.forRepo(repoName).get(0);
            assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
            final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.forRepo(repoName).get(1);
            assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
            final SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus = startedSnapshot.shards().get(routingShardId1);
            assertThat(shardSnapshotStatus.state(), is(SnapshotsInProgress.ShardState.INIT));
            assertThat(shardSnapshotStatus.nodeId(), is(dataNodeId));
            assertIsNoop(updatedClusterState, completeShardClone);
        }

        // 3. case: shard that just finished cloning is currently initializing -> shard snapshot should go to WAITING state
        final ClusterState stateWithInitializingRoutingShard = ClusterState.builder(stateWithUnassignedRoutingShard)
            .routingTable(
                RoutingTable.builder(stateWithUnassignedRoutingShard.routingTable())
                    .add(
                        IndexRoutingTable.builder(routingShardId1.getIndex())
                            .addIndexShard(
                                IndexShardRoutingTable.builder(routingShardId1)
                                    .addShard(
                                        TestShardRouting.newShardRouting(routingShardId1, dataNodeId, true, ShardRoutingState.INITIALIZING)
                                    )
                            )
                    )
                    .build()
            )
            .build();
        {
            final ClusterState updatedClusterState = applyUpdates(stateWithInitializingRoutingShard, completeShardClone);
            final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
            final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.forRepo(repoName).get(0);
            assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
            final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.forRepo(repoName).get(1);
            assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
            assertThat(startedSnapshot.shards().get(routingShardId1).state(), is(SnapshotsInProgress.ShardState.WAITING));
            assertIsNoop(updatedClusterState, completeShardClone);
        }
    }

    public void testCompletedSnapshotStartsClone() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName);
        final RepositoryShardId repositoryShardId = new RepositoryShardId(indexId1, 0);
        final SnapshotsInProgress.Entry cloneSingleShard = cloneEntry(
            targetSnapshot,
            sourceSnapshot.getSnapshotId(),
            Map.of(repositoryShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );

        final ClusterState stateWithIndex = stateWithUnassignedIndices(indexName);
        final Snapshot plainSnapshot = snapshot(repoName, "test-snapshot");
        final ShardId routingShardId = new ShardId(stateWithIndex.metadata().getProject().index(indexName).getIndex(), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(
            plainSnapshot,
            Collections.singletonMap(indexId1.getName(), indexId1),
            Map.of(routingShardId, initShardStatus(dataNodeId))
        );

        assertThat(cloneSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(plainSnapshot, routingShardId, dataNodeId);

        final ClusterState updatedClusterState = applyUpdates(
            stateWithSnapshots(repoName, snapshotSingleShard, cloneSingleShard),
            completeShard
        );
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.forRepo(repoName).get(0);
        assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
        final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.forRepo(repoName).get(1);
        assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
        final SnapshotsInProgress.ShardSnapshotStatus shardCloneStatus = startedSnapshot.shardSnapshotStatusByRepoShardId()
            .get(repositoryShardId);
        assertThat(shardCloneStatus.state(), is(SnapshotsInProgress.ShardState.INIT));
        assertThat(shardCloneStatus.nodeId(), is(updatedClusterState.nodes().getLocalNodeId()));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testCompletedSnapshotStartsNextSnapshot() throws Exception {
        final String repoName = "test-repo";
        final String indexName = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName);

        final ClusterState stateWithIndex = stateWithUnassignedIndices(indexName);
        final Snapshot plainSnapshot = snapshot(repoName, "test-snapshot-1");
        final ShardId routingShardId = new ShardId(stateWithIndex.metadata().getProject().index(indexName).getIndex(), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(
            plainSnapshot,
            Collections.singletonMap(indexId1.getName(), indexId1),
            Map.of(routingShardId, initShardStatus(dataNodeId))
        );

        final Snapshot queuedSnapshot = snapshot(repoName, "test-snapshot-2");
        final SnapshotsInProgress.Entry queuedSnapshotSingleShard = snapshotEntry(
            queuedSnapshot,
            Collections.singletonMap(indexId1.getName(), indexId1),
            Map.of(routingShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(plainSnapshot, routingShardId, dataNodeId);

        final ClusterState updatedClusterState = applyUpdates(
            stateWithSnapshots(repoName, snapshotSingleShard, queuedSnapshotSingleShard),
            completeShard
        );
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry completedSnapshot = snapshotsInProgress.forRepo(repoName).get(0);
        assertThat(completedSnapshot.state(), is(SnapshotsInProgress.State.SUCCESS));
        final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.forRepo(repoName).get(1);
        assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.SUCCESS));
        final SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus = startedSnapshot.shards().get(routingShardId);
        assertThat(shardSnapshotStatus.state(), is(SnapshotsInProgress.ShardState.MISSING));
        assertNull(shardSnapshotStatus.nodeId());
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testCompletedCloneStartsNextClone() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final String masterNodeId = uuid();
        final SnapshotsInProgress.Entry cloneSingleShard = cloneEntry(
            targetSnapshot,
            sourceSnapshot.getSnapshotId(),
            Map.of(shardId1, initShardStatus(masterNodeId))
        );

        final Snapshot queuedTargetSnapshot = snapshot(repoName, "test-snapshot");
        final SnapshotsInProgress.Entry queuedClone = cloneEntry(
            queuedTargetSnapshot,
            sourceSnapshot.getSnapshotId(),
            Map.of(shardId1, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );

        assertThat(cloneSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final ClusterState stateWithUnassignedRoutingShard = stateWithSnapshots(
            ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes(masterNodeId)).build(),
            repoName,
            cloneSingleShard,
            queuedClone
        );
        final SnapshotsService.ShardSnapshotUpdate completeShardClone = successUpdate(targetSnapshot, shardId1, masterNodeId);

        final ClusterState updatedClusterState = applyUpdates(stateWithUnassignedRoutingShard, completeShardClone);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.forRepo(repoName).get(0);
        assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
        final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.forRepo(repoName).get(1);
        assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
        assertThat(startedSnapshot.shardSnapshotStatusByRepoShardId().get(shardId1).state(), is(SnapshotsInProgress.ShardState.INIT));
        assertIsNoop(updatedClusterState, completeShardClone);
    }

    public void testPauseForNodeRemovalWithQueuedShards() throws Exception {
        final var repoName = "test-repo";
        final var snapshot1 = snapshot(repoName, "snap-1");
        final var snapshot2 = snapshot(repoName, "snap-2");
        final var indexName = "index-1";
        final var shardId = new ShardId(index(indexName), 0);
        final var repositoryShardId = new RepositoryShardId(indexId(indexName), 0);
        final var nodeId = uuid();

        final var runningEntry = snapshotEntry(
            snapshot1,
            Collections.singletonMap(indexName, repositoryShardId.index()),
            Map.of(shardId, initShardStatus(nodeId))
        );

        final var queuedEntry = snapshotEntry(
            snapshot2,
            Collections.singletonMap(indexName, repositoryShardId.index()),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );

        final var initialState = stateWithSnapshots(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId)).localNodeId(nodeId).masterNodeId(nodeId).build())
                .routingTable(
                    RoutingTable.builder()
                        .add(
                            IndexRoutingTable.builder(shardId.getIndex())
                                .addShard(TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.STARTED))
                        )
                        .build()
                )
                .build(),
            repoName,
            runningEntry,
            queuedEntry
        );

        final var updatedState = applyUpdates(
            initialState,
            new SnapshotsService.ShardSnapshotUpdate(
                snapshot1,
                shardId,
                null,
                new SnapshotsInProgress.ShardSnapshotStatus(
                    nodeId,
                    SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL,
                    runningEntry.shards().get(shardId).generation()
                ),
                ActionTestUtils.assertNoFailureListener(t -> {})
            )
        );

        assertEquals(
            SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL,
            SnapshotsInProgress.get(updatedState).snapshot(snapshot1).shards().get(shardId).state()
        );

        assertEquals(
            SnapshotsInProgress.ShardState.QUEUED,
            SnapshotsInProgress.get(updatedState).snapshot(snapshot2).shards().get(shardId).state()
        );
    }

    private SnapshotsInProgress.ShardSnapshotStatus successShardSnapshotStatus(
        String nodeId,
        ShardId shardId,
        SnapshotsInProgress.Entry entry
    ) {
        return SnapshotsInProgress.ShardSnapshotStatus.success(
            nodeId,
            new ShardSnapshotResult(entry.shards().get(shardId).generation(), ByteSizeValue.ofBytes(1L), 1)
        );
    }

    private SnapshotsInProgress.ShardSnapshotStatus failedShardSnapshotStatus(
        String nodeId,
        ShardId shardId,
        SnapshotsInProgress.Entry entry
    ) {
        return new SnapshotsInProgress.ShardSnapshotStatus(
            nodeId,
            SnapshotsInProgress.ShardState.FAILED,
            entry.shards().get(shardId).generation(),
            "test injected failure"
        );
    }

    private SnapshotsInProgress.ShardSnapshotStatus pausedShardSnapshotStatus(
        String nodeId,
        ShardId shardId,
        SnapshotsInProgress.Entry entry
    ) {
        return new SnapshotsInProgress.ShardSnapshotStatus(
            nodeId,
            SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL,
            entry.shards().get(shardId).generation()
        );
    }

    /**
     * Tests that, within the same cluster state batched update execution, a shard snapshot status update of PAUSED_FOR_NODE_REMOVAL will be
     * ignored after the same shard snapshot has already been updated to a completed state. On the other hand, a PAUSED_FOR_NODE_REMOVAL
     * update follow by a SUCCESS, or other completed state, update should be applied and result in SUCCESS.
     */
    public void testBatchedShardSnapshotUpdatesCannotApplyPausedAfterCompleted() throws Exception {
        final var repoName = "test-repo-name";
        final var snapshot1 = snapshot(repoName, "test-snap-1");
        final var snapshot2 = snapshot(repoName, "test-snap-2");
        final var indexName = "test-index-name";
        final var shardId = new ShardId(index(indexName), 0);
        final var repositoryShardId = new RepositoryShardId(indexId(indexName), 0);
        final var originalNodeId = uuid();
        final var otherNodeId = uuid();

        final SnapshotsInProgress.Entry runningSnapshotEntry = snapshotEntry(
            snapshot1,
            Collections.singletonMap(indexName, repositoryShardId.index()),
            Map.of(shardId, initShardStatus(originalNodeId))
        );

        final SnapshotsInProgress.Entry queuedSnapshotEntry = snapshotEntry(
            snapshot2,
            Collections.singletonMap(indexName, repositoryShardId.index()),
            Map.of(shardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
        );

        final ClusterState initialState = stateWithSnapshots(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(DiscoveryNodeUtils.create(originalNodeId))
                        .localNodeId(originalNodeId)
                        .masterNodeId(originalNodeId)
                        .build()
                )
                .routingTable(
                    RoutingTable.builder()
                        .add(
                            IndexRoutingTable.builder(shardId.getIndex())
                                .addShard(TestShardRouting.newShardRouting(shardId, originalNodeId, true, ShardRoutingState.STARTED))
                        )
                        .build()
                )
                .build(),
            repoName,
            runningSnapshotEntry,
            queuedSnapshotEntry
        );

        assertEquals(
            SnapshotsInProgress.ShardState.QUEUED,
            SnapshotsInProgress.get(initialState).snapshot(snapshot2).shards().get(shardId).state()
        );

        /**
         * In this scenario, {@link originalNodeId} is the original shard owner that resends PAUSED, and {@link otherNodeId} is the new
         * shard owner that completes the shard snapshot. The production code doesn't verify node ownership, but it's helpful for the test.
         */

        // Ultimately ignored statuses.
        var pausedOnOriginalNodeStatus = pausedShardSnapshotStatus(originalNodeId, shardId, runningSnapshotEntry);
        var successfulOnOriginalNodeStatus = successShardSnapshotStatus(originalNodeId, shardId, runningSnapshotEntry);

        // Ultimately applied statuses.
        var successfulOnOtherNodeStatus = successShardSnapshotStatus(otherNodeId, shardId, runningSnapshotEntry);
        var failedOnOtherNodeStatus = failedShardSnapshotStatus(otherNodeId, shardId, runningSnapshotEntry);

        var completedUpdateOnOtherNode = new SnapshotsService.ShardSnapshotUpdate(
            snapshot1,
            shardId,
            null,
            // Success and failure are both completed shard snapshot states, so paused should be ignored when either is set.
            randomBoolean() ? successfulOnOtherNodeStatus : failedOnOtherNodeStatus,
            ActionTestUtils.assertNoFailureListener(t -> {})
        );
        var pausedUpdateOnOriginalNode = new SnapshotsService.ShardSnapshotUpdate(
            snapshot1,
            shardId,
            null,
            pausedOnOriginalNodeStatus,
            ActionTestUtils.assertNoFailureListener(t -> {})
        );
        var completedUpdateOnOriginalNode = new SnapshotsService.ShardSnapshotUpdate(
            snapshot1,
            shardId,
            null,
            successfulOnOriginalNodeStatus,
            ActionTestUtils.assertNoFailureListener(t -> {})
        );

        boolean random = randomBoolean();
        ClusterState updatedState;
        if (randomBoolean()) {
            updatedState = applyUpdates(
                initialState,
                // Randomize the order of completed and paused updates but make sure that there's one of each. If the paused update comes
                // after the completed update, paused should be ignored and the shard snapshot remains in a completed state.
                random ? completedUpdateOnOtherNode : pausedUpdateOnOriginalNode,
                random ? pausedUpdateOnOriginalNode : completedUpdateOnOtherNode
            );
        } else {
            updatedState = applyUpdates(
                initialState,
                random ? completedUpdateOnOtherNode : pausedUpdateOnOriginalNode,
                random ? pausedUpdateOnOriginalNode : completedUpdateOnOtherNode,
                // Randomly add another update that will be ignored because the shard snapshot is complete.
                // Note: the originalNodeId is used for this update, so we can verify afterward that the update is not applied.
                randomBoolean() ? completedUpdateOnOriginalNode : pausedUpdateOnOriginalNode
            );
        }

        assertTrue(SnapshotsInProgress.get(updatedState).snapshot(snapshot1).shards().get(shardId).state().completed());
        assertEquals(otherNodeId, SnapshotsInProgress.get(updatedState).snapshot(snapshot1).shards().get(shardId).nodeId());

        // Since the first snapshot completed, the second snapshot should be set to proceed with snapshotting the same shard.
        assertEquals(
            SnapshotsInProgress.ShardState.INIT,
            SnapshotsInProgress.get(updatedState).snapshot(snapshot2).shards().get(shardId).state()
        );
    }

    public void testSnapshottingIndicesExcludesClones() {
        final String repoName = "test-repo";
        final String indexName = "index";
        final ClusterState clusterState = stateWithSnapshots(
            stateWithUnassignedIndices(indexName),
            repoName,
            cloneEntry(
                snapshot(repoName, "target-snapshot"),
                snapshot(repoName, "source-snapshot").getSnapshotId(),
                Map.of(new RepositoryShardId(indexId(indexName), 0), initShardStatus(uuid()))
            )
        );

        assertThat(
            SnapshotsService.snapshottingIndices(
                clusterState.projectState(),
                singleton(clusterState.metadata().getProject().index(indexName).getIndex())
            ),
            empty()
        );
    }

    private static DiscoveryNodes discoveryNodes(String localNodeId) {
        return DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder(localNodeId).roles(new HashSet<>(DiscoveryNodeRole.roles())).build())
            .localNodeId(localNodeId)
            .build();
    }

    private static SnapshotsService.ShardSnapshotUpdate successUpdate(Snapshot snapshot, ShardId shardId, String nodeId) {
        return new SnapshotsService.ShardSnapshotUpdate(
            snapshot,
            shardId,
            null,
            successfulShardStatus(nodeId),
            ActionTestUtils.assertNoFailureListener(t -> {})
        );
    }

    private static SnapshotsService.ShardSnapshotUpdate successUpdate(Snapshot snapshot, RepositoryShardId shardId, String nodeId) {
        return new SnapshotsService.ShardSnapshotUpdate(
            snapshot,
            null,
            shardId,
            successfulShardStatus(nodeId),
            ActionTestUtils.assertNoFailureListener(t -> {})
        );
    }

    private static ClusterState stateWithUnassignedIndices(String... indexNames) {
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

    private static ClusterState stateWithSnapshots(ClusterState state, String repository, SnapshotsInProgress.Entry... entries) {
        return ClusterState.builder(state)
            .version(state.version() + 1L)
            .putCustom(
                SnapshotsInProgress.TYPE,
                SnapshotsInProgress.EMPTY.createCopyWithUpdatedEntriesForRepo(repository, Arrays.asList(entries))
            )
            .build();
    }

    private static ClusterState stateWithSnapshots(String repository, SnapshotsInProgress.Entry... entries) {
        return stateWithSnapshots(
            ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes(uuid())).build(),
            repository,
            entries
        );
    }

    private static void assertIsNoop(ClusterState state, SnapshotsService.SnapshotTask shardCompletion) throws Exception {
        assertSame(applyUpdates(state, shardCompletion), state);
    }

    /**
     * Runs the shard snapshot updates through a ClusterStateTaskExecutor that executes the
     * {@link SnapshotsService.SnapshotShardsUpdateContext}.
     *
     * @param state Original cluster state
     * @param updates List of SnapshotTask tasks to apply to the cluster state
     * @return An updated cluster state, or, if no change were made, the original given cluster state.
     */
    private static ClusterState applyUpdates(ClusterState state, SnapshotsService.SnapshotTask... updates) throws Exception {
        return ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(state, batchExecutionContext -> {
            final SnapshotsInProgress existing = SnapshotsInProgress.get(batchExecutionContext.initialState());
            final var context = new SnapshotsService.SnapshotShardsUpdateContext(
                batchExecutionContext,
                /* on completion handler */ (shardSnapshotUpdateResult, newlyCompletedEntries, updatedRepositories) -> {}
            );
            final SnapshotsInProgress updated = context.computeUpdatedState();
            context.setupSuccessfulPublicationCallbacks(updated);
            if (existing == updated) {
                return batchExecutionContext.initialState();
            }
            return ClusterState.builder(batchExecutionContext.initialState()).putCustom(SnapshotsInProgress.TYPE, updated).build();
        }, Arrays.asList(updates));
    }

    private static SnapshotsInProgress.Entry snapshotEntry(
        Snapshot snapshot,
        Map<String, IndexId> indexIds,
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards
    ) {
        return SnapshotsInProgress.startedEntry(
            snapshot,
            randomBoolean(),
            randomBoolean(),
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

    private static SnapshotsInProgress.Entry cloneEntry(
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

    /**
     * Helper method to create a shard snapshot status with state {@link SnapshotsInProgress.ShardState#INIT}.
     */
    private static SnapshotsInProgress.ShardSnapshotStatus initShardStatus(String nodeId) {
        return new SnapshotsInProgress.ShardSnapshotStatus(nodeId, ShardGeneration.newGeneration(random()));
    }

    private static SnapshotsInProgress.ShardSnapshotStatus successfulShardStatus(String nodeId) {
        return SnapshotsInProgress.ShardSnapshotStatus.success(
            nodeId,
            new ShardSnapshotResult(ShardGeneration.newGeneration(random()), ByteSizeValue.ofBytes(1L), 1)
        );
    }

    private static Snapshot snapshot(String repoName, String name) {
        return new Snapshot(repoName, new SnapshotId(name, uuid()));
    }

    private static Index index(String name) {
        return new Index(name, uuid());
    }

    private static IndexId indexId(String name) {
        return new IndexId(name, uuid());
    }

    private static String uuid() {
        return UUIDs.randomBase64UUID(random());
    }
}

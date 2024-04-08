/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        final ShardId routingShardId1 = new ShardId(stateWithIndex.metadata().index(indexName1).getIndex(), 0);
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
        final ShardId routingShardId = new ShardId(stateWithIndex.metadata().index(indexName).getIndex(), 0);
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
        final SnapshotsInProgress.ShardSnapshotStatus shardCloneStatus = startedSnapshot.shardsByRepoShardId().get(repositoryShardId);
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
        final ShardId routingShardId = new ShardId(stateWithIndex.metadata().index(indexName).getIndex(), 0);
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
        assertThat(startedSnapshot.shardsByRepoShardId().get(shardId1).state(), is(SnapshotsInProgress.ShardState.INIT));
        assertIsNoop(updatedClusterState, completeShardClone);
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
            SnapshotsService.snapshottingIndices(clusterState, singleton(clusterState.metadata().index(indexName).getIndex())),
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
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withUpdatedEntriesForRepo(repository, Arrays.asList(entries)))
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

    private static ClusterState applyUpdates(ClusterState state, SnapshotsService.SnapshotTask... updates) throws Exception {
        return ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(state, batchExecutionContext -> {
            final SnapshotsInProgress existing = SnapshotsInProgress.get(batchExecutionContext.initialState());
            final var context = new SnapshotsService.SnapshotShardsUpdateContext(batchExecutionContext, (a, b, c) -> {});
            final SnapshotsInProgress updated = context.computeUpdatedState();
            context.completeWithUpdatedState(updated);
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

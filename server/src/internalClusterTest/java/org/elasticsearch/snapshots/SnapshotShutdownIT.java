/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.oneOf;

public class SnapshotShutdownIT extends AbstractSnapshotIntegTestCase {

    private static final String REQUIRE_NODE_NAME_SETTING = IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testRestartNodeDuringSnapshot() throws Exception {
        // Marking a node for restart has no impact on snapshots (see #71333 for how to handle this case)
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, originalNode).build());

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, originalNode);
        final var snapshotCompletesWithoutPausingListener = ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> {
            final var entriesForRepo = SnapshotsInProgress.get(state).forRepo(repoName);
            if (entriesForRepo.isEmpty()) {
                return true;
            }
            assertThat(entriesForRepo, hasSize(1));
            final var shardSnapshotStatuses = entriesForRepo.iterator().next().shards().values();
            assertThat(shardSnapshotStatuses, hasSize(1));
            assertThat(
                shardSnapshotStatuses.iterator().next().state(),
                oneOf(SnapshotsInProgress.ShardState.INIT, SnapshotsInProgress.ShardState.SUCCESS)
            );
            return false;
        });
        addUnassignedShardsWatcher(clusterService, indexName);

        safeAwait(
            (ActionListener<Void> listener) -> putShutdownMetadata(
                clusterService,
                SingleNodeShutdownMetadata.builder()
                    .setType(SingleNodeShutdownMetadata.Type.RESTART)
                    .setStartedAtMillis(clusterService.threadPool().absoluteTimeInMillis())
                    .setReason("test"),
                originalNode,
                listener
            )
        );
        assertFalse(snapshotCompletesWithoutPausingListener.isDone());
        unblockAllDataNodes(repoName); // lets the shard snapshot continue so the snapshot can succeed
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());
        safeAwait(snapshotCompletesWithoutPausingListener);
        clearShutdownMetadata(clusterService);
    }

    public void testRemoveNodeDuringSnapshot() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, originalNode).build());

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, originalNode);
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName);
        addUnassignedShardsWatcher(clusterService, indexName);

        updateIndexSettings(Settings.builder().putNull(REQUIRE_NODE_NAME_SETTING), indexName);
        putShutdownForRemovalMetadata(originalNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, which frees up the shard so it can move
        safeAwait(snapshotPausedListener);

        // snapshot completes when the node vacates even though it hasn't been removed yet
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());

        if (randomBoolean()) {
            internalCluster().stopNode(originalNode);
        }

        clearShutdownMetadata(clusterService);
    }

    public void testRemoveNodeAndFailoverMasterDuringSnapshot() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, originalNode).build());

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, originalNode);
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName);
        addUnassignedShardsWatcher(clusterService, indexName);

        final var snapshotStatusUpdateBarrier = new CyclicBarrier(2);
        final var masterName = internalCluster().getMasterName();
        final var masterTransportService = MockTransportService.getInstance(masterName);
        masterTransportService.addRequestHandlingBehavior(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            (handler, request, channel, task) -> masterTransportService.getThreadPool().generic().execute(() -> {
                safeAwait(snapshotStatusUpdateBarrier);
                safeAwait(snapshotStatusUpdateBarrier);
                try {
                    handler.messageReceived(request, channel, task);
                } catch (Exception e) {
                    fail(e);
                }
            })
        );

        updateIndexSettings(Settings.builder().putNull(REQUIRE_NODE_NAME_SETTING), indexName);
        putShutdownForRemovalMetadata(originalNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, which frees up the shard so it can move
        safeAwait(snapshotStatusUpdateBarrier); // wait for the data node to finish and then try and update the master
        masterTransportService.clearAllRules(); // the shard might migrate to the old master, so let it process more updates

        if (internalCluster().numMasterNodes() == 1) {
            internalCluster().startMasterOnlyNode();
        }
        safeAwait(
            SubscribableListener.<ActionResponse.Empty>newForked(
                l -> client().execute(
                    TransportAddVotingConfigExclusionsAction.TYPE,
                    new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY, new String[] { masterName }, TimeValue.timeValueSeconds(10)),
                    l
                )
            )
        );
        safeAwait(
            ClusterServiceUtils.addTemporaryStateListener(
                clusterService,
                s -> s.nodes().getMasterNode() != null && s.nodes().getMasterNode().getName().equals(masterName) == false
            )
        );

        logger.info("--> new master elected, releasing blocked request");
        safeAwait(snapshotStatusUpdateBarrier); // let the old master try and update the state
        logger.info("--> waiting for snapshot pause");
        safeAwait(snapshotPausedListener);
        logger.info("--> snapshot was paused");

        // snapshot API fails on master failover
        assertThat(
            asInstanceOf(
                SnapshotException.class,
                ExceptionsHelper.unwrapCause(
                    expectThrows(ExecutionException.class, RuntimeException.class, () -> snapshotFuture.get(10, TimeUnit.SECONDS))
                )
            ).getMessage(),
            containsString("no longer master")
        );

        // but the snapshot itself completes
        safeAwait(ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> SnapshotsInProgress.get(state).isEmpty()));

        // flush master queue to ensure the completion is applied everywhere
        safeAwait(
            SubscribableListener.<ClusterHealthResponse>newForked(
                l -> client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).execute(l)
            )
        );

        // and succeeds
        final var snapshots = safeAwait(
            SubscribableListener.<GetSnapshotsResponse>newForked(
                l -> client().admin().cluster().getSnapshots(new GetSnapshotsRequest(TEST_REQUEST_TIMEOUT, repoName), l)
            )
        ).getSnapshots();
        assertThat(snapshots, hasSize(1));
        assertEquals(SnapshotState.SUCCESS, snapshots.get(0).state());

        if (randomBoolean()) {
            internalCluster().stopNode(originalNode);
        }

        safeAwait(SubscribableListener.<ActionResponse.Empty>newForked(l -> {
            final var clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest();
            clearVotingConfigExclusionsRequest.setWaitForRemoval(false);
            client().execute(TransportClearVotingConfigExclusionsAction.TYPE, clearVotingConfigExclusionsRequest, l);
        }));

        clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
    }

    public void testRemoveNodeDuringSnapshotWithOtherRunningShardSnapshots() throws Exception {
        // SnapshotInProgressAllocationDecider only considers snapshots having shards in INIT state, so a single-shard snapshot such as the
        // one in testRemoveNodeDuringSnapshot will be ignored when the shard is paused, permitting the shard movement. This test verifies
        // that the shard is permitted to move even when the snapshot has other shards in INIT state.

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        // create another index on another node which will be blocked (remain in state INIT) throughout
        final var otherNode = internalCluster().startDataOnlyNode();
        final var otherIndex = randomIdentifier();
        createIndexWithContent(otherIndex, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, otherNode).build());
        blockDataNode(repoName, otherNode);

        final var nodeForRemoval = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, nodeForRemoval).build());

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, nodeForRemoval);
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName);
        addUnassignedShardsWatcher(clusterService, indexName);

        waitForBlock(otherNode, repoName);

        putShutdownForRemovalMetadata(nodeForRemoval, clusterService);
        unblockNode(repoName, nodeForRemoval); // lets the shard snapshot abort, which frees up the shard to move
        safeAwait(snapshotPausedListener);

        // adjust the allocation filter so that the shard moves
        updateIndexSettings(Settings.builder().putNull(REQUIRE_NODE_NAME_SETTING), indexName);

        // wait for the target shard snapshot to succeed
        safeAwait(
            ClusterServiceUtils.addTemporaryStateListener(
                clusterService,
                state -> SnapshotsInProgress.get(state)
                    .asStream()
                    .allMatch(
                        e -> e.shards()
                            .entrySet()
                            .stream()
                            .anyMatch(
                                shardEntry -> shardEntry.getKey().getIndexName().equals(indexName)
                                    && switch (shardEntry.getValue().state()) {
                                        case INIT, PAUSED_FOR_NODE_REMOVAL -> false;
                                        case SUCCESS -> true;
                                        case FAILED, ABORTED, MISSING, QUEUED, WAITING -> throw new AssertionError(shardEntry.toString());
                                    }
                            )
                    )
            )
        );

        unblockAllDataNodes(repoName);

        // snapshot completes when the node vacates even though it hasn't been removed yet
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());

        if (randomBoolean()) {
            internalCluster().stopNode(nodeForRemoval);
        }

        clearShutdownMetadata(clusterService);
    }

    public void testStartRemoveNodeButDoNotComplete() throws Exception {
        final var primaryNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, primaryNode).build());

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, primaryNode);
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName);
        addUnassignedShardsWatcher(clusterService, indexName);

        putShutdownForRemovalMetadata(primaryNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, but allocation filtering stops it from moving
        safeAwait(snapshotPausedListener);
        assertFalse(snapshotFuture.isDone());

        // give up on the node shutdown so the shard snapshot can restart
        clearShutdownMetadata(clusterService);

        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());
    }

    public void testAbortSnapshotWhileRemovingNode() throws Exception {
        final var primaryNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, primaryNode).build());

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var snapshotName = randomIdentifier();
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(snapshotName, repoName, primaryNode);

        final var updateSnapshotStatusBarrier = new CyclicBarrier(2);
        final var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        masterTransportService.addRequestHandlingBehavior(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            (handler, request, channel, task) -> masterTransportService.getThreadPool().generic().execute(() -> {
                safeAwait(updateSnapshotStatusBarrier);
                safeAwait(updateSnapshotStatusBarrier);
                try {
                    handler.messageReceived(request, channel, task);
                } catch (Exception e) {
                    fail(e);
                }
            })
        );

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        addUnassignedShardsWatcher(clusterService, indexName);
        putShutdownForRemovalMetadata(primaryNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, but allocation filtering stops it from moving
        safeAwait(updateSnapshotStatusBarrier); // wait for data node to notify master that the shard snapshot is paused

        // abort snapshot (and wait for the abort to land in the cluster state)
        final var deleteStartedListener = ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> {
            if (SnapshotDeletionsInProgress.get(state).getEntries().isEmpty()) {
                return false;
            }

            assertEquals(SnapshotsInProgress.State.ABORTED, SnapshotsInProgress.get(state).forRepo(repoName).get(0).state());
            return true;
        });

        final var deleteSnapshotFuture = startDeleteSnapshot(repoName, snapshotName); // abort the snapshot
        safeAwait(deleteStartedListener);

        safeAwait(updateSnapshotStatusBarrier); // process pause notification now that the snapshot is ABORTED

        assertEquals(SnapshotState.FAILED, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());
        assertTrue(deleteSnapshotFuture.get(10, TimeUnit.SECONDS).isAcknowledged());

        clearShutdownMetadata(clusterService);
    }

    public void testShutdownWhileSuccessInFlight() throws Exception {
        final var primaryNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, primaryNode).build());

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        masterTransportService.addRequestHandlingBehavior(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            (handler, request, channel, task) -> putShutdownForRemovalMetadata(
                clusterService,
                primaryNode,
                ActionTestUtils.assertNoFailureListener(ignored -> handler.messageReceived(request, channel, task))
            )
        );

        addUnassignedShardsWatcher(clusterService, indexName);
        assertEquals(
            SnapshotState.SUCCESS,
            startFullSnapshot(repoName, randomIdentifier()).get(10, TimeUnit.SECONDS).getSnapshotInfo().state()
        );
        clearShutdownMetadata(clusterService);
    }

    private static SubscribableListener<Void> createSnapshotPausedListener(
        ClusterService clusterService,
        String repoName,
        String indexName
    ) {
        return ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> {
            final var entriesForRepo = SnapshotsInProgress.get(state).forRepo(repoName);
            if (entriesForRepo.isEmpty()) {
                // it's (just about) possible for the data node to apply the initial snapshot state, start on the first shard snapshot, and
                // hit the IO block, before the master even applies this cluster state, in which case we simply retry:
                return false;
            }
            assertThat(entriesForRepo, hasSize(1));
            final var shardSnapshotStatuses = entriesForRepo.iterator()
                .next()
                .shards()
                .entrySet()
                .stream()
                .flatMap(e -> e.getKey().getIndexName().equals(indexName) ? Stream.of(e.getValue()) : Stream.of())
                .toList();
            assertThat(shardSnapshotStatuses, hasSize(1));
            final var shardState = shardSnapshotStatuses.iterator().next().state();
            assertThat(shardState, oneOf(SnapshotsInProgress.ShardState.INIT, SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL));
            return shardState == SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL;
        });
    }

    private static void addUnassignedShardsWatcher(ClusterService clusterService, String indexName) {
        ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> {
            final var indexRoutingTable = state.routingTable().index(indexName);
            if (indexRoutingTable == null) {
                // index was deleted, can remove this listener now
                return true;
            }
            assertThat(indexRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED), empty());
            return false;
        });
    }

    private static void putShutdownForRemovalMetadata(String nodeName, ClusterService clusterService) {
        safeAwait((ActionListener<Void> listener) -> putShutdownForRemovalMetadata(clusterService, nodeName, listener));
    }

    private static void flushMasterQueue(ClusterService clusterService, ActionListener<Void> listener) {
        clusterService.submitUnbatchedStateUpdateTask("flush queue", new ClusterStateUpdateTask(Priority.LANGUID) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                listener.onResponse(null);
            }
        });
    }

    private static void putShutdownForRemovalMetadata(ClusterService clusterService, String nodeName, ActionListener<Void> listener) {
        // not testing REPLACE just because it requires us to specify the replacement node
        final var shutdownType = randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM);
        final var shutdownMetadata = SingleNodeShutdownMetadata.builder()
            .setType(shutdownType)
            .setStartedAtMillis(clusterService.threadPool().absoluteTimeInMillis())
            .setReason("test");
        switch (shutdownType) {
            case SIGTERM -> shutdownMetadata.setGracePeriod(TimeValue.timeValueSeconds(60));
        }
        SubscribableListener

            .<Void>newForked(l -> putShutdownMetadata(clusterService, shutdownMetadata, nodeName, l))
            .<Void>andThen(l -> flushMasterQueue(clusterService, l))
            .addListener(listener);
    }

    private static void putShutdownMetadata(
        ClusterService clusterService,
        SingleNodeShutdownMetadata.Builder shutdownMetadataBuilder,
        String nodeName,
        ActionListener<Void> listener
    ) {
        clusterService.submitUnbatchedStateUpdateTask("mark node for removal", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final var nodeId = currentState.nodes().resolveNode(nodeName).getId();
                return currentState.copyAndUpdateMetadata(
                    mdb -> mdb.putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(Map.of(nodeId, shutdownMetadataBuilder.setNodeId(nodeId).build()))
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                listener.onResponse(null);
            }
        });
    }

    private static void clearShutdownMetadata(ClusterService clusterService) {
        safeAwait(listener -> clusterService.submitUnbatchedStateUpdateTask("remove restart marker", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState.copyAndUpdateMetadata(mdb -> mdb.putCustom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY));
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                listener.onResponse(null);
            }
        }));
    }
}

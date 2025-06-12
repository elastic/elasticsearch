/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.snapshots.SnapshotShutdownProgressTracker.SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.oneOf;

public class SnapshotShutdownIT extends AbstractSnapshotIntegTestCase {

    private static final String REQUIRE_NODE_NAME_SETTING = IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name";

    private MockLog mockLog;

    public void setUp() throws Exception {
        super.setUp();
        mockLog = MockLog.capture(SnapshotShutdownProgressTracker.class);
    }

    private void resetMockLog() {
        mockLog.close();
        mockLog = MockLog.capture(SnapshotShutdownProgressTracker.class);
    }

    public void tearDown() throws Exception {
        mockLog.close();
        super.tearDown();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    /**
     * Tests that shard snapshots on a node with RESTART shutdown metadata will finish on the same node.
     */
    @TestLogging(
        value = "org.elasticsearch.snapshots.SnapshotShutdownProgressTracker:DEBUG",
        reason = "Testing SnapshotShutdownProgressTracker's progress, which is reported at the DEBUG logging level"
    )
    public void testRestartNodeDuringSnapshot() throws Exception {
        // Marking a node for restart has no impact on snapshots (see #71333 for how to handle this case)
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode(
            // Speed up the logging frequency, so that the test doesn't have to wait too long to check for log messages.
            Settings.builder().put(SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200)).build()
        );
        final String originalNodeId = internalCluster().getInstance(NodeEnvironment.class, originalNode).nodeId();

        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(1, 0).put(REQUIRE_NODE_NAME_SETTING, originalNode).build());

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, originalNode);
        safeAwait((ActionListener<Void> l) -> flushMasterQueue(clusterService, l));
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

        // Ensure that the SnapshotShutdownProgressTracker does not start logging in RESTART mode.
        mockLog.addExpectation(
            new MockLog.UnseenEventExpectation(
                "SnapshotShutdownProgressTracker start log message",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.DEBUG,
                "Starting shutdown snapshot progress logging on node [" + originalNodeId + "]"
            )
        );

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

        // Verify no SnapshotShutdownProgressTracker logging in RESTART mode.
        mockLog.awaitAllExpectationsMatched();
        resetMockLog();

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
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName, 1);
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
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName, 1);
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
                    new AddVotingConfigExclusionsRequest(
                        TEST_REQUEST_TIMEOUT,
                        Strings.EMPTY_ARRAY,
                        new String[] { masterName },
                        TimeValue.timeValueSeconds(10)
                    ),
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
                l -> client().admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).execute(l)
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
            final var clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT);
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
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName, 1);
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
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName, 1);
        addUnassignedShardsWatcher(clusterService, indexName);

        putShutdownForRemovalMetadata(primaryNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, but allocation filtering stops it from moving
        safeAwait(snapshotPausedListener);
        assertFalse(snapshotFuture.isDone());

        // give up on the node shutdown so the shard snapshot can restart
        clearShutdownMetadata(clusterService);

        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());
    }

    /**
     * Tests that deleting a snapshot will abort paused shard snapshots on a node with shutdown metadata.
     */
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
        unblockAllDataNodes(repoName); // lets the shard snapshot pause, but allocation filtering stops it from moving
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

    /**
     * This test exercises the SnapshotShutdownProgressTracker's log messages reporting the progress of shard snapshots on data nodes.
     */
    @TestLogging(
        value = "org.elasticsearch.snapshots.SnapshotShutdownProgressTracker:TRACE",
        reason = "Testing SnapshotShutdownProgressTracker's progress, which is reported at the TRACE logging level"
    )
    public void testSnapshotShutdownProgressTracker() throws Exception {
        final var repoName = randomIdentifier();
        final int numShards = randomIntBetween(1, 9);
        createRepository(repoName, "mock");

        // Create another index on another node which will be blocked (remain in state INIT) throughout.
        // Not required for this test, just adds some more concurrency.
        final var otherNode = internalCluster().startDataOnlyNode();
        final var otherIndex = randomIdentifier();
        createIndexWithContent(otherIndex, indexSettings(numShards, 0).put(REQUIRE_NODE_NAME_SETTING, otherNode).build());
        blockDataNode(repoName, otherNode);

        final var nodeForRemoval = internalCluster().startDataOnlyNode(
            // Speed up the logging frequency, so that the test doesn't have to wait too long to check for log messages.
            Settings.builder().put(SNAPSHOT_PROGRESS_DURING_SHUTDOWN_LOG_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200)).build()
        );
        final String nodeForRemovalId = internalCluster().getInstance(NodeEnvironment.class, nodeForRemoval).nodeId();
        final var indexName = randomIdentifier();
        createIndexWithContent(indexName, indexSettings(numShards, 0).put(REQUIRE_NODE_NAME_SETTING, nodeForRemoval).build());
        indexAllShardsToAnEqualOrGreaterMinimumSize(indexName, ByteSizeValue.of(2, ByteSizeUnit.KB).getBytes());

        // Start the snapshot with blocking in place on the data node not to allow shard snapshots to finish yet.
        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, nodeForRemoval);
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName, indexName, numShards);
        addUnassignedShardsWatcher(clusterService, indexName);

        waitForBlock(otherNode, repoName);

        logger.info("---> nodeForRemovalId: " + nodeForRemovalId + ", numShards: " + numShards);
        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "SnapshotShutdownProgressTracker start log message",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.DEBUG,
                "Starting shutdown snapshot progress logging on node [" + nodeForRemovalId + "]"
            )
        );
        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "SnapshotShutdownProgressTracker pause set log message",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.DEBUG,
                "Pause signals have been set for all shard snapshots on data node [" + nodeForRemovalId + "]"
            )
        );
        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "SnapshotShutdownProgressTracker index shard snapshot status messages",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                // Expect the shard snapshot to stall in data file upload, since we've blocked the data node file upload to the blob store.
                "statusDescription='enqueued file snapshot tasks: threads running concurrent file uploads'"
            )
        );

        putShutdownForRemovalMetadata(nodeForRemoval, clusterService);

        // Check that the SnapshotShutdownProgressTracker was turned on after the shutdown metadata is set above.
        mockLog.awaitAllExpectationsMatched();
        resetMockLog();

        // At least one shard reached the MockRepository's blocking code when waitForBlock was called. However, there's no guarantee that
        // the other shards got that far before the shutdown flag was put in place, in which case the other shards may be paused instead.
        mockLog.addExpectation(
            new MockLog.PatternSeenEventExpectation(
                "SnapshotShutdownProgressTracker running number of snapshots",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                ".+Number shard snapshots running \\[[1-" + numShards + "]].+"
            )
        );

        // Check that the SnapshotShutdownProgressTracker is tracking the active (not yet paused) shard snapshots.
        mockLog.awaitAllExpectationsMatched();
        resetMockLog();

        // Block on the master when a shard snapshot request comes in, until we can verify that the Tracker saw the outgoing request.
        final CountDownLatch snapshotStatusUpdateLatch = new CountDownLatch(1);
        final var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        masterTransportService.addRequestHandlingBehavior(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            (handler, request, channel, task) -> masterTransportService.getThreadPool().generic().execute(() -> {
                safeAwait(snapshotStatusUpdateLatch);
                try {
                    handler.messageReceived(request, channel, task);
                } catch (Exception e) {
                    fail(e);
                }
            })
        );

        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "SnapshotShutdownProgressTracker shard snapshot has paused log message",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                "*Number shard snapshots waiting for master node reply to status update request [" + numShards + "]*"
            )
        );

        // Let the shard snapshot proceed. It will still get stuck waiting for the master node to respond.
        unblockNode(repoName, nodeForRemoval);

        // Check that the SnapshotShutdownProgressTracker observed the request sent to the master node.
        mockLog.awaitAllExpectationsMatched();
        resetMockLog();

        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "SnapshotShutdownProgressTracker shard snapshot has paused log message",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                "Current active shard snapshot stats on data node [" + nodeForRemovalId + "]*Paused [" + numShards + "]"
            )
        );
        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "SnapshotShutdownProgressTracker index shard snapshot messages",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.INFO,
                "*successfully sent shard snapshot state [PAUSED_FOR_NODE_REMOVAL] update to the master node*"
            )
        );

        // Release the master node to respond
        snapshotStatusUpdateLatch.countDown();

        // Wait for the snapshot to fully pause.
        safeAwait(snapshotPausedListener);

        // Check that the SnapshotShutdownProgressTracker observed the shard snapshot finishing as paused.
        mockLog.awaitAllExpectationsMatched();
        resetMockLog();

        // Remove the allocation filter so that the shard moves off of the node shutting down.
        updateIndexSettings(Settings.builder().putNull(REQUIRE_NODE_NAME_SETTING), indexName);

        // Wait for the shard snapshot to succeed on the non-shutting down node.
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

        // Snapshot completes when the node vacates even though it hasn't been removed yet
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());

        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "SnapshotShutdownProgressTracker cancelled log message",
                SnapshotShutdownProgressTracker.class.getCanonicalName(),
                Level.DEBUG,
                "Cancelling shutdown snapshot progress logging on node [" + nodeForRemovalId + "]"
            )
        );

        clearShutdownMetadata(clusterService);

        // Check that the SnapshotShutdownProgressTracker logging was cancelled by the removal of the shutdown metadata.
        mockLog.awaitAllExpectationsMatched();
        resetMockLog();
    }

    private static SubscribableListener<Void> createSnapshotPausedListener(
        ClusterService clusterService,
        String repoName,
        String indexName,
        int numShards
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
            assertThat(shardSnapshotStatuses, hasSize(numShards));
            for (var shardStatus : shardSnapshotStatuses) {
                assertThat(
                    shardStatus.state(),
                    oneOf(SnapshotsInProgress.ShardState.INIT, SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL)
                );
                if (shardStatus.state() == SnapshotsInProgress.ShardState.INIT) {
                    return false;
                }
            }
            return true;
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
                final var node = currentState.nodes().resolveNode(nodeName);
                return currentState.copyAndUpdateMetadata(
                    mdb -> mdb.putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                node.getId(),
                                shutdownMetadataBuilder.setNodeId(node.getId()).setNodeEphemeralId(node.getEphemeralId()).build()
                            )
                        )
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

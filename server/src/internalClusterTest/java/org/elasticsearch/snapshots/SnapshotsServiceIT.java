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
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SnapshotsServiceIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testShardSnapshotWaitForDesiredAllocation() throws Exception {
        createRepository("test-repo", "fs");
        internalCluster().ensureAtLeastNumDataNodes(3);
        final int numDataNodes = internalCluster().numDataNodes();
        final var indexName = "index";
        createIndex(indexName, indexSettings(numDataNodes, 0).build());
        indexRandomDocs(indexName, between(42, 100));
        ensureGreen(indexName);
        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final Set<String> dataNodeIds = clusterService.state().nodes().getDataNodes().keySet();
        logger.info("--> all data nodes: {}", dataNodeIds);

        final Index index = clusterService().state().routingTable(ProjectId.DEFAULT).index(indexName).getIndex();
        final var shardsAllocator = (DesiredBalanceShardsAllocator) internalCluster().getCurrentMasterNodeInstance(ShardsAllocator.class);

        // Get all the node IDs that hosting at least one shard of the index
        final var initialNodeIdsForAllocation = internalCluster().nodesInclude(indexName)
            .stream()
            .map(ESIntegTestCase::getNodeId)
            .collect(Collectors.toUnmodifiableSet());

        // Delay shard snapshot status updates so that we control when snapshot is completed
        final var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        final Set<CheckedRunnable<Exception>> updateShardRunnables = ConcurrentCollections.newConcurrentSet();
        final var allRunnablesReadyLatch = new AtomicReference<>(new CountDownLatch(1));
        masterTransportService.addRequestHandlingBehavior(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            (handler, request, channel, task) -> {
                final var updateRequest = asInstanceOf(UpdateIndexShardSnapshotStatusRequest.class, request);
                if (updateRequest.shardId().getIndex().equals(index)) {
                    updateShardRunnables.add(() -> handler.messageReceived(request, channel, task));
                    if (updateShardRunnables.size() == numDataNodes) {
                        allRunnablesReadyLatch.get().countDown();
                    }
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        // Start snap-0 asynchronously and it will be delayed at shard snapshot status updates
        safeGet(
            client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "snap-0")
                .setWaitForCompletion(false)
                .setIndices(indexName)
                .execute()
        );
        safeAwait(allRunnablesReadyLatch.get());

        // Start snap-1 asynchronously and all shards will be queued because snap-0 is in progress
        safeGet(
            client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "snap-1")
                .setWaitForCompletion(false)
                .setIndices(indexName)
                .execute()
        );

        // Ensure all shards of snap-1 are queued
        safeAwait(ClusterServiceUtils.addTemporaryStateListener(clusterService, clusterState -> {
            final var snapshotsInProgress = SnapshotsInProgress.get(clusterState);
            final SnapshotsInProgress.Entry snapshotEntry = snapshotsInProgress.asStream()
                .filter(entry -> entry.snapshot().getSnapshotId().getName().equals("snap-1"))
                .findFirst()
                .orElse(null);
            if (snapshotEntry == null) {
                return false;
            }
            final List<SnapshotsInProgress.ShardState> states = snapshotEntry.shards()
                .values()
                .stream()
                .map(SnapshotsInProgress.ShardSnapshotStatus::state)
                .toList();
            if (states.size() != numDataNodes) {
                return false;
            }
            return states.stream().allMatch(state -> state == SnapshotsInProgress.ShardState.QUEUED);
        }));

        // Create undesired allocation by allowing allocation on only one node
        final String nodeIdAllowAllocation = randomFrom(dataNodeIds);
        logger.info("--> update settings to allow allocation on node [{}]", nodeIdAllowAllocation);
        updateIndexSettings(
            Settings.builder()
                .put(
                    "index.routing.allocation.exclude._id",
                    Strings.collectionToCommaDelimitedString(
                        dataNodeIds.stream().filter(nodeId -> nodeId.equals(nodeIdAllowAllocation) == false).toList()
                    )
                )
        );
        ClusterRerouteUtils.reroute(client());
        // Desired balance should be updated to allow allocation on nodeIdAllowAllocation
        final DesiredBalance desiredBalance = shardsAllocator.getDesiredBalance();
        for (int j = 0; j < numDataNodes; j++) {
            final ShardAssignment assignment = desiredBalance.getAssignment(new ShardId(index, j));
            assertThat(assignment.nodeIds(), contains(nodeIdAllowAllocation));
            logger.info("--> after update settings shard [{}] Assignment {}", j, assignment);
        }

        // All shards are still not moved because they are locked down by snap-0
        assertThat(
            internalCluster().nodesInclude(indexName).stream().map(ESIntegTestCase::getNodeId).collect(Collectors.toUnmodifiableSet()),
            equalTo(initialNodeIdsForAllocation)
        );
        final var hostingNodeIds = internalCluster().nodesInclude(indexName)
            .stream()
            .map(ESIntegTestCase::getNodeId)
            .collect(Collectors.toUnmodifiableSet());

        // Add a listener to ensure shards of snap-1 going through the state of waiting for desired allocation
        final var waitingForDesiredAllocationListener = ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> {
            final var snapshotsInProgress = SnapshotsInProgress.get(state);
            final SnapshotsInProgress.Entry snapshotEntry = snapshotsInProgress.asStream()
                .filter(entry -> entry.snapshot().getSnapshotId().getName().equals("snap-1"))
                .findFirst()
                .orElseThrow();
            return snapshotEntry.shards().entrySet().stream().anyMatch(entry -> entry.getValue().isWaitingForDesiredAllocation());
        });

        // Complete snap-0 to allow relocation of shards
        final var snap0UpdateShardRunnables = Set.copyOf(updateShardRunnables);
        updateShardRunnables.clear();
        allRunnablesReadyLatch.set(new CountDownLatch(1));
        for (CheckedRunnable<Exception> runnable : snap0UpdateShardRunnables) {
            runnable.run();
        }

        // We should see shards of snap-1 change to waiting for desired allocation as an intermediate state
        safeAwait(waitingForDesiredAllocationListener);

        // Wait for snap-0 to be ready to complete. This means relocations are completed.
        safeAwait(allRunnablesReadyLatch.get());

        // Shards should be relocated to the desired node before snap-1 is completed
        assertThat(
            internalCluster().nodesInclude(indexName).stream().map(ESIntegTestCase::getNodeId).collect(Collectors.toUnmodifiableSet()),
            contains(nodeIdAllowAllocation)
        );

        // Let snap-1 complete
        for (var runnable : updateShardRunnables) {
            runnable.run();
        }
        safeAwait(ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> SnapshotsInProgress.get(state).isEmpty()));

        // Both snapshots are completed successfully
        final var getSnapshotsResponse = safeGet(
            client().admin().cluster().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "test-repo", "snap-*").execute()
        );
        assertThat(
            getSnapshotsResponse.getSnapshots().stream().map(info -> info.snapshotId().getName()).toList(),
            containsInAnyOrder("snap-0", "snap-1")
        );
        assertTrue(getSnapshotsResponse.getSnapshots().stream().map(SnapshotInfo::state).allMatch(state -> state == SnapshotState.SUCCESS));
    }

    public void testDeletingSnapshotsIsLoggedAfterClusterStateIsProcessed() throws Exception {
        createRepository("test-repo", "fs");
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 42));
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));

        try (var mockLog = MockLog.capture(SnapshotsService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "[does-not-exist]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "deleting snapshots [does-not-exist] from repository [test-repo]"
                )
            );

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "[deleting test-snapshot]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "deleting snapshots [test-snapshot] from repository [test-repo]"
                )
            );

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "[test-snapshot deleted]",
                    SnapshotsService.class.getName(),
                    Level.INFO,
                    "snapshots [test-snapshot/*] deleted"
                )
            );

            final SnapshotMissingException e = expectThrows(
                SnapshotMissingException.class,
                startDeleteSnapshot("test-repo", "does-not-exist")
            );
            assertThat(e.getMessage(), containsString("[test-repo:does-not-exist] is missing"));
            assertThat(startDeleteSnapshot("test-repo", "test-snapshot").actionGet().isAcknowledged(), is(true));

            awaitNoMoreRunningOperations(); // ensure background file deletion is completed
            mockLog.assertAllExpectationsMatched();
        } finally {
            deleteRepository("test-repo");
        }
    }

    public void testSnapshotDeletionFailureShouldBeLogged() throws Exception {
        createRepository("test-repo", "mock");
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 42));
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));

        try (var mockLog = MockLog.capture(SnapshotsService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "[test-snapshot]",
                    SnapshotsService.class.getName(),
                    Level.WARN,
                    "failed to complete snapshot deletion for [test-snapshot] from repository [test-repo]"
                )
            );

            if (randomBoolean()) {
                // Failure when listing root blobs
                final MockRepository mockRepository = getRepositoryOnMaster("test-repo");
                mockRepository.setRandomControlIOExceptionRate(1.0);
                final Exception e = expectThrows(Exception.class, startDeleteSnapshot("test-repo", "test-snapshot"));
                assertThat(e.getCause().getMessage(), containsString("Random IOException"));
            } else {
                // Failure when finalizing on index-N file
                final ActionFuture<AcknowledgedResponse> deleteFuture;
                blockMasterFromFinalizingSnapshotOnIndexFile("test-repo");
                deleteFuture = startDeleteSnapshot("test-repo", "test-snapshot");
                waitForBlock(internalCluster().getMasterName(), "test-repo");
                unblockNode("test-repo", internalCluster().getMasterName());
                final Exception e = expectThrows(Exception.class, deleteFuture);
                assertThat(e.getCause().getMessage(), containsString("exception after block"));
            }

            mockLog.assertAllExpectationsMatched();
        } finally {
            deleteRepository("test-repo");
        }
    }

    public void testDeleteSnapshotWhenNotWaitingForCompletion() throws Exception {
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 5));
        createRepository("test-repo", "mock");
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));
        MockRepository repository = getRepositoryOnMaster("test-repo");
        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        SubscribableListener<Void> snapshotDeletionListener = createSnapshotDeletionListener("test-repo");
        repository.blockOnDataFiles();
        try {
            clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snapshot")
                .setWaitForCompletion(false)
                .execute(listener);
            // The request will complete as soon as the deletion is scheduled
            safeGet(listener);
            // The deletion won't complete until the block is removed
            assertFalse(snapshotDeletionListener.isDone());
        } finally {
            repository.unblock();
        }
        safeAwait(snapshotDeletionListener);
    }

    public void testDeleteSnapshotWhenWaitingForCompletion() throws Exception {
        createIndexWithRandomDocs("test-index", randomIntBetween(1, 5));
        createRepository("test-repo", "mock");
        createSnapshot("test-repo", "test-snapshot", List.of("test-index"));
        MockRepository repository = getRepositoryOnMaster("test-repo");
        PlainActionFuture<AcknowledgedResponse> requestCompleteListener = new PlainActionFuture<>();
        SubscribableListener<Void> snapshotDeletionListener = createSnapshotDeletionListener("test-repo");
        repository.blockOnDataFiles();
        try {
            clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snapshot")
                .setWaitForCompletion(true)
                .execute(requestCompleteListener);
            // Neither the request nor the deletion will complete until we remove the block
            assertFalse(requestCompleteListener.isDone());
            assertFalse(snapshotDeletionListener.isDone());
        } finally {
            repository.unblock();
        }
        safeGet(requestCompleteListener);
        safeAwait(snapshotDeletionListener);
    }

    /**
     * Create a listener that completes once it has observed a snapshot delete begin and end for a specific repository
     *
     * @param repositoryName The repository to monitor for deletions
     * @return the listener
     */
    private SubscribableListener<Void> createSnapshotDeletionListener(String repositoryName) {
        AtomicBoolean deleteHasStarted = new AtomicBoolean(false);
        return ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            SnapshotDeletionsInProgress deletionsInProgress = (SnapshotDeletionsInProgress) state.getCustoms()
                .get(SnapshotDeletionsInProgress.TYPE);
            if (deletionsInProgress == null) {
                return false;
            }
            if (deleteHasStarted.get() == false) {
                deleteHasStarted.set(deletionsInProgress.hasExecutingDeletion(repositoryName));
                return false;
            } else {
                return deletionsInProgress.hasExecutingDeletion(repositoryName) == false;
            }
        });
    }

    public void testRerouteWhenShardSnapshotsCompleted() throws Exception {
        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode();

        final var indexName = randomIdentifier();
        createIndexWithContent(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", originalNode).build()
        );

        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, originalNode);

        // Use allocation filtering to push the shard to a new node, but it will not do so yet because of the ongoing snapshot.
        updateIndexSettings(
            Settings.builder()
                .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name")
                .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", originalNode)
        );

        final var shardMovedListener = ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            final var primaryShard = state.routingTable().index(indexName).shard(0).primaryShard();
            return primaryShard.started() && originalNode.equals(state.nodes().get(primaryShard.currentNodeId()).getName()) == false;
        });
        assertFalse(shardMovedListener.isDone());

        unblockAllDataNodes(repoName);
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());

        // Now that the snapshot completed the shard should move to its new location.
        safeAwait(shardMovedListener);
        ensureGreen(indexName);
    }

    @TestLogging(reason = "testing task description, logged at DEBUG", value = "org.elasticsearch.cluster.service.MasterService:DEBUG")
    public void testCreateSnapshotTaskDescription() {
        createIndexWithRandomDocs(randomIdentifier(), randomIntBetween(1, 5));
        final var repositoryName = randomIdentifier();
        createRepository(repositoryName, "mock");

        final var snapshotName = randomIdentifier();
        MockLog.assertThatLogger(
            () -> createFullSnapshot(repositoryName, snapshotName),
            MasterService.class,
            new MockLog.SeenEventExpectation(
                "executing cluster state update debug message",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [create_snapshot ["
                    + snapshotName
                    + "][CreateSnapshotTask{repository="
                    + repositoryName
                    + ", snapshot=*"
                    + snapshotName
                    + "*}]]"
            )
        );
    }

}

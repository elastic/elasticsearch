/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.IndexEngineDynamicSettings;
import org.elasticsearch.xpack.stateless.engine.RefreshManagerService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.cluster.routing.IndexRoutingTestHelper.makeIdThatRoutesToShard;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.reshard.ReshardingTestHelpers.postSplitRouting;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;

public class SnapshotsWithReshardingIT extends AbstractStatelessPluginIntegTestCase {
    public void testShardSnapshotIsFailedWhenReshardingMetadataIsPresent() {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        ensureStableCluster(2);

        createRepository("test-repo", "fs");

        final int shards = between(1, 10);
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, shards, 0);
        ensureGreen();
        indexDocs(indexName, randomIntBetween(10, 100));

        var index = resolveIndex(indexName);

        // Block one of the resharding steps to be sure that resharding metadata is present during a snapshot.
        var latch = new CountDownLatch(1);
        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (org.elasticsearch.xpack.stateless.reshard.TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                    if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.DONE
                            && latch.getCount() > 0) {
                            latch.await();
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        ReshardIndexRequest reshardRequest = new ReshardIndexRequest(indexName);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet();

        awaitClusterState(indexNode, state -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() != null);
        ensureGreen(indexName);

        var createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", indexName + "-snap")
            .setWaitForCompletion(true)
            .setIndices(indexName)
            // so that we can assert status of shards in the response
            .setPartial(true)
            .get();

        int afterSplitShards = shards * 2;
        try {
            assertEquals(SnapshotState.PARTIAL, createSnapshotResponse.getSnapshotInfo().state());
            assertEquals(afterSplitShards, createSnapshotResponse.getSnapshotInfo().totalShards());
            assertEquals(
                createSnapshotResponse.getSnapshotInfo().toString(),
                0,
                createSnapshotResponse.getSnapshotInfo().successfulShards()
            );
            assertEquals(afterSplitShards, createSnapshotResponse.getSnapshotInfo().failedShards());

            assertTrue(
                createSnapshotResponse.getSnapshotInfo().shardFailures().toString(),
                createSnapshotResponse.getSnapshotInfo()
                    .shardFailures()
                    .stream()
                    .allMatch(ssf -> ssf.reason().contains("cannot snapshot a shard during resharding"))
            );
        } finally {
            latch.countDown();
        }

        waitForReshardCompletion(resolveIndex(indexName));

        // We should be able to restore this snapshot as partial.
        String restoredIndexName = indexName + "-restored";
        var restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", indexName + "-snap")
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();

        // All shard snapshots are failed.
        assertEquals(afterSplitShards, restoreSnapshotResponse.getRestoreInfo().totalShards());
        assertEquals(0, restoreSnapshotResponse.getRestoreInfo().successfulShards());
        assertEquals(afterSplitShards, restoreSnapshotResponse.getRestoreInfo().failedShards());

        // However the index should be restored successfully without data.
        ensureGreen(restoredIndexName);

        var restoredMetadata = clusterService().state().metadata().indexMetadata(resolveIndex(restoredIndexName));
        assertEquals(afterSplitShards, restoredMetadata.getNumberOfShards());
        // Restored index should not have resharding metadata to avoid
        // restarting a split from an inconsistent state.
        assertNull(restoredMetadata.getReshardingMetadata());
    }

    public void testShardSnapshotIsFailedDueToConcurrentCompletedSplit() {
        var indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        createRepository("test-repo", "fs");

        final int shards = between(1, 10);
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, shards, 0);
        ensureGreen();
        indexDocs(indexName, randomIntBetween(10, 100));

        // Block all shard snapshots so that we can execute a split
        // in the middle of the snapshot.
        // Since we block commit acquisition, we know that index metadata will not have
        // resharding metadata in it because the check is performed after acquiring a commit.
        var latch = new CountDownLatch(1);
        var plugin = findPlugin(indexNode, CustomSnapshotCommitSupplierPlugin.class);
        plugin.blockCommitAcquireForSnapshot.set(latch);

        client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", indexName + "-snap")
            .setWaitForCompletion(false)
            .setIndices(indexName)
            .setPartial(true)
            .get();

        ReshardIndexRequest reshardRequest = new ReshardIndexRequest(indexName);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet();
        waitForReshardCompletion(resolveIndex(indexName));

        latch.countDown();

        awaitClusterState(state -> SnapshotsInProgress.get(state).count() == 0);
        var snapshot = client().admin()
            .cluster()
            .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "test-repo")
            .setSnapshots(indexName + "-snap")
            .get()
            .getSnapshots()
            .get(0);

        assertEquals(SnapshotState.PARTIAL, snapshot.state());
        // Since the snapshot started before the split, it contains pre-split number of shards.
        assertEquals(shards, snapshot.totalShards());
        assertEquals(0, snapshot.successfulShards());
        assertEquals(shards, snapshot.failedShards());

        assertTrue(
            snapshot.shardFailures().toString(),
            snapshot.shardFailures().stream().allMatch(ssf -> ssf.reason().contains("cannot snapshot a shard during resharding"))
        );

        // We also should be able to restore this snapshot as partial.
        String restoredIndexName = indexName + "-restored";
        var restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", indexName + "-snap")
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();

        // All shard snapshots are failed.
        assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
        assertEquals(0, restoreSnapshotResponse.getRestoreInfo().successfulShards());
        assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().failedShards());

        // However the index should be restored successfully without data.
        ensureGreen(restoredIndexName);

        var restoredMetadata = clusterService().state().metadata().indexMetadata(resolveIndex(restoredIndexName));
        // Number of shards is "rolled back" to pre-split value to be able to restore the snapshot.
        assertEquals(shards, restoredMetadata.getNumberOfShards());
        // Sanity check - it should be null anyway since the split is complete when snapshot is finalized.
        assertNull(restoredMetadata.getReshardingMetadata());
    }

    public void testSnapshotWithConcurrentSplit() {
        startMasterOnlyNode();
        var nodes = startIndexNodes(3);
        startSearchNode();
        ensureStableCluster(5);

        createRepository("test-repo", "fs");

        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, 3, 1);
        ensureGreen();

        // Index documents that would result in predefined amount documents per shard after a split.
        int documentsPerShard = randomIntBetween(2, 10);
        final Index index = resolveIndex(indexName);
        final var indexRoutingPostSplit = postSplitRouting(clusterService().state(), index, 6);
        for (int shard = 0; shard < 3; shard++) {
            for (int doc = 0; doc < documentsPerShard; doc++) {
                var id = makeIdThatRoutesToShard(indexRoutingPostSplit, shard, Integer.toString(shard) + doc);
                prepareIndex(indexName).setId(id).setSource(Map.of("field", randomAlphaOfLength(5))).get();
            }
        }

        // We'll allow one shard snapshot to proceed at a time to test different scenarios.
        var shardSnapshotSemaphore = new Semaphore(1);
        for (var node : nodes) {
            var plugin = findPlugin(node, CustomSnapshotCommitSupplierPlugin.class);
            plugin.limitCommitAcquireForSnapshot.set(shardSnapshotSemaphore);
        }

        client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", indexName + "-snap")
            .setWaitForCompletion(false)
            .setIndices(indexName)
            .setPartial(true)
            .get();

        // One shard will successfully complete since the split hasn't started yet.
        awaitClusterState(state -> {
            var shardsStatus = SnapshotsInProgress.get(state).asStream().findFirst().get().shards();
            return shardsStatus.values().stream().anyMatch(s -> s.state() == SnapshotsInProgress.ShardState.SUCCESS);
        });

        // Now start a split but block it to ensure the next shard snapshot sees the resharding metadata.
        var blockSplitLatch = new CountDownLatch(1);
        // can be any node since there should be a split target shard on every node.
        MockTransportService mockTransportService = MockTransportService.getInstance(nodes.get(0));
        mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                    if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.DONE
                            && blockSplitLatch.getCount() > 0) {
                            blockSplitLatch.await();
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        ReshardIndexRequest reshardRequest = new ReshardIndexRequest(indexName);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet();

        shardSnapshotSemaphore.release(1);
        awaitClusterState(state -> {
            var shardsStatus = SnapshotsInProgress.get(state).asStream().findFirst().get().shards();
            return shardsStatus.values().stream().filter(s -> s.state() != SnapshotsInProgress.ShardState.INIT).count() == 2;
        });

        blockSplitLatch.countDown();
        waitForReshardCompletion(resolveIndex(indexName));

        // Unblock another shard snapshot now that the split has completed
        shardSnapshotSemaphore.release(1);
        // And the snapshot completes.
        awaitClusterState(state -> SnapshotsInProgress.get(state).count() == 0);
        var snapshot = client().admin()
            .cluster()
            .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "test-repo")
            .setSnapshots(indexName + "-snap")
            .get()
            .getSnapshots()
            .get(0);

        assertEquals(SnapshotState.PARTIAL, snapshot.state());
        // Since the snapshot started before the split, it contains pre-split number of shards.
        assertEquals(3, snapshot.totalShards());
        // Shard snapshot that used a commit before the split started is successful.
        assertEquals(1, snapshot.successfulShards());
        // Shard snapshot taken when resharding metadata is present is failed.
        // Shard snapshot taken after the split is also failed since a mismatch between index metadata
        // and snapshot metadata is detected.
        assertEquals(2, snapshot.failedShards());

        assertTrue(snapshot.shardFailures().stream().allMatch(ssf -> ssf.reason().contains("cannot snapshot a shard during resharding")));

        // We should be able to restore this snapshot as partial.
        String restoredIndexName = indexName + "-restored";
        var restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", indexName + "-snap")
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();

        assertEquals(3, restoreSnapshotResponse.getRestoreInfo().totalShards());
        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());
        assertEquals(2, restoreSnapshotResponse.getRestoreInfo().failedShards());

        ensureGreen(restoredIndexName);

        var restoredMetadata = clusterService().state().metadata().indexMetadata(resolveIndex(restoredIndexName));
        assertEquals(3, restoredMetadata.getNumberOfShards());
        // Sanity check - it should be null anyway since the split is complete when snapshot is finalized.
        assertNull(restoredMetadata.getReshardingMetadata());

        assertResponse(
            prepareSearch(restoredIndexName).setQuery(QueryBuilders.matchAllQuery())
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(false),
            searchResponse -> {
                // We should have documents from the one shard that has a successful snapshot.
                assertEquals(documentsPerShard, searchResponse.getHits().getTotalHits().value());
            }
        );
    }

    // Tests that when we adjust index metadata we properly handle index metadata deduplication.
    // Also verifies post-reshard snapshot captures document deletes from cleanup by restoring and searching
    public void testNextSnapshotHasCorrectMetadata() {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        createRepository("test-repo", "fs");

        final int shards = between(1, 10);
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        // Need at least one search replica so the restored index is searchable.
        createIndex(indexName, shards, 1);
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        if (randomBoolean()) {
            // Create a pre-split snapshot to ensure we won't reuse the metadata later.
            String preSplitSnapshotName = indexName + "-pre-split-snap";
            var preSplitSnapshot = client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .setPartial(true)
                .get();

            assertEquals(SnapshotState.SUCCESS, preSplitSnapshot.getSnapshotInfo().state());

            assertEquals(shards, preSplitSnapshot.getSnapshotInfo().totalShards());
            assertEquals(shards, preSplitSnapshot.getSnapshotInfo().successfulShards());
            assertEquals(0, preSplitSnapshot.getSnapshotInfo().failedShards());
        }

        // Block all shard snapshots so that we can execute a split
        // in the middle of the snapshot.
        // Since we block commit acquisition, we know that index metadata will not have
        // resharding metadata in it because the check is performed after acquiring a commit.
        var latch = new CountDownLatch(1);
        var plugin = findPlugin(indexNode, CustomSnapshotCommitSupplierPlugin.class);
        plugin.blockCommitAcquireForSnapshot.set(latch);

        client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", indexName + "-snap")
            .setWaitForCompletion(false)
            .setIndices(indexName)
            .setPartial(true)
            .get();

        ReshardIndexRequest reshardRequest = new ReshardIndexRequest(indexName);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet();
        waitForReshardCompletion(resolveIndex(indexName));

        latch.countDown();

        awaitClusterState(state -> SnapshotsInProgress.get(state).count() == 0);
        var snapshot = client().admin()
            .cluster()
            .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "test-repo")
            .setSnapshots(indexName + "-snap")
            .get()
            .getSnapshots()
            .get(0);

        assertEquals(SnapshotState.PARTIAL, snapshot.state());
        assertEquals(shards, snapshot.totalShards());

        // We test that this snapshot can be restored in testShardSnapshotIsFailedDueToConcurrentCompletedSplit above.

        // Now we'll take another snapshot.
        String postSplitSnapshotName = indexName + "-post-split-snap";
        var postSplitSnapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", postSplitSnapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .setPartial(true)
            .get();

        // This snapshot should be successful and contain complete post-reshard state of the index metadata.
        // We shouldn't reuse adjusted index metadata blob from the previous snapshot.
        assertEquals(SnapshotState.SUCCESS, postSplitSnapshot.getSnapshotInfo().state());

        assertEquals(shards * 2, postSplitSnapshot.getSnapshotInfo().totalShards());
        assertEquals(shards * 2, postSplitSnapshot.getSnapshotInfo().successfulShards());
        assertEquals(0, postSplitSnapshot.getSnapshotInfo().failedShards());

        String restoredIndexName = indexName + "-restored";
        var restorePostSplitSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", postSplitSnapshotName)
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();

        assertEquals(shards * 2, restorePostSplitSnapshotResponse.getRestoreInfo().totalShards());
        assertEquals(shards * 2, restorePostSplitSnapshotResponse.getRestoreInfo().successfulShards());
        assertEquals(0, restorePostSplitSnapshotResponse.getRestoreInfo().failedShards());

        ensureGreen(restoredIndexName);

        var restoredMetadata = clusterService().state().metadata().indexMetadata(resolveIndex(restoredIndexName));
        assertEquals(shards * 2, restoredMetadata.getNumberOfShards());
        // Sanity check - it should be null anyway since the split is complete.
        assertNull(restoredMetadata.getReshardingMetadata());

        assertResponse(
            prepareSearch(restoredIndexName).setQuery(QueryBuilders.matchAllQuery())
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(false),
            searchResponse -> {
                assertEquals(numDocs, searchResponse.getHits().getTotalHits().value());
            }
        );
    }

    // Restoring over an open index while it is actively being resharded used to be rejected outright. The restore now discards the
    // resharding metadata and the extra target shards along with everything else, the same as any other in-place restore to a
    // different shard count. IndicesClusterStateService#isRestoreHistoryUuidTransition tears down and recreates every shard of the
    // index, which safely unwinds the in-progress split too. The tests below hold the split's target shards in each of the states they
    // pass through before they are done, since the in-flight work that has to be abandoned differs in each.

    /**
     * Restores while the targets are still cloning from their sources. Unlike the other variants, the restore is not held back until the
     * clone is finished: it is submitted as soon as the resharding metadata appears, so it races with the copy that is in progress.
     */
    public void testRestoreOverOpenIndexWhileResharding_targetsInClone() throws Exception {
        restoreOverOpenIndexWhileResharding(IndexReshardingState.Split.TargetShardState.CLONE, false, false);
    }

    /**
     * Restores while every source shard is handing off to its target. The source has stopped indexing (it holds all of its operation
     * permits) and is waiting for the cluster state to show the target in HANDOFF, which does not happen because the target's request
     * to move to HANDOFF is held back. The source's handoff must be abandoned, and its permits released, when the restore replaces the
     * index.
     */
    public void testRestoreOverOpenIndexWhileResharding_handoffInFlight() throws Exception {
        restoreOverOpenIndexWhileResharding(IndexReshardingState.Split.TargetShardState.CLONE, true, false);
    }

    /** Restores once all targets have received the handoff and are in the HANDOFF state, before they move to SPLIT. */
    public void testRestoreOverOpenIndexWhileResharding_targetsInHandoff() throws Exception {
        restoreOverOpenIndexWhileResharding(IndexReshardingState.Split.TargetShardState.HANDOFF, true, false);
    }

    /** Restores once the handoff is complete and the targets are in the split state, waiting to be marked as done. */
    public void testRestoreOverOpenIndexWhileResharding_targetsInSplit() throws Exception {
        restoreOverOpenIndexWhileResharding(IndexReshardingState.Split.TargetShardState.SPLIT, true, false);
    }

    /**
     * As {@link #testRestoreOverOpenIndexWhileResharding_handoffInFlight}, but the source and the target are on different nodes, so the
     * handoff request and the target's state change requests cross the network, and each node has to clean up its own side of the split.
     */
    public void testRestoreOverOpenIndexWhileResharding_onSeparateNodes_handoffInFlight() throws Exception {
        restoreOverOpenIndexWhileResharding(IndexReshardingState.Split.TargetShardState.CLONE, true, true);
    }

    /** As {@link #testRestoreOverOpenIndexWhileResharding_targetsInHandoff}, but the source and the target are on different nodes. */
    public void testRestoreOverOpenIndexWhileResharding_onSeparateNodes_targetsInHandoff() throws Exception {
        restoreOverOpenIndexWhileResharding(IndexReshardingState.Split.TargetShardState.HANDOFF, true, true);
    }

    /** As {@link #testRestoreOverOpenIndexWhileResharding_targetsInSplit}, but the source and the target are on different nodes. */
    public void testRestoreOverOpenIndexWhileResharding_onSeparateNodes_targetsInSplit() throws Exception {
        restoreOverOpenIndexWhileResharding(IndexReshardingState.Split.TargetShardState.SPLIT, true, true);
    }

    /**
     * Restores once every target is done, while the sources have not started their cleanup: each source's request to move to
     * READY_FOR_CLEANUP is held back.
     */
    public void testRestoreOverOpenIndexWhileResharding_sourceCleanupNotStarted() throws Exception {
        restoreOverOpenIndexDuringSourceCleanup(IndexReshardingState.Split.SourceShardState.READY_FOR_CLEANUP);
    }

    /**
     * Restores once every target is done and the sources have deleted the documents they no longer own, while each source's request
     * to move to DONE is held back, so that the source is still in READY_FOR_CLEANUP.
     */
    public void testRestoreOverOpenIndexWhileResharding_sourceCleanupComplete() throws Exception {
        restoreOverOpenIndexDuringSourceCleanup(IndexReshardingState.Split.SourceShardState.DONE);
    }

    /**
     * Starts a split, holds its target shards in {@code heldState}, restores the pre-split snapshot over the index, and verifies that the
     * restore succeeds and that the abandoned split leaves nothing behind.
     * <p>
     * A target shard is held in a state by blocking, on the target's node, the request that would move it to the following state.
     *
     * @param heldState         the state the targets are held in
     * @param awaitHeldRequests whether to wait until every target's request to leave {@code heldState} has been held back before
     *                          restoring. If not, the restore races with whatever the targets are doing in that state.
     * @param targetOnSeparateNode whether the target shard is on a different index node than its source. That limits the index to a
     *                          single shard, so that the one target shard is the only one that can be placed on the second node.
     */
    private void restoreOverOpenIndexWhileResharding(
        IndexReshardingState.Split.TargetShardState heldState,
        boolean awaitHeldRequests,
        boolean targetOnSeparateNode
    ) throws Exception {
        final IndexReshardingState.Split.TargetShardState blockedTransition = switch (heldState) {
            case CLONE -> IndexReshardingState.Split.TargetShardState.HANDOFF;
            case HANDOFF -> IndexReshardingState.Split.TargetShardState.SPLIT;
            case SPLIT -> IndexReshardingState.Split.TargetShardState.DONE;
            case DONE -> throw new AssertionError("a split cannot be held in " + heldState);
        };

        // The master must be a different node from the one holding the shards. A state change request sent to the local master is
        // executed directly instead of being sent over the transport, which would bypass the send behavior used below to hold it.
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        // The node the target shard is on, which is where its state change requests are sent from and so where they are held
        var targetNode = indexNode;
        final int shards;
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        if (targetOnSeparateNode) {
            // Allowing a single shard per node keeps the target off the source's node. After the split there are two shards (and two
            // replicas), so there need to be two search nodes for the index to be green.
            startSearchNodes(2);
            ensureStableCluster(4);
            createRepository("test-repo", "fs");
            shards = 1;
            createIndex(
                indexName,
                indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
            );
            ensureGreen(indexName);
            // The source must stay where it is when the second index node joins
            updateClusterSettings(
                Settings.builder()
                    .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            );
            targetNode = startIndexNode();
            ensureStableCluster(5);
        } else {
            startSearchNode();
            ensureStableCluster(3);
            createRepository("test-repo", "fs");
            // Each held request occupies a thread until it is released. Keep the number of targets low enough that all of them can be
            // held at once, since a request that can't get a thread would never be seen to be held.
            shards = between(1, 3);
            // Need at least one search replica so the restored index is searchable.
            createIndex(indexName, shards, 1);
        }
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        // Snapshot before resharding starts, so we have something with the pre-split shard count to restore back to.
        var preSplitSnapshotName = indexName + "-pre-split-snap";
        var preSplitSnapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(SnapshotState.SUCCESS, preSplitSnapshot.getSnapshotInfo().state());

        var index = resolveIndex(indexName);

        // Block the transition out of heldState so that the targets stay in it, with the resharding metadata still present, until the
        // restore has been applied. The count of blocked requests lets the test know when every target has reached that point.
        var latch = new CountDownLatch(1);
        var blockedRequests = new AtomicInteger();
        MockTransportService targetTransportService = MockTransportService.getInstance(targetNode);
        targetTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                    if (actualRequest instanceof SplitStateRequest splitStateRequest
                        && splitStateRequest.getNewTargetShardState() == blockedTransition
                        && latch.getCount() > 0) {
                        blockedRequests.incrementAndGet();
                        latch.await();
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        ReshardIndexRequest reshardRequest = new ReshardIndexRequest(indexName);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet();

        awaitClusterState(indexNode, state -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() != null);
        final int targetShards = clusterService().state()
            .metadata()
            .projectFor(index)
            .index(indexName)
            .getReshardingMetadata()
            .getSplit()
            .targetStates()
            .toList()
            .size();
        if (targetOnSeparateNode) {
            // Guard the premise of the test: the source and the target are on different nodes
            final var finalTargetNode = targetNode;
            awaitClusterState(indexNode, state -> {
                final var shardRoutingTable = state.routingTable(ProjectId.DEFAULT).index(index).shard(1);
                return shardRoutingTable != null
                    && shardRoutingTable.primaryShard().currentNodeId() != null
                    && state.nodes().get(shardRoutingTable.primaryShard().currentNodeId()).getName().equals(finalTargetNode);
            });
            assertNotEquals(indexNode, targetNode);
        }

        // Cluster state listeners of the abandoned split, such as the source's handoff observer, must cope with the restored state.
        // An exception from one of them is only logged by the cluster applier, so look for that.
        try (var mockLog = MockLog.capture(ClusterApplierService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "cluster state listener failed",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to notify ClusterStateListener"
                )
            );
            try {
                if (awaitHeldRequests) {
                    // Wait until every target is held, in the cluster state and at the blocked request, before restoring.
                    awaitClusterState(
                        indexNode,
                        state -> state.metadata()
                            .projectFor(index)
                            .index(indexName)
                            .getReshardingMetadata()
                            .getSplit()
                            .targetStates()
                            .allMatch(targetState -> targetState == heldState)
                    );
                    assertBusy(() -> assertEquals(targetShards, blockedRequests.get()));
                }

                // Restore over the open, actively-resharding index, back to its pre-split shard count.
                var restoreRequest = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName).indices(indexName)
                    .restoreOverExisting(true)
                    .waitForCompletion(true);
                var restoreResponse = client().execute(TransportRestoreSnapshotAction.TYPE, restoreRequest).actionGet();

                assertEquals(shards, restoreResponse.getRestoreInfo().totalShards());
                assertEquals(shards, restoreResponse.getRestoreInfo().successfulShards());
                assertEquals(0, restoreResponse.getRestoreInfo().failedShards());
            } finally {
                // The held requests are only released after the restore, so that they reach the master as stale requests from shards
                // that no longer exist.
                latch.countDown();
            }

            assertRestoredAndAbandonedSplitCleanedUp(indexName, shards, numDocs, indexNode, targetNode);
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * A split source's state machine asks for its state to become READY_FOR_CLEANUP and then waits to observe that. Here the answer to that
     * request is held back while a restore replaces the index and drops its resharding metadata. The source's old shard is also kept
     * from finishing its close, so its state machine has not been cancelled when it finally looks at the (restored) cluster state. It
     * must not carry on with the cleanup of a split that no longer exists, in particular not on the shard that replaced the old one.
     */
    public void testRestoreOverOpenIndexWhileSourceObservesReadyForCleanup() throws Exception {
        var masterNode = startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        createRepository("test-repo", "fs");

        final int shards = 1;
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, shards, 1);
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        var preSplitSnapshotName = indexName + "-pre-split-snap";
        var preSplitSnapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(SnapshotState.SUCCESS, preSplitSnapshot.getSnapshotInfo().state());

        var index = resolveIndex(indexName);
        var plugin = findPlugin(indexNode, CustomSnapshotCommitSupplierPlugin.class);
        var releaseResponse = new CountDownLatch(1);
        var heldResponses = new AtomicInteger();
        var releaseClose = new CountDownLatch(1);
        plugin.holdEngineClose.set(releaseClose);

        // The master applies the source's move to READY_FOR_CLEANUP, but the source only hears about it once released
        var masterTransport = MockTransportService.getInstance(masterNode);
        masterTransport.addRequestHandlingBehavior(
            TransportUpdateSplitSourceShardStateAction.TYPE.name(),
            (handler, request, channel, task) -> {
                var actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                if (actualRequest instanceof TransportUpdateSplitSourceShardStateAction.Request sourceStateRequest
                    && sourceStateRequest.getState() == IndexReshardingState.Split.SourceShardState.READY_FOR_CLEANUP) {
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            heldResponses.incrementAndGet();
                            masterTransport.getThreadPool().generic().execute(() -> {
                                ESTestCase.safeAwait(releaseResponse);
                                channel.sendResponse(response);
                            });
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            channel.sendResponse(exception);
                        }

                        @Override
                        public TransportVersion getVersion() {
                            return channel.getVersion();
                        }
                    }, task);
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        try (var mockLog = MockLog.capture(SplitSourceService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "cleanup continued for a split that no longer exists",
                    SplitSourceService.class.getCanonicalName(),
                    Level.INFO,
                    "Advancing split source shard state machine for shard * to ReadyForCleanup*"
                )
            );

            client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
            // Every target is done and the source has asked for READY_FOR_CLEANUP, but has not been told about it yet
            assertBusy(() -> assertEquals(shards, heldResponses.get()));

            var restoreRequest = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName).indices(indexName)
                .restoreOverExisting(true)
                .waitForCompletion(true);
            var restore = client().execute(TransportRestoreSnapshotAction.TYPE, restoreRequest);
            awaitClusterState(indexNode, state -> {
                var metadata = state.metadata().projectFor(index).index(indexName);
                return metadata.getReshardingMetadata() == null && metadata.getNumberOfShards() == shards;
            });
            // The old source shard is closed but cannot finish closing, so its state machine has not been cancelled
            assertBusy(() -> assertThat(plugin.heldCloses.get(), greaterThan(0)));

            // Now let the state machine find out, and look at the cluster state, which is the restored one
            releaseResponse.countDown();
            // There is no event to wait for if the state machine does the right thing, so allow the wrong one some time to show itself
            safeSleep(TimeValue.timeValueSeconds(2));

            releaseClose.countDown();
            restore.actionGet();
            assertRestoredAndAbandonedSplitCleanedUp(indexName, shards, numDocs, indexNode);
            mockLog.assertAllExpectationsMatched();
        } finally {
            releaseResponse.countDown();
            releaseClose.countDown();
        }
    }

    /**
     * An index keeps its UUID through a restore, so the object store paths of its shards, which include the primary term, are shared by
     * every incarnation of a shard id. A shard id that disappears (the restore has fewer shards) and comes back (a reshard, or a restore
     * with more shards) must therefore never be given a primary term that an earlier incarnation of the id might have written under.
     * <p>
     * This follows the primary terms the master publishes through a series of restores and reshards that remove and re-add shard 1, and
     * checks that the terms of the shards that are created or re-created are always higher than every term seen before.
     */
    public void testPrimaryTermsAreNotReusedWhenShardsComeAndGoThroughRestoreAndReshard() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        createRepository("test-repo", "fs");

        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        // Need a search replica so that the index is searchable
        createIndex(indexName, 1, 1);
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);
        var index = resolveIndex(indexName);

        // The highest primary term that has been published for each shard id, including shard ids that have since been removed
        final Map<Integer, Long> highestTerms = new ConcurrentHashMap<>();
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final ClusterStateListener termRecorder = event -> {
            var metadata = event.state().metadata().findIndex(index).orElse(null);
            if (metadata != null) {
                for (int shard = 0; shard < metadata.getNumberOfShards(); shard++) {
                    highestTerms.merge(shard, metadata.primaryTerm(shard), Math::max);
                }
            }
        };
        masterClusterService.addListener(termRecorder);
        try {
            final var oneShardSnapshot = indexName + "-one-shard";
            createSnapshotAndAssertSuccess(oneShardSnapshot, indexName);

            // 1 -> 2 shards. Shard 1 is created.
            reshardAndAwaitCompletion(index, indexName);
            ensureGreen(indexName);
            final var twoShardSnapshot = indexName + "-two-shards";
            createSnapshotAndAssertSuccess(twoShardSnapshot, indexName);
            assertEquals(2, numberOfShards(index));

            // Restore back to 1 shard: shard 1 is removed, and shard 0 must have a higher term than either shard ever had.
            var highestBeforeShrink = Map.copyOf(highestTerms);
            restoreOverExisting(oneShardSnapshot, indexName);
            assertEquals(1, numberOfShards(index));
            assertTermsAreAbove(index, highestBeforeShrink, Collections.max(highestBeforeShrink.values()), "restore to fewer shards");

            // Reshard again. Shard 1 is re-created and must not reuse a term of its first incarnation.
            var highestBeforeSecondReshard = Map.copyOf(highestTerms);
            reshardAndAwaitCompletion(index, indexName);
            ensureGreen(indexName);
            assertEquals(2, numberOfShards(index));
            assertThat(
                "re-created shard 1 must have a term above its first incarnation",
                primaryTerm(index, 1),
                greaterThan(highestBeforeSecondReshard.get(1))
            );

            // Restore the snapshot of the 2 shard index over the 2 shard index (same shard count): all terms go up
            var highestBeforeSameCount = Map.copyOf(highestTerms);
            restoreOverExisting(twoShardSnapshot, indexName);
            assertEquals(2, numberOfShards(index));
            assertTermsAreAbove(index, highestBeforeSameCount, Collections.max(highestBeforeSameCount.values()), "restore to same shards");

            // Shrink again, then restore the 2 shard snapshot: shard 1 comes back through the restore this time.
            restoreOverExisting(oneShardSnapshot, indexName);
            assertEquals(1, numberOfShards(index));
            var highestBeforeGrow = Map.copyOf(highestTerms);
            restoreOverExisting(twoShardSnapshot, indexName);
            assertEquals(2, numberOfShards(index));
            assertTermsAreAbove(index, highestBeforeGrow, Collections.max(highestBeforeGrow.values()), "restore to more shards");

            assertResponse(
                prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery())
                    .setTrackTotalHits(true)
                    .setAllowPartialSearchResults(false),
                searchResponse -> assertEquals(numDocs, searchResponse.getHits().getTotalHits().value())
            );
        } finally {
            masterClusterService.removeListener(termRecorder);
        }
    }

    /**
     * Asserts that every shard of the index has a primary term above all the terms seen before the operation. This includes the shards that
     * existed above their own earlier terms, and all shards above {@code highestOfAnyShard}, which is what a restore has to guarantee for
     * the ids it removes or re-creates, since those no longer have a term of their own to build on.
     */
    private void assertTermsAreAbove(Index index, Map<Integer, Long> highestBefore, long highestOfAnyShard, String operation) {
        for (int shard = 0; shard < numberOfShards(index); shard++) {
            final long term = primaryTerm(index, shard);
            assertThat(operation + ": term of shard " + shard, term, greaterThan(highestBefore.getOrDefault(shard, 0L)));
            assertThat(operation + ": term of shard " + shard + " compared with every shard", term, greaterThan(highestOfAnyShard));
        }
    }

    private int numberOfShards(Index index) {
        return clusterService().state().metadata().projectFor(index).index(index.getName()).getNumberOfShards();
    }

    private long primaryTerm(Index index, int shard) {
        return clusterService().state().metadata().projectFor(index).index(index.getName()).primaryTerm(shard);
    }

    /**
     * Restores a snapshot that has as many shards as the split in progress would create (possible when the snapshot is of an earlier
     * index of the same name), so the target shard still exists afterwards. The source's handoff is then not abandoned for lack of shards,
     * and its permits must still be released: the restore advances the primary terms of the recreated shards, which the handoff observes.
     */
    public void testRestoreOverOpenIndexWhileHandoffInFlightToSnapshotWithTheTargetShards() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);
        createRepository("test-repo", "fs");
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, 2, 1);
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);
        createSnapshotAndAssertSuccess("two-shard-snap", indexName);
        assertAcked(client().admin().indices().prepareDelete(indexName));
        createIndex(indexName, 1, 1);
        ensureGreen();
        indexDocs(indexName, 5);
        var index = resolveIndex(indexName);

        var latch = new CountDownLatch(1);
        var blockedRequests = new AtomicInteger();
        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                    if (actualRequest instanceof SplitStateRequest r
                        && r.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.HANDOFF
                        && latch.getCount() > 0) {
                        blockedRequests.incrementAndGet();
                        latch.await();
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        awaitClusterState(indexNode, state -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() != null);
        assertBusy(() -> assertEquals(1, blockedRequests.get()));
        try {
            var restoreRequest = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "test-repo", "two-shard-snap").indices(indexName)
                .restoreOverExisting(true)
                .waitForCompletion(true);
            var restoreResponse = client().execute(TransportRestoreSnapshotAction.TYPE, restoreRequest).actionGet();
            assertEquals(0, restoreResponse.getRestoreInfo().failedShards());
        } finally {
            latch.countDown();
        }
        assertRestoredAndAbandonedSplitCleanedUp(indexName, 2, numDocs, indexNode);
    }

    private void createSnapshotAndAssertSuccess(String snapshotName, String indexName) {
        var snapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(SnapshotState.SUCCESS, snapshot.getSnapshotInfo().state());
    }

    private void reshardAndAwaitCompletion(Index index, String indexName) {
        client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        awaitClusterState(state -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() != null);
        waitForReshardCompletion(index);
    }

    private void restoreOverExisting(String snapshotName, String indexName) {
        var restoreRequest = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "test-repo", snapshotName).indices(indexName)
            .restoreOverExisting(true)
            .waitForCompletion(true);
        var response = client().execute(TransportRestoreSnapshotAction.TYPE, restoreRequest).actionGet();
        assertEquals(0, response.getRestoreInfo().failedShards());
        ensureGreen(indexName);
    }

    /**
     * After the handoff a target shard waits for the search shards of its own to come online before it moves on to SPLIT. That wait
     * inspects every cluster state it sees, and must cope with the restored one, in which the target shard may no longer exist.
     */
    public void testRestoreOverOpenIndexWhileTargetWaitsForSearchShards() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        createRepository("test-repo", "fs");

        final int shards = 1;
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, shards, 1);
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        var preSplitSnapshotName = indexName + "-pre-split-snap";
        createSnapshotAndAssertSuccess(preSplitSnapshotName, indexName);

        var index = resolveIndex(indexName);
        // The only search node is not allowed to hold the index, so the target's search shard cannot start and the target waits for it.
        // The setting is replaced by the one in the snapshot when the index is restored.
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNode), indexName);

        try (var mockLog = MockLog.capture(ClusterApplierService.class); var targetLog = MockLog.capture(SplitTargetService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "cluster state listener failed",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to notify ClusterStateListener"
                )
            );
            targetLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "target reached the wait for search shards",
                    SplitTargetService.class.getCanonicalName(),
                    Level.INFO,
                    "Advancing split target shard state machine for shard * to Handoff[]"
                )
            );

            client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
            // The handoff is done and the target shard has started, so the target is now waiting for its search shards. That the cluster
            // state shows HANDOFF is not enough: the target's state machine only gets to the wait once its shard has started.
            targetLog.awaitAllExpectationsMatched();

            var restoreRequest = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName).indices(indexName)
                .restoreOverExisting(true)
                .waitForCompletion(true);
            var restoreResponse = client().execute(TransportRestoreSnapshotAction.TYPE, restoreRequest).actionGet();
            assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

            assertRestoredAndAbandonedSplitCleanedUp(indexName, shards, numDocs, indexNode);
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * A reshard requested while a restore over the index is still in progress. The restore has been applied, but cannot complete because
     * the old shard cannot finish closing. Nothing rejects the reshard, so it is accepted. It then has to wait: shards are only created
     * once the shards of earlier cluster states have closed, so neither the restored shard nor the split's target starts until the old
     * one has closed. Both then run to completion, and the index ends up with all its documents and the shard count of the reshard.
     */
    public void testReshardRequestedWhileRestoreIsInProgress() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        createRepository("test-repo", "fs");

        final int shards = 1;
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, shards, 1);
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);
        var index = resolveIndex(indexName);

        var snapshotName = indexName + "-snap";
        createSnapshotAndAssertSuccess(snapshotName, indexName);

        var plugin = findPlugin(indexNode, CustomSnapshotCommitSupplierPlugin.class);
        var releaseClose = new CountDownLatch(1);
        plugin.holdEngineClose.set(releaseClose);
        try (var mockLog = MockLog.capture(ClusterApplierService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "cluster state listener failed",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to notify ClusterStateListener"
                )
            );

            var restoreRequest = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "test-repo", snapshotName).indices(indexName)
                .restoreOverExisting(true)
                .waitForCompletion(true);
            var restore = client().execute(TransportRestoreSnapshotAction.TYPE, restoreRequest);
            // The restore has been applied, but cannot complete while the old shard cannot close
            awaitClusterState(indexNode, state -> RestoreInProgress.get(state).isEmpty() == false);
            assertFalse(restore.isDone());

            client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
            awaitClusterState(
                indexNode,
                state -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() != null
                    || state.metadata().projectFor(index).index(indexName).getNumberOfShards() == 2
            );
            safeSleep(TimeValue.timeValueMillis(500));
            assertFalse("the restore cannot complete while the old shard cannot close", restore.isDone());

            releaseClose.countDown();
            var restoreResponse = restore.actionGet();
            assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

            // The reshard runs to completion on the restored shard
            awaitClusterState(
                indexNode,
                state -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() == null
                    && state.metadata().projectFor(index).index(indexName).getNumberOfShards() == 2
            );
            ensureGreen(indexName);
            assertResponse(
                prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery())
                    .setTrackTotalHits(true)
                    .setAllowPartialSearchResults(false),
                searchResponse -> assertEquals(numDocs, searchResponse.getHits().getTotalHits().value())
            );
            mockLog.assertAllExpectationsMatched();
        } finally {
            releaseClose.countDown();
        }
    }

    /**
     * Verifies that the index was restored to {@code shards} shards without any resharding metadata, still has all its documents, and
     * that the abandoned split left no state behind in the split services of any of {@code indexNodes}.
     */
    private void assertRestoredAndAbandonedSplitCleanedUp(String indexName, int shards, int numDocs, String... indexNodes)
        throws Exception {
        ensureGreen(indexName);

        var restoredMetadata = clusterService().state().metadata().indexMetadata(resolveIndex(indexName));
        assertEquals(shards, restoredMetadata.getNumberOfShards());
        assertNull(restoredMetadata.getReshardingMetadata());

        assertResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true).setAllowPartialSearchResults(false),
            searchResponse -> assertEquals(numDocs, searchResponse.getHits().getTotalHits().value())
        );

        // The abandoned split must leave no state behind on either service, or it leaks memory.
        for (String indexNode : new LinkedHashSet<>(Arrays.asList(indexNodes))) {
            final var splitTargetService = internalCluster().getInstance(SplitTargetService.class, indexNode);
            final var splitSourceService = internalCluster().getInstance(SplitSourceService.class, indexNode);
            assertBusy(() -> {
                assertThat("Split target state left behind on " + indexNode, splitTargetService.getShardsWithOngoingSplits(), empty());
                assertThat("Split source state left behind on " + indexNode, splitSourceService.getShardsWithActiveSplitState(), empty());
            });
        }
    }

    /**
     * Starts a split, lets every target finish, holds the sources' request to leave their cleanup stage at {@code heldTransition}, and
     * restores the pre-split snapshot over the index. The restore must succeed and leave nothing of the split behind, in particular
     * not the sources' cleanup, which is by then waiting for, or has sent, a state change request for a source that no longer exists.
     *
     * @param heldTransition the source state whose request is held back: READY_FOR_CLEANUP (cleanup not started) or DONE (cleanup of
     *                       unowned documents complete)
     */
    private void restoreOverOpenIndexDuringSourceCleanup(IndexReshardingState.Split.SourceShardState heldTransition) throws Exception {
        // As elsewhere, the master is a separate node so that the state change requests go over the transport, where they are held.
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        createRepository("test-repo", "fs");

        final int shards = between(1, 3);
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, shards, 1);
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        var preSplitSnapshotName = indexName + "-pre-split-snap";
        var preSplitSnapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(SnapshotState.SUCCESS, preSplitSnapshot.getSnapshotInfo().state());

        var index = resolveIndex(indexName);

        var latch = new CountDownLatch(1);
        var blockedRequests = new AtomicInteger();
        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitSourceShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                    if (actualRequest instanceof TransportUpdateSplitSourceShardStateAction.Request sourceStateRequest
                        && sourceStateRequest.getState() == heldTransition
                        && latch.getCount() > 0) {
                        blockedRequests.incrementAndGet();
                        latch.await();
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try (var mockLog = MockLog.capture(ClusterApplierService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "cluster state listener failed",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to notify ClusterStateListener"
                )
            );

            client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
            try {
                // Every target finishes, and then every source reaches the held request
                awaitClusterState(indexNode, state -> {
                    var resharding = state.metadata().projectFor(index).index(indexName).getReshardingMetadata();
                    return resharding != null
                        && resharding.getSplit().targetStates().allMatch(t -> t == IndexReshardingState.Split.TargetShardState.DONE);
                });
                assertBusy(() -> assertEquals(shards, blockedRequests.get()));

                var restoreRequest = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName).indices(indexName)
                    .restoreOverExisting(true)
                    .waitForCompletion(true);
                var restoreResponse = client().execute(TransportRestoreSnapshotAction.TYPE, restoreRequest).actionGet();
                assertEquals(shards, restoreResponse.getRestoreInfo().totalShards());
                assertEquals(shards, restoreResponse.getRestoreInfo().successfulShards());
                assertEquals(0, restoreResponse.getRestoreInfo().failedShards());
            } finally {
                // Released only after the restore, so that the held requests reach the master as stale requests from sources that
                // no longer exist.
                latch.countDown();
            }

            assertRestoredAndAbandonedSplitCleanedUp(indexName, shards, numDocs, indexNode);
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Restores while the source shards are in the middle of preparing for the handoff: they hold all of their operation permits and are
     * flushing, which is held back. The restore is submitted and applied while the flush is stuck, and only then is the flush released,
     * so that the source finds its shard closed (and the index replaced) when it resumes.
     */
    public void testRestoreOverOpenIndexWhileSourceIsFlushingWithPermitsHeld() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        createRepository("test-repo", "fs");

        final int shards = between(1, 3);
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, shards, 1);
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        var preSplitSnapshotName = indexName + "-pre-split-snap";
        var preSplitSnapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertEquals(SnapshotState.SUCCESS, preSplitSnapshot.getSnapshotInfo().state());

        var index = resolveIndex(indexName);
        var plugin = findPlugin(indexNode, CustomSnapshotCommitSupplierPlugin.class);
        var releaseFlush = new CountDownLatch(1);
        plugin.holdFlushWhileOperationsAreBlocked.set(releaseFlush);

        try (var mockLog = MockLog.capture(ClusterApplierService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "cluster state listener failed",
                    ClusterApplierService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to notify ClusterStateListener"
                )
            );

            client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
            // At least one source has acquired all of its permits and is stuck in the flush that follows
            assertBusy(() -> assertThat(plugin.heldFlushes.get(), greaterThan(0)));

            // Submit the restore without waiting for it: closing the old shards waits for the stuck flush, so it cannot complete yet.
            var restoreRequest = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "test-repo", preSplitSnapshotName).indices(indexName)
                .restoreOverExisting(true)
                .waitForCompletion(true);
            var restore = client().execute(TransportRestoreSnapshotAction.TYPE, restoreRequest);
            awaitClusterState(indexNode, state -> {
                var metadata = state.metadata().projectFor(index).index(indexName);
                return metadata.getReshardingMetadata() == null && metadata.getNumberOfShards() == shards;
            });
            assertFalse("the restore cannot complete while the old shards cannot close", restore.isDone());

            releaseFlush.countDown();
            var restoreResponse = restore.actionGet();
            assertEquals(shards, restoreResponse.getRestoreInfo().totalShards());
            assertEquals(shards, restoreResponse.getRestoreInfo().successfulShards());
            assertEquals(0, restoreResponse.getRestoreInfo().failedShards());

            assertRestoredAndAbandonedSplitCleanedUp(indexName, shards, numDocs, indexNode);
            mockLog.assertAllExpectationsMatched();
        } finally {
            releaseFlush.countDown();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(CustomSnapshotCommitSupplierPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // These tests are carefully set up and do not hit the situations that the delete unowned grace period prevents.
        return super.nodeSettings().put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO);
    }

    private void waitForReshardCompletion(Index index) {
        awaitClusterState((state) -> state.metadata().projectFor(index).index(index.getName()).getReshardingMetadata() == null);
    }

    public static class CustomSnapshotCommitSupplierPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        AtomicReference<CountDownLatch> blockCommitAcquireForSnapshot = new AtomicReference<>();
        AtomicReference<Semaphore> limitCommitAcquireForSnapshot = new AtomicReference<>();

        /**
         * When set, a flush of a shard that has had all of its operation permits acquired, as a split source does while preparing for
         * handoff, is held until the latch is released.
         */
        final AtomicReference<CountDownLatch> holdFlushWhileOperationsAreBlocked = new AtomicReference<>();
        final AtomicInteger heldFlushes = new AtomicInteger();

        /** When set, closing an engine is held until the latch is released, which keeps the shard from finishing its close. */
        final AtomicReference<CountDownLatch> holdEngineClose = new AtomicReference<>();
        final AtomicInteger heldCloses = new AtomicInteger();

        public CustomSnapshotCommitSupplierPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshManagerService refreshManagerService,
            ReshardIndexService reshardIndexService,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics,
            IndexEngineDynamicSettings indexEngineDynamicSettings
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics,
                indexEngineDynamicSettings,
                shardId -> {
                    final var indexShard = getIndicesService().getShardOrNull(shardId);
                    return indexShard == null || indexShard.routingEntry().relocating();
                },
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            ) {
                @Override
                public void flushAndClose() throws IOException {
                    final var release = holdEngineClose.get();
                    if (release != null) {
                        heldCloses.incrementAndGet();
                        ESTestCase.safeAwait(release);
                    }
                    super.flushAndClose();
                }

                @Override
                protected void flushHoldingLock(boolean force, boolean waitIfOngoing, FlushResultListener listener) throws EngineException {
                    final var release = holdFlushWhileOperationsAreBlocked.get();
                    if (release != null) {
                        final var indexShard = getIndicesService().getShardOrNull(engineConfig.getShardId());
                        if (indexShard != null && indexShard.getActiveOperationsCount() == IndexShard.OPERATIONS_BLOCKED) {
                            heldFlushes.incrementAndGet();
                            ESTestCase.safeAwait(release);
                        }
                    }
                    super.flushHoldingLock(force, waitIfOngoing, listener);
                }
            };
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(engineConfig -> {
                var factory = super.getEngineFactory(indexSettings).get();

                var snapshotCommitSupplier = engineConfig.getSnapshotCommitSupplier();
                IndexStorePlugin.SnapshotCommitSupplier customSnapshotCommitSupplier = engine -> {
                    if (blockCommitAcquireForSnapshot.get() != null) {
                        try {
                            blockCommitAcquireForSnapshot.get().await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (limitCommitAcquireForSnapshot.get() != null) {
                        try {
                            limitCommitAcquireForSnapshot.get().acquire();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    return snapshotCommitSupplier.acquireIndexCommitForSnapshot(engine);
                };

                EngineConfig customEngineConfig = EngineConfig.builder(engineConfig)
                    .snapshotCommitSupplier(customSnapshotCommitSupplier)
                    .build();

                return factory.newReadWriteEngine(customEngineConfig);
            });
        }
    }
}

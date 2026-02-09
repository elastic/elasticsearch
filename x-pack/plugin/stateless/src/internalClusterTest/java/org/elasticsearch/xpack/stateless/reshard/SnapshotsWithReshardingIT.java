/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class SnapshotsWithReshardingIT extends AbstractStatelessPluginIntegTestCase {
    public void testShardSnapshotIsFailedWhenReshardingMetadataIsPresent() throws Exception {
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

    public void testShardSnapshotIsFailedDueToConcurrentCompletedSplit() throws Exception {
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

    public void testSnapshotWithConcurrentSplit() throws Exception {
        startMasterOnlyNode();
        var nodes = startIndexNodes(3);
        startSearchNode();
        ensureStableCluster(5);

        createRepository("test-repo", "fs");

        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, 3, 1);
        ensureGreen();
        indexDocs(indexName, randomIntBetween(10, 100));

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
                assertTrue(searchResponse.getHits().getTotalHits().value() > 0);
            }
        );
    }

    // Tests that when we adjust index metadata we properly handle index metadata deduplication.
    public void testNextSnapshotHasCorrectMetadata() {
        var indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        createRepository("test-repo", "fs");

        final int shards = between(1, 10);
        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, shards, 0);
        ensureGreen();
        indexDocs(indexName, randomIntBetween(10, 100));

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
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(CustomSnapshotCommitSupplierPlugin.class);
        return plugins;
    }

    private void waitForReshardCompletion(Index index) {
        awaitClusterState((state) -> state.metadata().projectFor(index).index(index.getName()).getReshardingMetadata() == null);
    }

    public static class CustomSnapshotCommitSupplierPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        AtomicReference<CountDownLatch> blockCommitAcquireForSnapshot = new AtomicReference<>();
        AtomicReference<Semaphore> limitCommitAcquireForSnapshot = new AtomicReference<>();

        public CustomSnapshotCommitSupplierPlugin(Settings settings) {
            super(settings);
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

                EngineConfig customEngineConfig = new EngineConfig(
                    engineConfig.getShardId(),
                    engineConfig.getThreadPool(),
                    engineConfig.getThreadPoolMergeExecutorService(),
                    engineConfig.getIndexSettings(),
                    engineConfig.getWarmer(),
                    engineConfig.getStore(),
                    engineConfig.getMergePolicy(),
                    engineConfig.getAnalyzer(),
                    engineConfig.getSimilarity(),
                    engineConfig.getCodecProvider(),
                    engineConfig.getEventListener(),
                    engineConfig.getQueryCache(),
                    engineConfig.getQueryCachingPolicy(),
                    engineConfig.getTranslogConfig(),
                    engineConfig.getFlushMergesAfter(),
                    engineConfig.getExternalRefreshListener(),
                    engineConfig.getInternalRefreshListener(),
                    engineConfig.getIndexSort(),
                    engineConfig.getCircuitBreakerService(),
                    engineConfig.getGlobalCheckpointSupplier(),
                    engineConfig.retentionLeasesSupplier(),
                    engineConfig.getPrimaryTermSupplier(),
                    customSnapshotCommitSupplier,
                    engineConfig.getLeafSorter(),
                    engineConfig.getRelativeTimeInNanosSupplier(),
                    engineConfig.getIndexCommitListener(),
                    engineConfig.isPromotableToPrimary(),
                    engineConfig.getMapperService(),
                    engineConfig.getEngineResetLock(),
                    engineConfig.getMergeMetrics(),
                    engineConfig.getIndexDeletionPolicyWrapper()
                );

                return factory.newReadWriteEngine(customEngineConfig);
            });
        }
    }
}

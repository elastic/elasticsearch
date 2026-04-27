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

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepository;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessSnapshotIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(SnapshotCommitInterceptPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    public void testStatelessSnapshotReadsFromObjectStore() {
        final var indexNodeName = startMasterAndIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );

        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        indexAndMaybeFlush(indexName);

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final var snapshotReadSeen = new AtomicBoolean(false);
        setNodeRepositoryStrategy(indexNodeName, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    snapshotReadSeen.set(true);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName);
            }

            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    snapshotReadSeen.set(true);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        // 1. Stateless snapshot is disabled by default so that indices reads are from local primary shard
        // The object store should not see any read with SNAPSHOT_DATA operation purpose
        createSnapshot(repoName, "snap-1", List.of(indexName), List.of());
        assertFalse(snapshotReadSeen.get());

        // 2. Enable stateless snapshot and take another snapshot. The object store should see reads with SNAPSHOT_DATA operation purpose
        indexAndMaybeFlush(indexName);
        updateClusterSettings(Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store"));
        createSnapshot(repoName, "snap-2", List.of(indexName), List.of());
        assertTrue(snapshotReadSeen.get());

        // 3. Disable stateless snapshot and take yet another snapshot.
        // The object store should no longer see any new read with SNAPSHOT_DATA operation purpose
        snapshotReadSeen.set(false);
        indexAndMaybeFlush(indexName);
        updateClusterSettings(Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "disabled"));
        createSnapshot(repoName, "snap-3", List.of(indexName), List.of());
        assertFalse(snapshotReadSeen.get());
    }

    public void testStatelessSnapshotBasic() {
        final var settings = Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        ensureStableCluster(2);

        final String indexName = randomIdentifier();
        final int numberOfShards = between(1, 5);
        createIndex(indexName, numberOfShards, 1);
        ensureGreen(indexName);
        final var nDocs = indexAndMaybeFlush(indexName);

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var snapshotName = randomIdentifier();
        createSnapshot(repoName, snapshotName, List.of(indexName), List.of());

        safeGet(client().admin().indices().prepareDelete(indexName).execute());
        final var restoreSnapshotResponse = safeGet(
            client().admin()
                .cluster()
                .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .execute()
        );
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(numberOfShards));

        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName), nDocs);
    }

    public void testStatelessSnapshotDoesNotReadFromCache() {
        final var settings = Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build();
        final var indexNode = startMasterAndIndexNode(settings);

        final String indexName = randomIndexName();
        final int numberOfShards = between(1, 5);
        createIndex(indexName, numberOfShards, 0);
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        flush(indexName); // flush so that snapshot does not flush it and lead to cache activities

        final var repoName = randomRepoName();
        createRepository(repoName, "fs");

        // Force evict cache and gather metrics before snapshot so that we can assert that snapshot does not lead to cache activities
        final var indexShardBlobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(
            findIndexShard(indexName).store().directory()
        );
        getCacheService(indexShardBlobStoreCacheDirectory).forceEvict((key) -> true);
        final var testTelemetryPlugin = findPlugin(indexNode, TestTelemetryPlugin.class);
        testTelemetryPlugin.collect();
        long readsBeforeSnapshot = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.read.total").getLast().getLong();
        long missesBeforeSnapshot = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.miss.total").getLast().getLong();

        final var snapshotName = randomSnapshotName();
        createSnapshot(repoName, snapshotName, List.of(indexName), List.of());
        // Assert snapshot does not lead to any cache activities
        testTelemetryPlugin.collect();
        assertThat(
            testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.read.total").getLast().getLong(),
            equalTo(readsBeforeSnapshot)
        );
        assertThat(
            testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.miss.total").getLast().getLong(),
            equalTo(missesBeforeSnapshot)
        );
    }

    public void testSnapshotHollowShard() throws Exception {
        final Settings settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store")
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        final int nDocs = indexAndMaybeFlush(indexName);

        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, node0);
        assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(findIndexShard(indexName)), equalTo(true)));

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indexName);

        final var indexShard = findIndexShard(indexName);
        assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var snapshotInfo = createSnapshot(repoName, "snap-1", List.of(indexName), List.of());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        final String restoredIndexName = "restored-" + indexName;
        client().admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-1")
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndexName)
            .setWaitForCompletion(true)
            .get();

        final var indicesStatsResponse = safeGet(client().admin().indices().prepareStats(restoredIndexName).setDocs(true).execute());
        final long count = indicesStatsResponse.getIndices().get(restoredIndexName).getTotal().getDocs().getCount();
        assertThat(count, equalTo((long) nDocs));
    }

    public void testRelocationBeforeCommitAcquire() {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put("thread_pool.snapshot.max", 1)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);

        // Block the single SNAPSHOT thread on node0 so the shard snapshot task cannot start
        final var snapshotThreadBarrier = new CyclicBarrier(2);
        internalCluster().getInstance(ThreadPool.class, node0).executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            safeAwait(snapshotThreadBarrier);
            safeAwait(snapshotThreadBarrier);
        });
        safeAwait(snapshotThreadBarrier);

        // Start the snapshot — shard snapshot task is enqueued but cannot run
        final String snapshotName = randomSnapshotName();
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        // Wait for the master to assign the shard snapshot to node0 before triggering relocation;
        // otherwise the master may process the settings update first and assign the snapshot
        // directly to node1, never exercising the local-then-remote fallback this test covers.
        final var node0Id = getNodeId(node0);
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        awaitClusterState(state -> {
            final var entry = SnapshotsInProgress.get(state)
                .asStream()
                .filter(e -> e.snapshot().getSnapshotId().getName().equals(snapshotName))
                .findFirst()
                .orElse(null);
            if (entry == null) {
                return false;
            }
            final var status = entry.shards().get(shardId);
            return status != null && node0Id.equals(status.nodeId());
        });

        // Relocate shard to node1 while the SNAPSHOT pool on node0 is blocked
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), equalTo(Set.of(node1)));

        // Intercept the get_commit_info request on node1 to verify the fallback to remote
        final var interceptedOnNode1 = new CountDownLatch(1);
        MockTransportService.getInstance(node1)
            .addRequestHandlingBehavior(TransportGetShardSnapshotCommitInfoAction.SHARD_ACTION_NAME, (handler, request, channel, task) -> {
                interceptedOnNode1.countDown();
                handler.messageReceived(request, channel, task);
            });

        // Unblock SNAPSHOT pool — shard snapshot task runs, asyncCreate fails (shard gone),
        // falls back to TransportGetShardSnapshotCommitInfoAction to node1
        safeAwait(snapshotThreadBarrier);

        // Verify the request was sent to node1
        safeAwait(interceptedOnNode1);

        // Verify the snapshot completed successfully
        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        // Verify SnapshotsCommitService has no leftover tracking on any node
        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertFalse(commitService.hasTrackingForShard(shardId));
        }
    }

    public void testRelocationAfterCommitAcquire() {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store")
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);
        // Exercise dynamic settings update
        updateClusterSettings(
            Settings.builder()
                .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
                .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
        );
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);

        // Block object store reads on node0 — when a SNAPSHOT_DATA read is intercepted,
        // the commit must have already been acquired locally
        final var readIntercepted = new CountDownLatch(1);
        final var unblockRead = new CountDownLatch(1);
        setNodeRepositoryStrategy(node0, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    readIntercepted.countDown();
                    safeAwait(unblockRead);
                }
                return originalSupplier.get();
            }

            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    readIntercepted.countDown();
                    safeAwait(unblockRead);
                }
                return originalSupplier.get();
            }
        });

        // Start the snapshot
        final String snapshotName = randomSnapshotName();
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        // Wait for the read to be intercepted — commit is already acquired at this point
        safeAwait(readIntercepted);

        // Relocate shard to node1 while the snapshot is reading data from the object store
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), equalTo(Set.of(node1)));

        // Unblock the read — snapshot proceeds using the retained commit
        unblockRead.countDown();

        // Verify the snapshot completed successfully
        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        // Verify SnapshotsCommitService has no leftover tracking on any node
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertFalse(commitService.hasTrackingForShard(shardId));
        }
    }

    public void testRelocationDuringCommitAcquisition() {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);

        // Hook into node0's primary engine: after acquireIndexCommitForSnapshot returns, signal the test so that it
        // relocates the shard before the full commit registration can complete
        final var acquiredOnNode0 = new CountDownLatch(1);
        final var unblockNode0 = new CountDownLatch(1);
        final var interceptPlugin = findPlugin(node0, SnapshotCommitInterceptPlugin.class);
        interceptPlugin.afterAcquireForSnapshot.set(() -> {
            acquiredOnNode0.countDown();
            safeAwait(unblockNode0);
        });

        // Intercept get_commit_info request on node1 to verify the fallback to the new primary
        final var interceptedOnNode1 = new CountDownLatch(1);
        MockTransportService.getInstance(node1)
            .addRequestHandlingBehavior(TransportGetShardSnapshotCommitInfoAction.SHARD_ACTION_NAME, (handler, request, channel, task) -> {
                interceptedOnNode1.countDown();
                handler.messageReceived(request, channel, task);
            });

        // Start the snapshot
        final String snapshotName = randomSnapshotName();
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        // Wait for commit is acquired on node0, i.e. the shard snapshot started but has not complete
        // acquireAndMaybeRegisterCommitForSnapshot yet, e.g. blob locations not yet computed.
        safeAwait(acquiredOnNode0);
        interceptPlugin.afterAcquireForSnapshot.set(null);

        // Relocate the shard to node1 while the snapshot task on node0 is paused. Relocation completes (source shard
        // closes and unregisters) while node0 still holds the pre-register SnapshotIndexCommit.
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), equalTo(Set.of(node1)));

        // Unblock node0's snapshot task. When it proceeds, commit registration fails on node0 at shardStateId computation
        // and retries on node1
        unblockNode0.countDown();
        // Verify the retry reached node1
        safeAwait(interceptedOnNode1);

        // Verify the snapshot completed successfully
        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        // Verify SnapshotsCommitService has no leftover tracking on any node
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertFalse(commitService.hasTrackingForShard(shardId));
        }
    }

    public void testSnapshotFailsCleanlyWhenShardClosesDuringDisconnect() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // Allow master to remove the disconnected node quickly
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .build();
        final var masterNode = startMasterOnlyNode(settings);
        final var indexNodeA = startIndexNode(settings);
        final var indexNodeB = startIndexNode(settings);
        ensureStableCluster(3);

        final String indexName = randomIdentifier();
        // Force one shard per node so we can address each node's primary independently below.
        createIndex(indexName, indexSettings(2, 0).put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build());
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        flush(indexName);

        final var initialState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        final IndexRoutingTable indexRoutingTable = initialState.routingTable(ProjectId.DEFAULT).index(indexName);
        final var shard0NodeId = indexRoutingTable.shard(0).primaryShard().currentNodeId();
        assertThat(shard0NodeId, not(equalTo(indexRoutingTable.shard(1).primaryShard().currentNodeId()))); // one shard per node
        final var shard0Node = initialState.nodes().get(shard0NodeId).getName();
        final var shard1Node = shard0Node.equals(indexNodeA) ? indexNodeB : indexNodeA;

        final var repoName = randomRepoName();
        createRepository(repoName, "fs");

        // Block snapshots on initial metadata reads on each node so that we can exercise disconnection.
        final var readStrategy = new BlockingMetadataReadStrategy(2);
        setNodeRepositoryStrategy(shard0Node, readStrategy);
        setNodeRepositoryStrategy(shard1Node, readStrategy);
        final var snapshotFuture = client(masterNode).admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-during-disconnect")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(readStrategy.readObserved);

        // Shard snapshot blocked, disconnect node hosting shard0
        final var disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(masterNode, shard1Node), Set.of(shard0Node)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        ensureStableCluster(2, masterNode);

        // Reconnect the node, the shard snapshot should fail cleanly without AssertionError
        disruption.stopDisrupting();
        internalCluster().clearDisruptionScheme();
        ensureStableCluster(3);

        // Wait for the commit of shard0 to be released after re-join
        final var snapshotsCommitService = internalCluster().getInstance(SnapshotsCommitService.class, shard0Node);
        final var shardId0 = new ShardId(indexRoutingTable.getIndex(), 0);
        assertBusy(() -> assertFalse(snapshotsCommitService.hasTrackingForShard(shardId0)));
        readStrategy.proceed.countDown(); // resume the snapshot

        final var response = safeGet(snapshotFuture);
        assertThat(response.getSnapshotInfo(), notNullValue());
    }

    public void testSnapshotFailsCleanlyWhenIndexDeletedDuringMetadataRead() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .build();
        final var node = startMasterAndIndexNode(settings);

        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        flush(indexName);

        final var repoName = randomRepoName();
        createRepository(repoName, "fs");

        final var shardId0 = new ShardId(resolveIndex(indexName), 0);

        // Block the shard snapshot on its metadataSnapshot read from the object store
        final var readStrategy = new BlockingMetadataReadStrategy(1);
        setNodeRepositoryStrategy(node, readStrategy);

        final var snapshotFuture = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-during-index-delete")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(readStrategy.readObserved);

        // While the shard snapshot is blocked inside metadataSnapshot, delete the index. This aborts the in-progress
        // snapshot and closes the shard which drops the SnapshotIndexCommit's initial ref, allowing the backing
        // BCC blobs to become eligible for deletion. metadataSnapshot holds an extra commit ref for the duration of the read,
        // which prevents the cleanup and NoSuchFileException. The shard snapshot will then find out the abort by deletion
        // when it starts to read shard files.
        safeGet(client().admin().indices().prepareDelete(indexName).execute());

        final var snapshotsCommitService = internalCluster().getInstance(SnapshotsCommitService.class, node);
        assertBusy(() -> assertFalse(snapshotsCommitService.hasTrackingForShard(shardId0)));

        readStrategy.proceed.countDown();
        final var response = safeGet(snapshotFuture);
        assertThat(response.getSnapshotInfo(), notNullValue());
    }

    public void testSnapshotFailsCleanlyWhenIndexDeletedBeforeMetadataRead() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .build();
        final var node = startMasterAndIndexNode(settings);

        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        flush(indexName);

        // Use a mock-type snapshot repo so we can block on reads to it (the object store's mock repo is a separate instance).
        final var repoName = randomRepoName();
        createRepository(repoName, StatelessMockRepositoryPlugin.TYPE);

        // A first successful snapshot establishes a real shard generation; without one, the next snapshot hits the
        // NEW_SHARD_GEN fast path in buildBlobStoreIndexShardSnapshots and never reads from the snapshot repo before
        // metadataSnapshot.
        createSnapshot(repoName, "snap-initial", List.of(indexName), List.of());
        indexAndMaybeFlush(indexName);
        flush(indexName);

        final var shardId0 = new ShardId(resolveIndex(indexName), 0);

        // Block the snapshot-repo metadata read in buildBlobStoreIndexShardSnapshots, which runs before metadataSnapshot.
        // Deleting the index while blocked means that, when the read resumes, the commit is released and the shard snapshot
        // is aborted. Subsequent metadataSnapshot call must bail out and not try to read a deleted BCC.
        final var readStrategy = new BlockingMetadataReadStrategy(1);
        final var snapshotRepo = (StatelessMockRepository) internalCluster().getInstance(RepositoriesService.class, node)
            .repository(ProjectId.DEFAULT, repoName);
        snapshotRepo.setStrategy(readStrategy);

        // Guard on the object store to ensure it does not attempt to read a deleted BCC
        setNodeRepositoryStrategy(node, new AssertNoMissingMetadataBlobStrategy());

        // Start the snapshot, wait for the it to start
        final var snapshotFuture = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-before-metadata-read")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(readStrategy.readObserved);
        // Now delete the index to trigger commit release and shard snapshot abort
        safeGet(client().admin().indices().prepareDelete(indexName).execute());

        final var snapshotsCommitService = internalCluster().getInstance(SnapshotsCommitService.class, node);
        assertBusy(() -> assertFalse(snapshotsCommitService.hasTrackingForShard(shardId0)));

        readStrategy.proceed.countDown();
        final var response = safeGet(snapshotFuture);
        assertThat(response.getSnapshotInfo(), notNullValue());
    }

    public void testSnapshotFailsCleanlyWhenShardClosesDuringRestart() {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .build();
        final var masterNode = startMasterOnlyNode(settings);
        final var indexNode = startIndexNode(settings);
        ensureStableCluster(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        flush(indexName);

        final var repoName = randomRepoName();
        createRepository(repoName, "fs");

        // A single node hosts shard 0, so we need only one blocking read
        final var readStrategy = new BlockingMetadataReadStrategy(1);
        setNodeRepositoryStrategy(indexNode, readStrategy);

        // Issue snapshot request via master so that it is not interrupted
        final var snapshotFuture = client(masterNode).admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-during-restart")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(readStrategy.readObserved);

        // Stop the node and block it after IndicesService stopped so that we can resume the snapshot and ensure it fails cleanly
        final var afterStopObserved = new CountDownLatch(1);
        final var afterStopProceed = new CountDownLatch(1);
        internalCluster().getInstance(IndicesService.class, indexNode).addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStop() {
                afterStopObserved.countDown();
                safeAwait(afterStopProceed);
            }
        });
        final var snapshotsCommitService = internalCluster().getInstance(SnapshotsCommitService.class, indexNode);
        final var shardId0 = new ShardId(resolveIndex(indexName), 0);

        final var restartThread = new Thread(() -> {
            try {
                internalCluster().restartNode(indexNode);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, "test-restart-node");
        restartThread.start();

        safeAwait(afterStopObserved); // Wait for IndicesService to stop
        assertFalse(snapshotsCommitService.hasTrackingForShard(shardId0));
        readStrategy.proceed.countDown(); // resume the snapshot task
        final var response = safeGet(snapshotFuture);
        afterStopProceed.countDown(); // unblock IndicesService#stop so restart can continue
        safeJoin(restartThread);

        assertThat(response.getSnapshotInfo(), notNullValue());
    }

    /**
     * Asserts that any {@link OperationPurpose#SNAPSHOT_METADATA} read which reaches the underlying blob store does not
     * surface a {@link NoSuchFileException}. Installed on the object store, this catches the race where a concurrently
     * deleted index removes a BCC blob while the snapshot process is reading it.
     */
    private static class AssertNoMissingMetadataBlobStrategy extends StatelessMockRepositoryStrategy {
        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName
        ) throws IOException {
            try {
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName);
            } catch (NoSuchFileException e) {
                if (purpose == OperationPurpose.SNAPSHOT_METADATA) {
                    throw new AssertionError("unexpected NoSuchFileException for snapshot metadata blob [" + blobName + "]", e);
                }
                throw e;
            }
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName,
            long position,
            long length
        ) throws IOException {
            try {
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            } catch (NoSuchFileException e) {
                if (purpose == OperationPurpose.SNAPSHOT_METADATA) {
                    throw new AssertionError("unexpected NoSuchFileException for snapshot metadata blob [" + blobName + "]", e);
                }
                throw e;
            }
        }
    }

    private class BlockingMetadataReadStrategy extends AssertNoMissingMetadataBlobStrategy {
        final CountDownLatch readObserved;
        final CountDownLatch proceed = new CountDownLatch(1);

        BlockingMetadataReadStrategy(int numberOfReadsToBlock) {
            this.readObserved = new CountDownLatch(numberOfReadsToBlock);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName
        ) throws IOException {
            maybeBlock(purpose);
            return super.blobContainerReadBlob(originalSupplier, purpose, blobName);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName,
            long position,
            long length
        ) throws IOException {
            maybeBlock(purpose);
            return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
        }

        private void maybeBlock(OperationPurpose purpose) {
            // Initial operation for each shard on each node is single threaded so that we can be sure each node counts down once
            if (purpose == OperationPurpose.SNAPSHOT_METADATA && readObserved.getCount() > 0) {
                logger.info("--> read observed");
                readObserved.countDown();
                safeAwait(proceed);
            }
        }
    }

    private int indexAndMaybeFlush(String indexName) {
        final int nDocs = between(50, 100);
        indexDocs(indexName, nDocs);
        if (randomBoolean()) {
            flush(indexName);
        }
        return nDocs;
    }

    /**
     * Stateless plugin that wraps the engine's {@link IndexStorePlugin.SnapshotCommitSupplier} so tests can inject a runnable that
     * executes after the underlying {@code acquireIndexCommitForSnapshot} returns. Used to reliably reproduce races between commit
     * acquisition and shard relocation.
     */
    public static class SnapshotCommitInterceptPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        final AtomicReference<Runnable> afterAcquireForSnapshot = new AtomicReference<>();

        public SnapshotCommitInterceptPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return super.getEngineFactory(indexSettings).map(factory -> engineConfig -> {
                final var delegate = engineConfig.getSnapshotCommitSupplier();
                final IndexStorePlugin.SnapshotCommitSupplier wrappedCommitSupplier = engine -> {
                    final var commitRef = delegate.acquireIndexCommitForSnapshot(engine);
                    final var hook = afterAcquireForSnapshot.get();
                    if (hook != null) {
                        hook.run();
                    }
                    return commitRef;
                };
                final var wrappedConfig = new EngineConfig(
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
                    wrappedCommitSupplier,
                    engineConfig.getLeafSorter(),
                    engineConfig.getRelativeTimeInNanosSupplier(),
                    engineConfig.getIndexCommitListener(),
                    engineConfig.isPromotableToPrimary(),
                    engineConfig.getMapperService(),
                    engineConfig.getEngineResetLock(),
                    engineConfig.getMergeMetrics(),
                    engineConfig.getIndexDeletionPolicyWrapper()
                );
                return factory.newReadWriteEngine(wrappedConfig);
            });
        }
    }
}

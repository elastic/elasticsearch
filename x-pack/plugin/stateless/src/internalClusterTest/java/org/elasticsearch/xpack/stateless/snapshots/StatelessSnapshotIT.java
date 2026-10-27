/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.NodeShutdownTestUtils;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepository;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.TestStatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.RefreshManagerService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
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
        plugins.add(ShutdownPlugin.class);
        plugins.add(InternalSettingsPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    public void testStatelessSnapshotReadsFromObjectStore() throws IOException {
        // Create the node and index with disabled background refresh, flush and merge
        // so that we get expected number and layout for generated commits.
        final var indexNodeName = startMasterAndIndexNode(
            Settings.builder()
                .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .build()
        );
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build());
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
        updateClusterSettings(
            Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), randomFrom("read_from_object_store", "enabled"))
        );
        createSnapshot(repoName, "snap-2", List.of(indexName), List.of());
        assertTrue(snapshotReadSeen.get());

        // 3. Disable stateless snapshot and take yet another snapshot.
        // The object store should no longer see any new read with SNAPSHOT_DATA operation purpose
        snapshotReadSeen.set(false);
        indexAndMaybeFlush(indexName);
        updateClusterSettings(Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "disabled"));
        createSnapshot(repoName, "snap-3", List.of(indexName), List.of());
        assertFalse(snapshotReadSeen.get());

        // Verify the snapshots should have expected deduplication, i.e. the later snapshot should reference files from the earlier
        // ones without re-creating them. Note this assumes no merge happens between snapshots which is true in this test since it is
        // disabled when creating the index earlier.
        final var repositoriesService = internalCluster().getInstance(RepositoriesService.class, indexNodeName);
        final var repositoryData = safeAwait(
            (ActionListener<RepositoryData> l) -> repositoriesService.getRepositoryData(ProjectId.DEFAULT, repoName, l)
        );
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        final var repo = (BlobStoreRepository) repositoriesService.repository(ProjectId.DEFAULT, repoName);
        final var blobStoreIndexShardSnapshots = repo.getBlobStoreIndexShardSnapshots(
            indexId,
            0,
            repositoryData.shardGenerations().getShardGen(indexId, 0)
        );

        // Build a map of snapshot names to their corresponding file names, excluding the segment_N file since it is
        // unique to each snapshot due to new commit being created in between.
        final Map<String, List<String>> snapshotToFiles = blobStoreIndexShardSnapshots.snapshots()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    SnapshotFiles::snapshot,
                    snapshotFiles -> snapshotFiles.indexFiles()
                        .stream()
                        .map(BlobStoreIndexShardSnapshot.FileInfo::name)
                        .filter(
                            physicalName -> blobStoreIndexShardSnapshots.findNameFile(physicalName)
                                .metadata()
                                .name()
                                .startsWith(IndexFileNames.SEGMENTS + "_") == false
                        )
                        .collect(Collectors.toList())
                )
            );

        assertTrue(
            "unmatched snapshot files: " + snapshotToFiles,
            snapshotToFiles.get("snap-2").containsAll(snapshotToFiles.get("snap-1"))
        );
        assertTrue(
            "unmatched snapshot files: " + snapshotToFiles,
            snapshotToFiles.get("snap-3").containsAll(snapshotToFiles.get("snap-2"))
        );
    }

    public void testStatelessSnapshotBasic() throws Exception {
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

        // continuing indexing works as expected
        final var moreDocs = between(10, 50);
        indexDocsAndRefresh(indexName, moreDocs);
        assertHitCount(prepareSearch(indexName), nDocs + moreDocs);
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

    public void testRelocationBeforeCommitAcquire() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put("thread_pool.snapshot.max", 1)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        // indexA goes through the relocation+remote-fallback success path; indexB is deleted while blocked, so its
        // shard snapshot is aborted via SnapshotShardsService's cluster-state listener before its task runs.
        final var indices = createTwoIndicesExcluding(node1);

        // Install a strategy to ensure snapshot does not try to read missing blobs
        setNodeRepositoryStrategy(node0, new AssertNoMissingBlobStrategy());

        // Block the single SNAPSHOT thread on node0 so the shard snapshot tasks cannot start.
        final var snapshotThreadBarrier = new CyclicBarrier(2);
        internalCluster().getInstance(ThreadPool.class, node0).executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            safeAwait(snapshotThreadBarrier);
            safeAwait(snapshotThreadBarrier);
        });
        safeAwait(snapshotThreadBarrier);

        final var shardSnapshotAborted = observeShardSnapshotAborted(node0, repoName, indices.shardIdB());

        // Start the snapshot — shard snapshot tasks are enqueued but cannot run.
        final String snapshotName = randomSnapshotName();
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indices.indexA(), indices.indexB())
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();

        // Wait for the master to assign the shard snapshot to node0 before triggering relocation;
        // otherwise the master may process the settings update first and assign the snapshot
        // directly to node1, never exercising the local-then-remote fallback this test covers.
        final var node0Id = getNodeId(node0);
        awaitClusterState(state -> {
            final var entry = SnapshotsInProgress.get(state)
                .asStream()
                .filter(e -> e.snapshot().getSnapshotId().getName().equals(snapshotName))
                .findFirst()
                .orElse(null);
            if (entry == null) {
                return false;
            }
            // Checking one shard is enough since all shard entries are created with a single cluster state update
            final var status = entry.shards().get(indices.shardIdA);
            return status != null && node0Id.equals(status.nodeId());
        });

        // Relocate both shards to node1 while the SNAPSHOT pool on node0 is blocked.
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indices.indexA(), indices.indexB());
        assertThat(internalCluster().nodesInclude(indices.indexA()), equalTo(Set.of(node1)));
        assertThat(internalCluster().nodesInclude(indices.indexB()), equalTo(Set.of(node1)));

        // Delete indexB while still blocked. This aborts its shard snapshot status on node0 via the cluster-state
        // listener while the primary is now on node1.
        safeGet(client().admin().indices().prepareDelete(indices.indexB()).execute());

        // Intercept the get_commit_info request on node1 — only indexA reaches the fallback (indexB short-circuits
        // at the first ensureNotAborted in snapshot()).
        final var interceptedOnNode1 = new CountDownLatch(1);
        MockTransportService.getInstance(node1)
            .addRequestHandlingBehavior(TransportGetShardSnapshotCommitInfoAction.SHARD_ACTION_NAME, (handler, request, channel, task) -> {
                final var getShardSnapshotCommitInfoRequest = (GetShardSnapshotCommitInfoRequest) request;
                assertThat(getShardSnapshotCommitInfoRequest.shardId().getIndexName(), equalTo(indices.indexA()));
                interceptedOnNode1.countDown();
                handler.messageReceived(request, channel, task);
            });

        // Unblock the SNAPSHOT pool. indexA's asyncCreate fails locally and falls back to TransportGetShardSnapshotCommitInfoAction
        // on node1. indexB's task throws AbortedSnapshotException due to index deletion
        safeAwait(snapshotThreadBarrier);

        safeAwait(interceptedOnNode1);
        safeAwait(shardSnapshotAborted);

        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.totalShards(), equalTo(1));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        // Verify SnapshotsCommitService has no leftover tracking on any node
        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(indices.shardIdA())));
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(indices.shardIdB())));
        }
    }

    public void testRelocationAfterCommitAcquire() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store")
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // Ensure both shards' snapshot tasks can run and block concurrently
            .put("thread_pool.snapshot.max", 2)
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

        // indexA goes through the success path (reads from the object store after relocation);
        // indexB is deleted mid-read so its shard snapshot is aborted.
        final var indices = createTwoIndicesExcluding(node1);

        // Block object store reads on node0 — when a SNAPSHOT_DATA read is intercepted, both shards' commits have
        // already been acquired locally.
        final var readIntercepted = new CountDownLatch(2); // expect 2 reads each from one shard snapshot
        final var unblockRead = new CountDownLatch(1);
        setNodeRepositoryStrategy(node0, new AssertNoMissingBlobStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    readIntercepted.countDown();
                    if (unblockRead.getCount() > 0) {
                        logger.info("--> blocking snapshot data read for [{}]", blobName);
                    }
                    safeAwait(unblockRead);
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
                    readIntercepted.countDown();
                    if (unblockRead.getCount() > 0) {
                        logger.info(
                            "--> blocking snapshot data read for [{}] at position [{}] with length [{}]",
                            blobName,
                            position,
                            length
                        );
                    }
                    safeAwait(unblockRead);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        // Commits are acquired before index deletion so that we should not see any remote get commit info request
        MockTransportService.getInstance(node1)
            .addRequestHandlingBehavior(TransportGetShardSnapshotCommitInfoAction.SHARD_ACTION_NAME, (handler, request, channel, task) -> {
                throw new AssertionError("unexpected get_commit_info request on node1");
            });

        final var shardSnapshotAborted = observeShardSnapshotAborted(node0, repoName, indices.shardIdB());

        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, randomSnapshotName())
            .setIndices(indices.indexA(), indices.indexB())
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();

        safeAwait(readIntercepted);

        // Relocate both shards to node1 while the snapshot is reading data from the object store.
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indices.indexA(), indices.indexB());
        assertThat(internalCluster().nodesInclude(indices.indexA()), equalTo(Set.of(node1)));
        assertThat(internalCluster().nodesInclude(indices.indexB()), equalTo(Set.of(node1)));

        safeGet(client().admin().indices().prepareDelete(indices.indexB()).execute());

        // Unblock the reads. indexA's snapshot proceeds using the retained commit after relocation. indexB's task encounters the
        // abort at the next per-file ensureNotAborted check and fails as aborted due to index deletion.
        unblockRead.countDown();

        safeAwait(shardSnapshotAborted);

        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.totalShards(), equalTo(1));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(indices.shardIdA())));
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(indices.shardIdB())));
        }
    }

    public void testCommitReleasedPromptlyOnRelocation() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put("thread_pool.snapshot.max", 1) // single thread for controlled test
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);

        final var bufferSize = BlobStoreRepository.BUFFER_SIZE_SETTING.get(Settings.EMPTY);
        // Create a large segment so that the file is large enough (> default 128KB buffer size) so that they require multiple reads
        indexDocs(indexName, 500, UnaryOperator.identity(), null, () -> Map.of("field", randomUnicodeOfCodepointLength(1024)));
        flush(indexName);

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final var shardId = new ShardId(resolveIndex(indexName), 0);

        // Block snapshot data reads twice so that the 2nd one has a chance to release the commit
        final var blockingCount = new AtomicInteger(2);
        final var dataReadProceedBarrier = new CyclicBarrier(2);

        setNodeRepositoryStrategy(node0, new AssertNoMissingBlobStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                final var original = super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
                if (purpose == OperationPurpose.SNAPSHOT_DATA && length > bufferSize.getBytes()) {
                    return new FilterInputStream(original) {
                        @Override
                        public int read() throws IOException {
                            maybeBlockRead();
                            return super.read();
                        }

                        @Override
                        public int read(byte[] b, int off, int len) throws IOException {
                            maybeBlockRead();
                            return super.read(b, off, len);
                        }

                        private void maybeBlockRead() {
                            if (blockingCount.decrementAndGet() >= 0) {
                                logger.info("--> blocking snapshot data read");
                                safeAwait(dataReadProceedBarrier);
                                safeAwait(dataReadProceedBarrier);
                                logger.info("--> proceeding with snapshot data read");
                            }
                        }
                    };
                }
                return original;
            }
        });

        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, randomSnapshotName())
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        // Wait till the snapshot read is blocked the first time
        safeAwait(dataReadProceedBarrier);

        logger.info("--> relocating to [{}]", node1);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), equalTo(Set.of(node1)));

        // The tracked commit is released by SnapshotsCommitService due to relocation
        final var snapshotCommitService = internalCluster().getInstance(SnapshotsCommitService.class, node0);
        assertBusy(() -> assertFalse(snapshotCommitService.hasTrackingForShard(shardId)));

        // Let the first snapshot data read proceed, it should release the commit on detecting relocation and keep reading.
        safeAwait(dataReadProceedBarrier);
        // Wait for it to block the 2nd time.
        safeAwait(dataReadProceedBarrier);

        // Verify the shard can relocate back, i.e. the store is closed because the commit is fully released
        logger.info("--> relocating back to [{}]", node0);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node1));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), equalTo(Set.of(node0)));

        // Unblock data reads and let the snapshot complete
        safeAwait(dataReadProceedBarrier);

        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(shardId)));
        }
    }

    public void testRelocationDuringCommitAcquisition() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // Ensure both shards' snapshot tasks can run and block concurrently
            .put("thread_pool.snapshot.max", 2)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        // indexA stays in the snapshot through the relocation; indexB is deleted while block at commit acquisition..
        final var indices = createTwoIndicesExcluding(node1);

        // Install a strategy to ensure snapshot does not try to read missing blobs
        setNodeRepositoryStrategy(node0, new AssertNoMissingBlobStrategy());

        // Per-shard hook into node0's primary engine: after acquireIndexCommitForSnapshot returns, signal the test
        // and pause until released.
        final var acquiredA = new CountDownLatch(1);
        final var acquiredB = new CountDownLatch(1);
        final var unblockA = new CountDownLatch(1);
        final var unblockB = new CountDownLatch(1);
        final var interceptPlugin = findPlugin(node0, SnapshotCommitInterceptPlugin.class);
        interceptPlugin.afterAcquireForSnapshot.put(indices.shardIdA(), () -> {
            acquiredA.countDown();
            safeAwait(unblockA);
        });
        interceptPlugin.afterAcquireForSnapshot.put(indices.shardIdB(), () -> {
            acquiredB.countDown();
            safeAwait(unblockB);
        });

        // Intercept get_commit_info request on node1 — only indexA reaches the fallback. indexB's shard snapshot fails
        // due to index deletion (IndexNotFoundException) before it can retry on remote node.
        final var interceptedOnNode1 = new CountDownLatch(1);
        MockTransportService.getInstance(node1)
            .addRequestHandlingBehavior(TransportGetShardSnapshotCommitInfoAction.SHARD_ACTION_NAME, (handler, request, channel, task) -> {
                final var getShardSnapshotCommitInfoRequest = (GetShardSnapshotCommitInfoRequest) request;
                assertThat(getShardSnapshotCommitInfoRequest.shardId().getIndexName(), equalTo(indices.indexA()));
                interceptedOnNode1.countDown();
                handler.messageReceived(request, channel, task);
            });

        final var shardSnapshotFailed = observeShardSnapshotFailed(node0, repoName, indices.shardIdB());

        // Start the snapshot covering both indices. Partial so master allows index deletion to proceed concurrently.
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, randomSnapshotName())
            .setIndices(indices.indexA(), indices.indexB())
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();

        // Both shards' commits are acquired on node0 and the tasks blocked inside the hook.
        safeAwait(acquiredA);
        safeAwait(acquiredB);
        interceptPlugin.afterAcquireForSnapshot.remove(indices.shardIdA());
        interceptPlugin.afterAcquireForSnapshot.remove(indices.shardIdB());

        // Relocate both shards to node1; the source shards close and release the commits while they are retained again on node1.
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indices.indexA(), indices.indexB());
        assertThat(internalCluster().nodesInclude(indices.indexA()), equalTo(Set.of(node1)));
        assertThat(internalCluster().nodesInclude(indices.indexB()), equalTo(Set.of(node1)));

        // Delete indexB while node0's snapshot task is still blocked.
        safeGet(client().admin().indices().prepareDelete(indices.indexB()).execute());

        // Unblock both tasks. indexA's task retries on node1; indexB shard snapshot fails
        unblockA.countDown();
        unblockB.countDown();

        safeAwait(interceptedOnNode1);
        safeAwait(shardSnapshotFailed);

        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.totalShards(), equalTo(1));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(indices.shardIdA())));
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(indices.shardIdB())));
        }
    }

    public void testRelocationDuringConcurrentFileSnapshotsReleasesCommitCleanly() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // Allow at least two FileSnapshotTask instances to run concurrently so multiple fileReader performs inc-ref
            .put("thread_pool.snapshot.max", 2)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        // Create large segments so that we can differentiate them from the smaller .si and segments_N files and block their reading
        for (int i = 0; i < 4; i++) {
            indexDocs(
                indexName,
                between(50, 100),
                UnaryOperator.identity(),
                null,
                () -> Map.of("field", randomUnicodeOfCodepointLength(500))
            );
            refresh(indexName);
        }
        final var shardId = new ShardId(resolveIndex(indexName), 0);

        final long inlineHashFileSizeThreshold = 1024L; // size hint to allow read for .si and segments_N to pass
        final var readIntercepted = new CountDownLatch(2);
        final var unblockRead = new CountDownLatch(1);
        setNodeRepositoryStrategy(node0, new AssertNoMissingBlobStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA && length > inlineHashFileSizeThreshold) {
                    readIntercepted.countDown();
                    safeAwait(unblockRead);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, randomSnapshotName())
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        // Wait until two FileSnapshotTasks are blocked on reading, which happens after they each inc-ref the commit
        safeAwait(readIntercepted);

        // Relocate the shard which releases the commit tracked by SnapshotsCommitService. It should not trigger any AssertionError
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), equalTo(Set.of(node1)));

        // Unblock the FileSnapshotTasks and snapshot should complete successfully
        unblockRead.countDown();

        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.totalShards(), equalTo(1));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(shardId)));
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

        // Block the shard snapshot on its store metadataSnapshot read from the object store.
        final var readStrategy = new BlockingMetadataReadStrategy(1);
        setNodeRepositoryStrategy(node, readStrategy);

        final var shardSnapshotAborted = observeShardSnapshotAborted(node, repoName, shardId0);

        final var snapshotFuture = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-during-index-delete")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(readStrategy.readObserved);

        // Delete the index while the shard snapshot is blocked mid-metadataSnapshot. This should trigger shard snapshot abort.
        safeGet(client().admin().indices().prepareDelete(indexName).execute());

        // Index deletion itself does not release the snapshot commit tracking when supportsRelocationDuringSnapshot is true
        final var snapshotsCommitService = internalCluster().getInstance(SnapshotsCommitService.class, node);
        assertTrue(snapshotsCommitService.hasTrackingForShard(shardId0));

        readStrategy.proceed.countDown();
        // Shard snapshot should abort and fail
        safeAwait(shardSnapshotAborted);
        safeGet(snapshotFuture);
        // Retained commit tracking is released by SnapshotsCommitService.clusterChanged once the snapshot is completed.
        assertBusy(() -> assertFalse(snapshotsCommitService.hasTrackingForShard(shardId0)));
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

        // Use a mock-type snapshot repo (separate instance from the object store mock) so we can block reads on it.
        final var repoName = randomRepoName();
        createRepository(repoName, StatelessMockRepositoryPlugin.TYPE);

        // A prior snapshot establishes a real shard generation; otherwise the next one hits the NEW_SHARD_GEN fast
        // path in buildBlobStoreIndexShardSnapshots and doesn't touch the snapshot repo before metadataSnapshot.
        createSnapshot(repoName, "snap-initial", List.of(indexName), List.of());
        indexAndMaybeFlush(indexName);
        flush(indexName);

        final var shardId0 = new ShardId(resolveIndex(indexName), 0);

        // Block the snapshot-repo metadata read in buildBlobStoreIndexShardSnapshots, which runs before metadataSnapshot.
        final var readStrategy = new BlockingMetadataReadStrategy(1);
        final var snapshotRepo = (StatelessMockRepository) internalCluster().getInstance(RepositoriesService.class, node)
            .repository(ProjectId.DEFAULT, repoName);
        snapshotRepo.setStrategy(readStrategy);

        // Fail the test if the object store ever receives a SNAPSHOT_METADATA read on a deleted BCC.
        setNodeRepositoryStrategy(node, new AssertNoMissingBlobStrategy());

        final var shardSnapshotAborted = observeShardSnapshotAborted(node, repoName, shardId0);

        final var snapshotFuture = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-before-metadata-read")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(readStrategy.readObserved);
        // Delete the index while the shard snapshot is blocked before metadataSnapshot. This should trigger snapshot abort.
        safeGet(client().admin().indices().prepareDelete(indexName).execute());

        // Index deletion itself does not release the snapshot commit tracking when supportsRelocationDuringSnapshot is true
        final var snapshotsCommitService = internalCluster().getInstance(SnapshotsCommitService.class, node);
        assertTrue(snapshotsCommitService.hasTrackingForShard(shardId0));

        readStrategy.proceed.countDown();
        // Shard snapshot should abort and fail
        safeAwait(shardSnapshotAborted);
        safeGet(snapshotFuture);
        // Retained commit tracking is released by SnapshotsCommitService.clusterChanged once the snapshot is done.
        assertBusy(() -> assertFalse(snapshotsCommitService.hasTrackingForShard(shardId0)));
    }

    public void testStaleNodePausedNotificationDoesNotAffectCommitOnNewNode() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put("thread_pool.snapshot.max", 1) // single thread for controlled test
            .build();

        startMasterOnlyNode(settings);
        startMasterOnlyNode(settings);
        final var indexNodeA = startIndexNode(settings);
        ensureStableCluster(3);
        // Create the index to have its shard land on the only data node
        final String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        // Add another data node for pause/resume shard snapshot
        final var indexNodeB = startIndexNode(settings);
        ensureStableCluster(4);

        final var repoName = randomRepoName();
        createRepository(repoName, "fs");

        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var indexNodeAId = getNodeId(indexNodeA);
        final var indexNodeBId = getNodeId(indexNodeB);

        // Block initial metadata read on indexNodeA so its snapshot is mid-flight when we mark the node for shutdown.
        final var blockOnA = new BlockingMetadataReadStrategy(1);
        setNodeRepositoryStrategy(indexNodeA, blockOnA);

        // Also block snapshot read on indexNodeB to wait for master failover
        final var unblockB = new CountDownLatch(1);
        final var blockedOnB = new CountDownLatch(1);
        setNodeRepositoryStrategy(indexNodeB, new AssertNoMissingBlobStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    if (blockedOnB.getCount() > 0) {
                        blockedOnB.countDown();
                    }
                    safeAwait(unblockB);
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
                    if (blockedOnB.getCount() > 0) {
                        blockedOnB.countDown();
                    }
                    safeAwait(unblockB);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        // Start snapshot, wait for it to block on indexNodeA and mark the node for shutdown
        final var snapshotName = "snap-stale-paused";
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(blockOnA.readObserved);
        final var currentMasterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        NodeShutdownTestUtils.putShutdownMetadata(
            indexNodeA,
            currentMasterClusterService,
            EnumSet.of(SingleNodeShutdownMetadata.Type.SIGTERM)
        );

        // Wait for master to know about the shutdown which means indexNodeA has paused it
        awaitClusterState(state -> SnapshotsInProgress.get(state).isNodeIdForRemoval(indexNodeAId));

        // Ensure master record the shard as PAUSED_FOR_NODE_REMOVAL on indexNodeA.
        final var shardSnapshotPausedListener = ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            final var entry = SnapshotsInProgress.get(state)
                .asStream()
                .filter(e -> snapshotName.equals(e.snapshot().getSnapshotId().getName()))
                .findFirst()
                .orElse(null);
            if (entry == null) {
                return false;
            }
            final var status = entry.shards().get(shardId);
            return status != null
                && status.state() == SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL
                && indexNodeAId.equals(status.nodeId());
        });

        // Ensure master reassign the shard snapshot to indexNodeB
        final var shardSnapshotReassignedListener = ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            final var entry = SnapshotsInProgress.get(state)
                .asStream()
                .filter(e -> snapshotName.equals(e.snapshot().getSnapshotId().getName()))
                .findFirst()
                .orElse(null);
            if (entry == null) {
                return false;
            }
            final var status = entry.shards().get(shardId);
            return status != null && status.state() == SnapshotsInProgress.ShardState.INIT && indexNodeBId.equals(status.nodeId());
        });

        // Allow indexNodeA's blocked read to proceed and it should notice the pause, update master etc
        blockOnA.proceed.countDown();

        safeAwait(shardSnapshotPausedListener);
        safeAwait(shardSnapshotReassignedListener);

        // Wait for indexNodeB's snapshot to acquire the commit and is reading data
        safeAwait(blockedOnB);
        final var commitServiceB = internalCluster().getInstance(SnapshotsCommitService.class, indexNodeB);
        assertTrue(commitServiceB.hasTrackingForShard(shardId));

        // Master failover so that data node re-sync with the new master
        final var oldMasterName = internalCluster().getMasterName();
        safeAwait(
            (ActionListener<Void> l) -> NodeShutdownTestUtils.putShutdownMetadata(
                currentMasterClusterService,
                SingleNodeShutdownMetadata.builder()
                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                    .setStartedAtMillis(currentMasterClusterService.threadPool().absoluteTimeInMillis())
                    .setReason("test")
                    .setGracePeriod(TimeValue.timeValueSeconds(60)),
                oldMasterName,
                l
            )
        );
        awaitMasterNode(); // Ensure new master takes over
        assertThat(internalCluster().getMasterName(), not(equalTo(oldMasterName)));

        // Even though the API request fails due to no-longer-master, internally the snapshot should complete successfully
        final var snapshotCompletedListener = ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            final var snapshotEntry = SnapshotsInProgress.get(state)
                .asStream()
                .filter(e -> snapshotName.equals(e.snapshot().getSnapshotId().getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("snapshot not found"));
            if (snapshotEntry.state().completed()) {
                assertThat(snapshotEntry.state(), is(SnapshotsInProgress.State.SUCCESS));
                assertThat(snapshotEntry.shards().get(shardId).state(), is(SnapshotsInProgress.ShardState.SUCCESS));
                return true;
            } else {
                return false;
            }
        });

        // Let snapshot continue on indexNodeB
        unblockB.countDown();

        // Request encounters not-master exception but the snapshot completes internally
        assertThat(
            expectThrows(SnapshotException.class, () -> snapshotFuture.actionGet(TEST_REQUEST_TIMEOUT)).getMessage(),
            containsString("no longer master")
        );
        safeAwait(snapshotCompletedListener);
        awaitClusterState(indexNodeB, state -> SnapshotsInProgress.get(state).count() == 0);

        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(shardId)));
        }
    }

    public void testRelocationBetweenInitialCommitAcquisitionAndRegistration() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put("thread_pool.snapshot.max", 1)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        final var shardId = new ShardId(resolveIndex(indexName), 0);

        setNodeRepositoryStrategy(node0, new AssertNoMissingBlobStrategy());

        // Block inside getLastSyncedGlobalCheckpoint after it returns a value — this is the point where
        // SnapshotShardContextHelper.acquireSnapshotIndexCommit has progressed far enough so that its caller
        // SnapshotsCommmitService.acquireAndMaybeRegisterCommitForSnapshot will be able to call
        // withSnapshotIndexCommitRef, which should detect and handle the relocated shard and see no error.
        final var checkpointCalled = new CountDownLatch(1);
        final var unblockAfterCheckpoint = new CountDownLatch(1);
        final var interceptPlugin = findPlugin(node0, SnapshotCommitInterceptPlugin.class);
        interceptPlugin.afterGetLastSyncedGlobalCheckpoint.put(shardId, () -> {
            checkpointCalled.countDown();
            safeAwait(unblockAfterCheckpoint);
        });

        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, randomSnapshotName())
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        safeAwait(checkpointCalled);
        interceptPlugin.afterGetLastSyncedGlobalCheckpoint.remove(shardId);

        // Relocate the shard before SnapshotsCommmitService can invoke withSnapshotIndexCommitRef
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), equalTo(Set.of(node1)));

        // Resume the snapshot and it should succeed
        unblockAfterCheckpoint.countDown();

        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        for (SnapshotsCommitService commitService : internalCluster().getInstances(SnapshotsCommitService.class)) {
            assertBusy(() -> assertFalse(commitService.hasTrackingForShard(shardId)));
        }
    }

    public void testConcurrentSnapshotAndRelocationWithHollowing() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put("thread_pool.snapshot.max", 1)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);

        final var repoName = randomRepoName();
        createRepository(repoName, "fs");

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        flush(indexName);

        // Block BCC upload for the hollow commit flushed by relocation
        final var hollowBccUploadStarted = new CountDownLatch(1);
        final var blockBccUpload = new CountDownLatch(1);
        setNodeRepositoryStrategy(node0, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (purpose == OperationPurpose.INDICES && blobName.startsWith("stateless_commit_")) {
                    hollowBccUploadStarted.countDown();
                    safeAwait(blockBccUpload);
                    super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                } else {
                    super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                }
            }
        });

        // Block the single SNAPSHOT thread on node0 so the shard snapshot task is queued
        final var snapshotThreadBarrier = new CyclicBarrier(2);
        internalCluster().getInstance(ThreadPool.class, node0).executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            safeAwait(snapshotThreadBarrier);
            safeAwait(snapshotThreadBarrier);
        });
        safeAwait(snapshotThreadBarrier);

        // Start the snapshot. The shard snapshot task will be queued
        final String snapshotName = randomSnapshotName();
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        // Wait for the master to assign the shard snapshot to node0 before triggering relocation.
        awaitSnapshotShardAssignedToNode(snapshotName, indexName, getNodeId(node0));

        // Trigger relocation which flushes a hollow commit
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        // Wait until the hollow BCC upload has started and blocked
        safeAwait(hollowBccUploadStarted);

        // Unblock the shard snapshot
        safeAwait(snapshotThreadBarrier);

        // Sometimes stall the BCC upload a bit more to ensure the shard snapshot task wait for its completion and does not fail with
        // NoSuchFileException for the not-yet-uploaded hollow commit.
        if (randomBoolean()) {
            safeSleep(between(10, 50));
        }
        blockBccUpload.countDown();

        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
    }

    public void testSnapshotRetryOnNewNodeOnRelocationAndHollowing() throws Exception {
        final var settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put("thread_pool.snapshot.max", 1)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);

        final var repoName = randomRepoName();
        createRepository(repoName, "fs");

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        indexAndMaybeFlush(indexName);
        flush(indexName);

        final var indexShard = findIndexShard(indexName);
        final var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final var commitService = (TestStatelessCommitService) indexEngine.getStatelessCommitService();

        // Stall the relocation after the shard's commit state is marked as relocated. This is after the hollowing but before the
        // shard instance is removed from IndicesService
        final CyclicBarrier afterRelocatedBarrier = new CyclicBarrier(2);
        commitService.setStrategy(new TestStatelessCommitService.Strategy() {
            @Override
            public ActionListener<Void> markRelocating(
                Supplier<ActionListener<Void>> originalSupplier,
                ShardId sid,
                long minRelocatedGeneration,
                ActionListener<Void> listener
            ) {
                return originalSupplier.get().delegateFailure((l, ignored) -> {
                    l.onResponse(null); // mark relocated
                    safeAwait(afterRelocatedBarrier);
                    safeAwait(afterRelocatedBarrier);
                });
            }
        });

        // Block the single SNAPSHOT thread on node0 so the shard snapshot task is queued but not yet executed.
        final var snapshotThreadBarrier = new CyclicBarrier(2);
        internalCluster().getInstance(ThreadPool.class, node0).executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            safeAwait(snapshotThreadBarrier);
            safeAwait(snapshotThreadBarrier);
        });
        safeAwait(snapshotThreadBarrier);
        final String snapshotName = randomSnapshotName();
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();
        // Wait for the master to assign the shard snapshot to node0 before triggering relocation.
        awaitSnapshotShardAssignedToNode(snapshotName, indexName, getNodeId(node0));

        // Intercept the retry request on node1 to verify the retry behaviour
        final var retriedOnNode1 = new CountDownLatch(1);
        MockTransportService.getInstance(node1)
            .addRequestHandlingBehavior(TransportGetShardSnapshotCommitInfoAction.SHARD_ACTION_NAME, (handler, request, channel, task) -> {
                assertThat(((GetShardSnapshotCommitInfoRequest) request).shardId().getIndexName(), equalTo(indexName));
                retriedOnNode1.countDown();
                handler.messageReceived(request, channel, task);
            });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        safeAwait(afterRelocatedBarrier); // Ensure the relocation progress to markRelocated()
        safeAwait(afterRelocatedBarrier); // Unblock it to race with the snapshot
        logger.info("--> unblock snapshot thread after markRelocated");
        safeAwait(snapshotThreadBarrier);

        safeAwait(retriedOnNode1);
        final var snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(1));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
    }

    private void awaitSnapshotShardAssignedToNode(String snapshotName, String indexName, String nodeId) throws Exception {
        awaitClusterState(state -> {
            final var entry = SnapshotsInProgress.get(state)
                .asStream()
                .filter(e -> e.snapshot().getSnapshotId().getName().equals(snapshotName))
                .findFirst()
                .orElse(null);
            if (entry == null) {
                return false;
            }
            final var shardStatus = entry.shards()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().getIndexName().equals(indexName))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
            if (shardStatus != null) {
                assertThat(shardStatus.nodeId(), equalTo(nodeId));
                return true;
            }
            return false;
        });
    }

    private SubscribableListener<Void> observeShardSnapshotAborted(String node, String repoName, ShardId shardId) {
        return ClusterServiceUtils.addTemporaryStateListener(internalCluster().getInstance(ClusterService.class, node), state -> {
            final var shardStatus = SnapshotsInProgress.get(state)
                .forRepo(ProjectId.DEFAULT, repoName)
                .stream()
                .findFirst()
                .map(entry -> entry.shards().get(shardId))
                .orElse(null);
            return shardStatus != null
                && shardStatus.state() == SnapshotsInProgress.ShardState.FAILED
                && "aborted".equals(shardStatus.reason());
        });
    }

    /** Completes when {@code shardId} reaches {@code FAILED} (with any reason) in the given repo's entry. */
    private SubscribableListener<Void> observeShardSnapshotFailed(String node, String repoName, ShardId shardId) {
        return ClusterServiceUtils.addTemporaryStateListener(internalCluster().getInstance(ClusterService.class, node), state -> {
            final var shardStatus = SnapshotsInProgress.get(state)
                .forRepo(ProjectId.DEFAULT, repoName)
                .stream()
                .findFirst()
                .map(entry -> entry.shards().get(shardId))
                .orElse(null);
            return shardStatus != null && shardStatus.state() == SnapshotsInProgress.ShardState.FAILED;
        });
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
     * Asserts that any snapshot metadata or data read which reaches the underlying blob store does not
     * surface a {@link NoSuchFileException}. Installed on the object store, this catches the race where a concurrently
     * deleted index removes a BCC blob while the snapshot process is reading it.
     */
    private static class AssertNoMissingBlobStrategy extends StatelessMockRepositoryStrategy {
        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName
        ) throws IOException {
            try {
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName);
            } catch (NoSuchFileException e) {
                if (purpose == OperationPurpose.SNAPSHOT_METADATA || purpose == OperationPurpose.SNAPSHOT_DATA) {
                    throw new AssertionError("unexpected NoSuchFileException for snapshot blob [" + blobName + "]", e);
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
                if (purpose == OperationPurpose.SNAPSHOT_METADATA || purpose == OperationPurpose.SNAPSHOT_DATA) {
                    throw new AssertionError("unexpected NoSuchFileException for snapshot blob [" + blobName + "]", e);
                }
                throw e;
            }
        }
    }

    private class BlockingMetadataReadStrategy extends AssertNoMissingBlobStrategy {
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

    private record IndexPair(String indexA, String indexB, ShardId shardIdA, ShardId shardIdB) {}

    /** Creates two single-shard indices whose primaries are kept off {@code excludeNode}, indexes some docs, and
     *  resolves the corresponding {@link ShardId}s. */
    private IndexPair createTwoIndicesExcluding(String excludeNode) {
        final var indexA = randomIdentifier("a");
        final var indexB = randomIdentifier("b");
        createIndex(indexA, indexSettings(1, 0).put("index.routing.allocation.exclude._name", excludeNode).build());
        createIndex(indexB, indexSettings(1, 0).put("index.routing.allocation.exclude._name", excludeNode).build());
        ensureGreen(indexA, indexB);
        indexAndMaybeFlush(indexA);
        indexAndMaybeFlush(indexB);
        return new IndexPair(indexA, indexB, new ShardId(resolveIndex(indexA), 0), new ShardId(resolveIndex(indexB), 0));
    }

    /**
     * Stateless plugin that wraps the engine's {@link IndexStorePlugin.SnapshotCommitSupplier} so tests can inject a runnable that
     * executes after the underlying {@code acquireIndexCommitForSnapshot} returns, and also overrides {@code newIndexEngine} to allow
     * injection of a runnable that executes after {@code getLastSyncedGlobalCheckpoint} returns. Used to reliably reproduce races
     * between commit acquisition and shard relocation.
     */
    public static class SnapshotCommitInterceptPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        final Map<ShardId, Runnable> afterAcquireForSnapshot = new ConcurrentHashMap<>();
        final Map<ShardId, Runnable> afterGetLastSyncedGlobalCheckpoint = new ConcurrentHashMap<>();

        public SnapshotCommitInterceptPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(Plugin.PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new TestStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return super.getEngineFactory(indexSettings).map(factory -> engineConfig -> {
                final var delegate = engineConfig.getSnapshotCommitSupplier();
                final var shardId = engineConfig.getShardId();
                final IndexStorePlugin.SnapshotCommitSupplier wrappedCommitSupplier = engine -> {
                    final var commitRef = delegate.acquireIndexCommitForSnapshot(engine);
                    final var hook = afterAcquireForSnapshot.get(shardId);
                    if (hook != null) {
                        hook.run();
                    }
                    return commitRef;
                };
                final var wrappedConfig = EngineConfig.builder(engineConfig).snapshotCommitSupplier(wrappedCommitSupplier).build();
                return factory.newReadWriteEngine(wrappedConfig);
            });
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
            IndexEngine.EngineMetrics engineMetrics
        ) {
            final var shardId = engineConfig.getShardId();
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                statelessCommitService.getCommitBCCResolverForShard(shardId),
                documentParsingProvider,
                engineMetrics,
                statelessCommitService.getShardLocalCommitsTracker(shardId).shardLocalReadersTracker()
            ) {
                @Override
                public long getLastSyncedGlobalCheckpoint() {
                    final long result = super.getLastSyncedGlobalCheckpoint();
                    final var hook = afterGetLastSyncedGlobalCheckpoint.get(shardId);
                    if (hook != null) {
                        hook.run();
                    }
                    return result;
                }
            };
        }
    }
}

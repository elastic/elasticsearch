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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.BlobCacheIndexInput;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexBlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;
import co.elastic.elasticsearch.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import org.apache.logging.log4j.Level;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.ApplyCommitRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryClusterStateDelayListeners;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.SHARD_INACTIVITY_DURATION_TIME_SETTING;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING;
import static co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.OBJECT_STORE_FILE_DELETION_DELAY;
import static org.elasticsearch.cluster.action.shard.ShardStateAction.SHARD_STARTED_ACTION_NAME;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.MAX_MISSED_HEARTBEATS;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessFileDeletionIT extends AbstractStatelessIntegTestCase {

    /**
     * A plugin that can block snapshot threads from opening {@link BlobCacheIndexInput} instances
     */
    public static class SnapshotBlockerStatelessPlugin extends Stateless {

        public final Semaphore snapshotBlocker = new Semaphore(Integer.MAX_VALUE);

        public SnapshotBlockerStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexBlobStoreCacheDirectory createIndexBlobStoreCacheDirectory(
            StatelessSharedBlobCacheService cacheService,
            ShardId shardId
        ) {
            return new TrackingIndexBlobStoreCacheDirectory(cacheService, shardId, snapshotBlocker);
        }

        private static class TrackingIndexBlobStoreCacheDirectory extends IndexBlobStoreCacheDirectory {

            public final Semaphore blocker;

            TrackingIndexBlobStoreCacheDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId, Semaphore blocker) {
                super(cacheService, shardId);
                this.blocker = blocker;
            }

            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                if (Objects.equals(EsExecutors.executorName(Thread.currentThread()), ThreadPool.Names.SNAPSHOT)) {
                    safeAcquire(blocker);
                    try {
                        return super.openInput(name, context);
                    } finally {
                        blocker.release();
                    }
                }
                return super.openInput(name, context);
            }
        }

        public Releasable blockSnapshots() {
            safeAcquire(Integer.MAX_VALUE, snapshotBlocker);
            return () -> {
                if (snapshotBlocker.availablePermits() == 0) {
                    unblockSnapshots();
                }
            };
        }

        public void unblockSnapshots() {
            assert snapshotBlocker.availablePermits() == 0;
            snapshotBlocker.release(Integer.MAX_VALUE);
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(SnapshotBlockerStatelessPlugin.class);
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "5s")
            .put(StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(), "100ms")
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    public void testSnapshotRetainsCommits() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        ensureStableCluster(2);

        boolean fastRefresh = randomBoolean();
        final var indexName = fastRefresh ? SYSTEM_INDEX_NAME : randomIdentifier();
        final var settings = indexSettings(1, 0).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), fastRefresh)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
            .build();
        if (fastRefresh) {
            createSystemIndex(settings);
        } else {
            createIndex(indexName, settings);
        }
        ensureGreen(indexName);

        indexDocsAndFlush(indexName);
        final var genA = getIndexingShardTermAndGeneration(indexName, 0);
        indexDocsAndFlush(indexName);
        final var genB = getIndexingShardTermAndGeneration(indexName, 0);

        final var indexShard = findIndexShard(indexName);

        createRepository(logger, "test-repo", "fs");

        final Thread snapshot = new Thread(() -> {
            try {
                String snapshotName = "test-snap-0";
                CreateSnapshotResponse createSnapshotResponse = client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", snapshotName)
                    .setIncludeGlobalState(true)
                    .setWaitForCompletion(true)
                    .get();
                final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
                assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
                assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
            } catch (Throwable e) {
                throw new AssertionError(e);
            }
        });

        var plugin = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(SnapshotBlockerStatelessPlugin.class)
            .findFirst()
            .get();
        try (Releasable ignored = plugin.blockSnapshots()) {
            logger.info("Starting snapshot");
            snapshot.start();
            assertBusy(() -> assertTrue(plugin.snapshotBlocker.hasQueuedThreads()));

            logger.info("Indexing more docs");
            indexDocsAndFlush(indexName);
            final var genC = getIndexingShardTermAndGeneration(indexName, 0);
            indexDocsAndFlush(indexName);
            final var genD = getIndexingShardTermAndGeneration(indexName, 0);

            // force merge to one segment
            logger.info("Force merging");
            var forceMerge = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(false).get();
            assertThat(forceMerge.getSuccessfulShards(), equalTo(1));
            logger.info("Flushing");
            flush(indexName);
            final var genE = getIndexingShardTermAndGeneration(indexName, 0);

            assertBusy(() -> {
                var blobCommits = listBlobsTermAndGenerations(indexShard.shardId());
                // genA and genB retained due to being referenced by the snapshot. genE retained as latest commit.
                assertThat(blobCommits, hasItems(genA, genB, genE));
                // genC and genD have been merged away and not referenced by anything.
                assertThat(blobCommits, not(hasItems(genC, genD)));
            });

            // Evict everything from the indexing node's cache
            logger.info("Evicting cache");
            BlobStoreCacheDirectory indexShardBlobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(
                indexShard.store().directory()
            );
            getCacheService(indexShardBlobStoreCacheDirectory).forceEvict((key) -> true);

            logger.info("Unblocking snapshot");
            plugin.unblockSnapshots();
            snapshot.join();

            assertBusy(() -> {
                var blobCommits = listBlobsTermAndGenerations(indexShard.shardId());
                // genE retained as latest commit.
                assertThat(blobCommits, hasItems(genE));
                // genB and genC are ultimately deleted after the snapshot has ended.
                assertThat(blobCommits, not(hasItems(genA, genB)));
            });
        }
    }

    public void testActiveTranslogFilesArePrunedAfterCommit() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = getTranslogReplicator(indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = getObjectStoreService(indexNode);
        assertTranslogBlobsExist(activeTranslogFiles, indexObjectStoreService);

        flush(indexName);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
            assertTranslogBlobsDoNotExist(activeTranslogFiles, indexObjectStoreService);
        });
    }

    public void testActiveTranslogFilesNotPrunedOnNodeStop() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = getTranslogReplicator(indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = getObjectStoreService(indexNode);
        var blobContainer = indexObjectStoreService.getTranslogBlobContainer();

        internalCluster().stopNode(indexNode);

        // The files should be pruned from memory, even though they will stay on the object store.
        assertBusy(() -> assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0)));

        for (TranslogReplicator.BlobTranslogFile translogFile : activeTranslogFiles) {
            assertTrue(blobContainer.blobExists(operationPurpose, translogFile.blobName()));
        }
        // Start a new node to take over the shards or else the test will hang for a while in teardown
        startIndexNode();
    }

    public void testActiveTranslogFilesNotPrunedOnFailure() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = getTranslogReplicator(indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = getObjectStoreService(indexNode);
        var blobContainer = indexObjectStoreService.getTranslogBlobContainer();

        Exception shardFailed = new Exception("Shard Failed");
        if (randomBoolean()) {
            ShardStateAction instance = internalCluster().getInstance(ShardStateAction.class, indexNode);
            PlainActionFuture<Void> listener = new PlainActionFuture<>();
            instance.localShardFailed(findIndexShard(indexName).routingEntry(), "test failure", shardFailed, listener);
            listener.actionGet();
        } else {
            internalCluster().getInstance(IndicesService.class, indexNode)
                .getShardOrNull(findIndexShard(indexName).shardId())
                .getEngineOrNull()
                .failEngine("test", shardFailed);
        }

        // Pause to wait async delete complete if it is scheduled
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));

        // The files should be pruned from memory, even though they will stay on the object store.
        assertBusy(() -> assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0)));

        for (TranslogReplicator.BlobTranslogFile translogFile : activeTranslogFiles) {
            assertTrue(blobContainer.blobExists(operationPurpose, translogFile.blobName()));
        }
    }

    public void testActiveTranslogFilesArePrunedAfterRelocation() throws Exception {
        startMasterOnlyNode();

        int deleteDelayMillis = rarely() ? randomIntBetween(500, 1000) : 0;
        var indexNodeA = startIndexNode(
            Settings.builder().put(OBJECT_STORE_FILE_DELETION_DELAY.getKey(), TimeValue.timeValueMillis(deleteDelayMillis)).build()
        );

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var translogReplicator = getTranslogReplicator(indexNodeA);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        var indexObjectStoreService = getObjectStoreService(indexNodeA);
        assertTranslogBlobsExist(activeTranslogFiles, indexObjectStoreService);

        long millisBeforeDeletions = System.currentTimeMillis();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodeB), indexName);

        ensureGreen(indexName);

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
            assertTranslogBlobsDoNotExist(activeTranslogFiles, indexObjectStoreService);
        });
        long millisForDeletions = System.currentTimeMillis() - millisBeforeDeletions;
        assertThat("delete delay should have taken effect", millisForDeletions, greaterThan((long) deleteDelayMillis));
    }

    public void testActiveTranslogFilesArePrunedCaseWithMultipleShards() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexNameA = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String indexNameB = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexNameA,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        createIndex(
            indexNameB,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexNameA, indexNameB);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexNameA, randomIntBetween(1, 100));
            indexDocs(indexNameB, randomIntBetween(1, 100));
        }

        var translogReplicator = getTranslogReplicator(indexNode);
        var objectStoreService = getObjectStoreService(indexNode);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        logger.info("--> activeTranslogFiles {}", activeTranslogFiles);
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        flush(indexNameA);

        HashSet<TranslogReplicator.BlobTranslogFile> toDelete = new HashSet<>();
        Set<TranslogReplicator.BlobTranslogFile> stillActive = translogReplicator.getActiveTranslogFiles();
        for (TranslogReplicator.BlobTranslogFile file : activeTranslogFiles) {
            if (stillActive.contains(file) == false) {
                toDelete.add(file);
            }
        }

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles().size(), greaterThan(0));
            assertTranslogBlobsDoNotExist(toDelete, objectStoreService);
        });

        // TODO: Implement the mechanism to allow translog file prune when index deleted
        if (true) {
            flush(indexNameB);
        } else {
            // If meanwhile the index is deleted, we should still be able to clean up the translog blobs, since
            // the other index is committed.
            assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexNameB)).actionGet());
        }

        assertBusy(() -> {
            assertThat(translogReplicator.getActiveTranslogFiles(), empty());

            assertTranslogBlobsDoNotExist(activeTranslogFiles, objectStoreService);
        });
    }

    public void testStaleNodeDoesNotDeleteFile() throws Exception {
        String masterNode = startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        String indexNodeA = startIndexNode(
            Settings.builder()
                .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        SeqNoStats beforeSeqNoStats = client(indexNodeA).admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();

        String indexNodeB = startIndexNode();

        ensureStableCluster(3);

        var translogReplicator = getTranslogReplicator(indexNodeA);

        Set<TranslogReplicator.BlobTranslogFile> activeTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeTranslogFiles.size(), greaterThan(0));

        final MockTransportService indexNodeTransportService = MockTransportService.getInstance(indexNodeA);
        final MockTransportService masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        ObjectStoreService indexNodeAObjectStoreService = getObjectStoreService(indexNodeA);
        ObjectStoreService indexNodeBObjectStoreService = getObjectStoreService(indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(indexNodeBObjectStoreService);
        repository.setBlockOnAnyFiles();

        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();

        final ClusterService masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        masterClusterService.addListener(clusterChangedEvent -> {
            if (removedNode.isDone() == false
                && clusterChangedEvent.nodesDelta().removedNodes().stream().anyMatch(d -> d.getName().equals(indexNodeA))) {
                removedNode.onResponse(null);
            }
        });

        try {
            masterTransportService.addUnresponsiveRule(indexNodeTransportService);
            removedNode.actionGet();

            // Slight delay to allow the new node to start recovering from an old commit before the new commit is triggered
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

            client(indexNodeA).admin().indices().prepareFlush(indexName).execute().actionGet();

            assertBusy(() -> { assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0)); });
            assertTranslogBlobsExist(activeTranslogFiles, indexNodeAObjectStoreService);

        } finally {
            masterTransportService.clearAllRules();
        }

        assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0));
        assertTranslogBlobsExist(activeTranslogFiles, indexNodeAObjectStoreService);

        repository.unblock();

        assertBusy(() -> assertTranslogBlobsDoNotExist(activeTranslogFiles, indexNodeAObjectStoreService));

        ensureGreen(indexName);

        SeqNoStats afterSeqNoStats = client(indexNodeB).admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();
        assertEquals(beforeSeqNoStats.getMaxSeqNo(), afterSeqNoStats.getMaxSeqNo());
    }

    public void testStaleCommitsArePrunedAfterBeingReleased() throws Exception {
        startMasterOnlyNode();
        int deleteDelayMillis = rarely() ? randomIntBetween(500, 1000) : 0;
        var indexNode = startIndexNode(
            Settings.builder().put(OBJECT_STORE_FILE_DELETION_DELAY.getKey(), TimeValue.timeValueMillis(deleteDelayMillis)).build()
        );
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);

        int totalIndexedDocs = 0;
        int numberOfCommitsBeforeMerge = 3;
        for (int i = 0; i < numberOfCommitsBeforeMerge; i++) {
            totalIndexedDocs += indexDocsAndFlush(indexName);
        }

        var blobsBeforeMerging = listBlobsWithAbsolutePath(shardCommitsContainer);

        long millisBeforeDeletions = System.currentTimeMillis();
        forceMerge();
        // We need to refresh so the local index reader releases the reference from the previous commit
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());

        assertBusy(() -> {
            var blobsAfterMerging = listBlobsWithAbsolutePath(shardCommitsContainer);
            assertThat(Sets.intersection(blobsBeforeMerging, blobsAfterMerging), empty());
        });
        long millisForDeletions = System.currentTimeMillis() - millisBeforeDeletions;
        assertThat("delete delay should have taken effect", millisForDeletions, greaterThan((long) deleteDelayMillis));
        assertThat(
            SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName).setQuery(matchAllQuery())),
            equalTo((long) totalIndexedDocs)
        );
    }

    public void testCommitsAreRetainedUntilScrollCloses() throws Exception {
        testCommitsRetainementWithSearchScroll(TestSearchScrollCase.COMMITS_RETAINED_UNTIL_SCROLL_CLOSES);
    }

    public void testCommitsAreDroppedAfterScrollClosesAndIndexingInactivity() throws Exception {
        testCommitsRetainementWithSearchScroll(TestSearchScrollCase.COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY);
    }

    @TestLogging(reason = "debugging", value = "co.elastic.elasticsearch.stateless.commits.StatelessCommitService:DEBUG")
    public void testCommitsOfScrollAreDeletedAfterIndexIsClosedAndOpened() throws Exception {
        testCommitsRetainementWithSearchScroll(TestSearchScrollCase.COMMITS_OF_SCROLL_DELETED_AFTER_INDEX_CLOSED_AND_OPENED);
    }

    public void testAllCommitsDeletedAfterIndexIsDeleted() throws Exception {
        testCommitsRetainementWithSearchScroll(TestSearchScrollCase.ALL_COMMITS_DELETED_AFTER_INDEX_DELETED);
    }

    private enum TestSearchScrollCase {
        COMMITS_RETAINED_UNTIL_SCROLL_CLOSES,
        COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY,
        COMMITS_OF_SCROLL_DELETED_AFTER_INDEX_CLOSED_AND_OPENED,
        ALL_COMMITS_DELETED_AFTER_INDEX_DELETED
    }

    private void testCommitsRetainementWithSearchScroll(TestSearchScrollCase testCase) throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode(
            testCase == TestSearchScrollCase.COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY
                ? Settings.builder()
                    .put(SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.getKey(), "100ms")
                    .put(SHARD_INACTIVITY_DURATION_TIME_SETTING.getKey(), "100ms")
                    .build()
                : Settings.EMPTY
        );
        var searchNode = startSearchNode();
        AtomicInteger countNewCommitNotifications = new AtomicInteger();
        AtomicReference<Set<PrimaryTermAndGeneration>> termAndGensInUse = new AtomicReference<>();
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return "default";
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        countNewCommitNotifications.incrementAndGet();
                        termAndGensInUse.set(((NewCommitNotificationResponse) response).getPrimaryTermAndGenerationsInUse());
                        channel.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        channel.sendResponse(exception);
                    }
                }, task);
            });

        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        var initialBlobs = listBlobsWithAbsolutePath(shardCommitsContainer);

        int totalIndexedDocs = 0;

        var numDocsBeforeOpenScroll = indexDocsAndFlush(indexName);
        totalIndexedDocs += numDocsBeforeOpenScroll;

        // We need to disregard the first empty commit
        var blobsUsedForScroll = Sets.difference(listBlobsWithAbsolutePath(shardCommitsContainer), initialBlobs);

        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        final AtomicReference<String> currentScrollId = new AtomicReference<>();
        assertResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(1).setScroll(TimeValue.timeValueMinutes(2)),
            scrollSearchResponse -> currentScrollId.set(scrollSearchResponse.getScrollId())
        );

        var numberOfCommitsAfterOpeningScroll = randomIntBetween(3, 5);
        for (int i = 0; i < numberOfCommitsAfterOpeningScroll; i++) {
            totalIndexedDocs += indexDocsAndFlush(indexName);
        }

        forceMerge();
        // We need to refresh so the local index reader releases the reference from the previous commit
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());

        // We request 1 document per search request
        int numberOfScrollRequests = numDocsBeforeOpenScroll - 1;
        for (int i = 0; i < numberOfScrollRequests; i++) {
            assertResponse(client().prepareSearchScroll(currentScrollId.get()).setScroll(TimeValue.timeValueMinutes(2)), searchResponse -> {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit, is(notNullValue()));
                currentScrollId.set(searchResponse.getScrollId());
            });
        }

        assertThat(
            SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName).setQuery(matchAllQuery())),
            equalTo((long) totalIndexedDocs)
        );

        var blobsBeforeReleasingScroll = listBlobsWithAbsolutePath(shardCommitsContainer);
        assertThat(blobsBeforeReleasingScroll.containsAll(blobsUsedForScroll), is(true));

        var indexShardCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var shardId = new ShardId(resolveIndex(indexName), 0);
        var latestUploadedBcc = indexShardCommitService.getLatestUploadedBcc(shardId);
        assertNotNull(latestUploadedBcc);
        // Wait for search node to respond to all new commit notifications
        assertBusy(() -> assertTrue(termAndGensInUse.get().contains(latestUploadedBcc.primaryTermAndGeneration())));

        var newCommitNotificationsBeforeReleasingScroll = countNewCommitNotifications.get();
        switch (testCase) {
            case COMMITS_RETAINED_UNTIL_SCROLL_CLOSES:
                client().prepareClearScroll().addScrollId(currentScrollId.get()).get();
                // Trigger a new flush so the index shard cleans the unused files after the search node responds with the used commits
                totalIndexedDocs += indexDocsAndFlush(indexName);
                break;
            case COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY:
                client().prepareClearScroll().addScrollId(currentScrollId.get()).get();
                // New commit notifications should be sent from the inactive indexing shard so that ultimately the new commit notification
                // responses do not contain the search's open readers anymore, and the shard cleans unused files.
                break;
            case COMMITS_OF_SCROLL_DELETED_AFTER_INDEX_CLOSED_AND_OPENED:
                assertAcked(indicesAdmin().close(new CloseIndexRequest(indexName)).actionGet());
                client().prepareClearScroll().addScrollId(currentScrollId.get()).get();
                assertAcked(indicesAdmin().open(new OpenIndexRequest(indexName)).actionGet());
                break;
            case ALL_COMMITS_DELETED_AFTER_INDEX_DELETED:
                assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
                assertBusy(() -> { assertThat(listBlobsWithAbsolutePath(shardCommitsContainer), empty()); }); // all blobs should be deleted
                return;
            default:
                assert false : "unknown test case " + testCase;
        }

        // Check that scroll's blobs are deleted
        assertBusy(() -> {
            var blobsAfterReleasingScroll = listBlobsWithAbsolutePath(shardCommitsContainer);
            assertThat(Sets.intersection(blobsUsedForScroll, blobsAfterReleasingScroll), empty());
        });
        // responses from newCommitNotification must be received
        assertThat(countNewCommitNotifications.get(), greaterThan(newCommitNotificationsBeforeReleasingScroll));

        if (testCase == TestSearchScrollCase.COMMITS_DROPPED_AFTER_SCROLL_CLOSES_AND_INDEXING_INACTIVITY) {
            // The last notification response should no longer contain blobs used for scroll
            assertThat(
                Sets.intersection(
                    blobsUsedForScroll.stream()
                        .map(name -> name.substring(name.lastIndexOf('/') + 1))
                        .map(StatelessCompoundCommit::parseGenerationFromBlobName)
                        .collect(Collectors.toUnmodifiableSet()),
                    termAndGensInUse.get().stream().map(PrimaryTermAndGeneration::generation).collect(Collectors.toUnmodifiableSet())
                ),
                empty()
            );
            // Verify that there is no more new commit notifications sent
            int currentCount = countNewCommitNotifications.get();
            indexShardCommitService.updateCommitUseTrackingForInactiveShards(() -> Long.MAX_VALUE);
            assertThat(countNewCommitNotifications.get(), equalTo(currentCount));
        }

        // Check that a new search returns all docs
        refresh(indexName);
        assertThat(
            SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName).setQuery(matchAllQuery())),
            equalTo((long) totalIndexedDocs)
        );
    }

    public void testDeleteIndexAfterFlush() throws Exception {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        indexDocsAndFlush(indexName);
        assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
        assertBusy(() -> { assertThat(listBlobsWithAbsolutePath(shardCommitsContainer), empty()); });
    }

    @TestLogging(
        reason = "verifying shutdown doesn't cause warnings",
        value = "co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService:WARN"
    )
    public void testDeleteIndexWhileNodeStopping() {
        var indexNode = startMasterAndIndexNode();
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        startMasterAndIndexNode(Settings.builder().put(HEARTBEAT_FREQUENCY.getKey(), "1s").put(MAX_MISSED_HEARTBEATS.getKey(), 1).build());

        indexDocsAndFlush(indexName);
        indexDocsAndFlush(indexName);

        final var startBarrier = new CyclicBarrier(3);

        var client = client(searchNode);
        final var indexDocsAndFlushThread = new Thread(() -> {
            try {
                safeAwait(startBarrier);
                var bulkRequest = client.prepareBulk();
                IntStream.rangeClosed(0, randomIntBetween(10, 20))
                    .mapToObj(ignored -> new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)))
                    .forEach(bulkRequest::add);
                var bulkResponse = bulkRequest.get();
                for (var item : bulkResponse.getItems()) {
                    if (item.getFailure() != null) {
                        // can happen while node is stopped
                        assertThat(item.getFailure().getStatus(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }
                flush(indexName); // asserts that shard failures are equal to RestStatus.SERVICE_UNAVAILABLE
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        final var forceMergeThread = new Thread(() -> {
            try {
                safeAwait(startBarrier);
                client.admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
            } catch (ClusterBlockException | MasterNotDiscoveredException e) {
                // can happen while node is stopped
                assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        MockLog.assertThatLogger(() -> {
            try {
                indexDocsAndFlushThread.start();
                forceMergeThread.start();
                safeAwait(startBarrier);
                internalCluster().stopNode(indexNode);
                indexDocsAndFlushThread.join();
                forceMergeThread.join();
            } catch (Exception e) {
                fail(e);
            }
        },
            ObjectStoreService.class,
            new MockLog.UnseenEventExpectation(
                "warnings",
                ObjectStoreService.class.getCanonicalName(),
                Level.WARN,
                "exception while attempting to delete blob files*"
            )
        );

        // no assertions really, just checking that this doesn't trip anything in ObjectStoreService; we can do more when ES-7400 is done.
    }

    public void testDeleteIndexAfterRecovery() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        final var searchNodeA = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        var totalIndexedDocs = indexDocsAndFlush(indexName);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNodeA, 0);

        startIndexNode();
        startSearchNode();
        ensureStableCluster(5);
        final var excludeIndexOrSearchNode = randomBoolean();
        String nodeToExclude = excludeIndexOrSearchNode ? indexNodeA : searchNodeA;
        boolean excludeOrStop = randomBoolean();
        logger.info(
            "--> {} {} node {}",
            excludeOrStop ? "excluding" : "stopping",
            excludeIndexOrSearchNode ? "index" : "search",
            nodeToExclude
        );
        if (excludeOrStop) {
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", nodeToExclude), indexName);
            if (randomBoolean()) {
                assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(nodeToExclude))));
            }
        } else {
            internalCluster().stopNode(nodeToExclude);
        }

        logger.info("--> deleting index");
        assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
        assertBusy(() -> assertThat(listBlobsWithAbsolutePath(shardCommitsContainer), empty())); // all blobs should be deleted
    }

    public void testDeleteIndexAfterPrimaryRelocation() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        final var searchNodeA = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        var totalIndexedDocs = indexDocsAndFlush(indexName);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNodeA, 0);

        startIndexNode();
        ensureStableCluster(4);

        logger.info("--> excluding {}", indexNodeA);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));

        logger.info("--> deleting index");
        assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
        assertBusy(() -> assertThat(listBlobsWithAbsolutePath(shardCommitsContainer), empty())); // all blobs should be deleted
    }

    public void testStaleNodeDoesNotDeleteCommitFiles() throws Exception {
        startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        String indexNodeA = startIndexNode(
            Settings.builder()
                // This prevents triggering an election in the isolated node once the link between it and the master is blackholed
                .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "200")
                .build()
        );
        ensureStableCluster(2);

        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNodeA, 0);
        var initialBlobs = listBlobsWithAbsolutePath(shardCommitsContainer);

        final int numberOfSegments = randomIntBetween(2, 5);
        for (int i = 0; i < numberOfSegments; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            flush(indexName);
        }

        var indexNodeB = startIndexNode();
        ensureStableCluster(3);

        var shardId = new ShardId(resolveIndex(indexName), 0);

        // We need to disregard the first empty commit that's deleted right away
        var blobsBeforeTriggeringForceMerge = Sets.difference(listBlobsWithAbsolutePath(shardCommitsContainer), initialBlobs);

        final MockTransportService indexNodeTransportService = MockTransportService.getInstance(indexNodeA);
        final MockTransportService masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());

        var primaryShardNodeRemoved = new PlainActionFuture<>();
        var shardRelocated = new PlainActionFuture<>();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(clusterChangedEvent -> {
            if (primaryShardNodeRemoved.isDone() == false
                && clusterChangedEvent.nodesDelta().removedNodes().stream().anyMatch(d -> d.getName().equals(indexNodeA))) {
                primaryShardNodeRemoved.onResponse(null);
            }
            if (shardRelocated.isDone() == false) {
                var clusterState = clusterChangedEvent.state();
                var primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
                if (primaryShard.started() && primaryShard.currentNodeId().equals(clusterState.nodes().resolveNode(indexNodeB).getId())) {
                    shardRelocated.onResponse(null);
                }
            }
        });

        var rejoinedCluster = new PlainActionFuture<>();
        internalCluster().getInstance(ClusterService.class, indexNodeA).addListener(clusterChangedEvent -> {
            if (rejoinedCluster.isDone() == false) {
                var nodesDelta = clusterChangedEvent.nodesDelta();
                if (nodesDelta.masterNodeChanged() && nodesDelta.previousMasterNode() == null) {
                    rejoinedCluster.onResponse(null);
                }
            }
        });

        masterTransportService.addUnresponsiveRule(indexNodeTransportService);
        primaryShardNodeRemoved.actionGet();

        // Trigger a force merge in the stale primary to force a "possible" deletion of the previous commits
        var forceMergeFuture = client(indexNodeA).admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute();

        // Ensure that the shard is relocated to indexNodeB
        shardRelocated.get();

        // The consistency check reads the blob store and notices that it's behind and waits until there's a new cluster state update
        masterTransportService.clearAllRules();

        forceMergeFuture.get();
        // The consistency check is executed in an observer that's applied before the listener that triggers this future
        rejoinedCluster.get();

        var blobsAfterNodeIsStale = listBlobsWithAbsolutePath(shardCommitsContainer);
        assertThat(blobsAfterNodeIsStale.containsAll(blobsBeforeTriggeringForceMerge), is(true));
    }

    // Since the commit deletion relies on a NewCommitNotification being processed on all unpromotables, while an unpromotable is
    // recovering (and does not respond to a NewCommitNotification), commits should not be deleted.
    public void testCommitsNotDeletedWhileAnUnpromotableIsRecovering() throws Exception {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);
        var searchNode2 = startSearchNode();
        ensureStableCluster(3);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        var initialBlobs = listBlobsWithAbsolutePath(shardCommitsContainer);

        var indexShard = findIndexShard(indexName);
        var initialGeneration = asInstanceOf(IndexEngine.class, indexShard.getEngineOrNull()).getCurrentGeneration();

        // Create some commits
        int commits = randomIntBetween(2, 5);
        for (int i = 0; i < commits; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            refresh(indexName);
        }

        final long recoveryGeneration = initialGeneration + commits;
        logger.debug("--> search shard 2 will recover from generation {}", recoveryGeneration);

        AtomicBoolean enableChecks = new AtomicBoolean(true);
        CountDownLatch commitRegistrationStarted = new CountDownLatch(1);
        MockRepository searchNode2Repository = ObjectStoreTestUtils.getObjectStoreMockRepository(getObjectStoreService(searchNode2));
        CountDownLatch getVbccChunkLatch = new CountDownLatch(1);

        Runnable blockGetVbccChunk = () -> MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(
                TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> {
                    safeAwait(getVbccChunkLatch);
                    handler.messageReceived(request, channel, task);
                }
            );

        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                handler.messageReceived(
                    request,
                    new TestTransportChannel(ActionListener.runBefore(new ChannelActionListener<>(channel), () -> {
                        if (enableChecks.get()) {
                            commitRegistrationStarted.countDown();
                            searchNode2Repository.setBlockOnAnyFiles(); // block recovery from object store
                            blockGetVbccChunk.run(); // block recovery from indexing node
                        }
                    })),
                    task
                );
            });

        final var blobsBeforeNewCommitNotificationResponse = new AtomicReference<Set<String>>();
        final var searchShardRecovered = new CountDownLatch(1);

        // Delay all new commit notifications on searchNode2 except the one to recover from
        final var delayedNotifications = new LinkedBlockingQueue<CheckedRunnable<Exception>>();
        final var newCommitNotificationReceived = new CountDownLatch(1);
        final var delayNotifications = new AtomicBoolean(true);

        logger.debug("--> start delaying new commit notifications on node [{}] for generations > {}", searchNode2, recoveryGeneration);
        MockTransportService.getInstance(searchNode2)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                var notification = asInstanceOf(NewCommitNotificationRequest.class, request);
                // we want to notification from recovery to be processed, as it is required to start the search shard
                if (delayNotifications.get() && (recoveryGeneration < notification.getGeneration())) {
                    logger.debug("--> delaying new commit notification for generation [{}]", notification.getGeneration());
                    delayedNotifications.add(
                        () -> handler.messageReceived(
                            request,
                            new TestTransportChannel(ActionListener.runBefore(new ChannelActionListener<>(channel), () -> {
                                if (enableChecks.get()) {
                                    // After the shard has recovered, but before sending any new commit notification response (that could
                                    // trigger
                                    // blob deletions), store the current blobs, so we later check that the blobs before the merge are
                                    // intact.
                                    // 30 seconds timeout to align with ensureGreen after we release the vbccChunkLatch
                                    try {
                                        assertTrue(searchShardRecovered.await(30, TimeUnit.SECONDS));
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        fail(e, "safeAwait: interrupted waiting for CountDownLatch to reach zero");
                                    }
                                    blobsBeforeNewCommitNotificationResponse.set(listBlobsWithAbsolutePath(shardCommitsContainer));
                                }
                            })),
                            task
                        )
                    );
                    newCommitNotificationReceived.countDown();
                    return;
                }
                logger.debug("--> handling new commit notification for generation [{}]", notification.getGeneration());
                handler.messageReceived(request, channel, task);
            });

        // Start the second search shard and waits for recovery to start
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2), indexName);
        safeAwait(commitRegistrationStarted);
        var blobsBeforeMerge = Sets.difference(listBlobsWithAbsolutePath(shardCommitsContainer), initialBlobs);

        // While search shard is recovering, create a new merged commit
        logger.debug("--> force merging");
        // Do not assert that we got one segment since the underlying method calls TransportIndicesSegmentsAction that would be blocked
        // on the searchNode2 due to the blocked recovery
        forceMerge(false);

        logger.debug("--> wait for the new commit notification to be processed on the search node");
        safeAwait(newCommitNotificationReceived);

        // Allow recovery to finish, and trigger check that files should not be deleted
        searchNode2Repository.unblock();
        getVbccChunkLatch.countDown();
        ensureGreen(indexName);
        searchShardRecovered.countDown();

        logger.debug("--> stop delaying new commit notifications and process delayed notifications on node [{}]", searchNode2);
        delayNotifications.set(false);
        CheckedRunnable<Exception> delayedNotification;
        while ((delayedNotification = delayedNotifications.poll()) != null) {
            delayedNotification.run();
        }

        assertBusy(() -> {
            assertThat(blobsBeforeNewCommitNotificationResponse.get(), notNullValue());
            assertThat(
                "blobs before merge = "
                    + blobsBeforeMerge
                    + ", blobs before new commit notification response ="
                    + blobsBeforeNewCommitNotificationResponse.get(),
                blobsBeforeNewCommitNotificationResponse.get().containsAll(blobsBeforeMerge),
                is(true)
            );
        });

        // Disable the handlers, do a refresh, and wait until the old commits are deleted.
        enableChecks.set(false);
        // In #3749 we introduced a change where unpromotable shard commit registrations would pessimistically acquire a reference
        // of all blobs instead of the recovery commit (to help transferring open PITs between search nodes). That,
        // in addition to the changes introduced in #2734 where new commit notifications for non uploaded commits are not taken into account
        // for blob removals, makes necessary to upload a new blob in order to release one of the blobs that was not used by the recovery
        // commit but was held anyway.
        indexDocs(indexName, randomIntBetween(10, 20));
        flush(indexName);
        assertBusy(() -> {
            var blobsAfterRecoveryAndRefresh = listBlobsWithAbsolutePath(shardCommitsContainer);
            assertThat(Sets.intersection(blobsAfterRecoveryAndRefresh, blobsBeforeMerge), is(empty()));
        });
    }

    public void testStatelessCommitServiceClusterStateListenerHandlesNewShardAssignmentsCorrectly() {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        final long initialClusterStateVersion = clusterService().state().version();

        var indexName = randomIdentifier();

        try (var recoveryClusterStateDelayListeners = new RecoveryClusterStateDelayListeners(initialClusterStateVersion)) {
            MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
            indexTransportService.addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
                assertThat(request, instanceOf(ApplyCommitRequest.class));
                recoveryClusterStateDelayListeners.getClusterStateDelayListener(((ApplyCommitRequest) request).getVersion())
                    .addListener(ActionListener.wrap(ignored -> handler.messageReceived(request, channel, task), channel::sendResponse));
            });
            recoveryClusterStateDelayListeners.addCleanup(indexTransportService::clearInboundRules);

            final var searchNodeClusterService = internalCluster().getInstance(ClusterService.class, searchNode);
            final var indexCreated = new AtomicBoolean();
            final ClusterStateListener clusterStateListener = event -> {
                final var indexNodeProceedListener = recoveryClusterStateDelayListeners.getClusterStateDelayListener(
                    event.state().version()
                );
                final var indexRoutingTable = event.state().routingTable().index(indexName);
                assertNotNull(indexRoutingTable);
                final var indexShardRoutingTable = indexRoutingTable.shard(0);

                if (indexShardRoutingTable.primaryShard().assignedToNode() == false && indexCreated.compareAndSet(false, true)) {
                    // this is the cluster state update which creates the index, so fail the application in order to increase the chances of
                    // missing the index in the cluster state when the shard is recovered from the empty store
                    indexNodeProceedListener.onFailure(new RuntimeException("Unable to process cluster state update"));
                } else {
                    // this is some other cluster state update, so we must let it proceed now
                    indexNodeProceedListener.onResponse(null);
                }
            };
            searchNodeClusterService.addListener(clusterStateListener);
            recoveryClusterStateDelayListeners.addCleanup(() -> searchNodeClusterService.removeListener(clusterStateListener));

            prepareCreate(indexName).setSettings(indexSettings(1, 1)).get();
            ensureGreen(indexName);
        }
    }

    public void testLatestCommitDependenciesUsesTheRightGenerations() throws Exception {
        var maxNonUploadedCommits = randomIntBetween(4, 5);
        var nodeSettings = Settings.builder()
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), maxNonUploadedCommits)
            // Set the inactivity monitor to a high value, and the inactivity threshold to a low value. This allows us to run it explicitly,
            // but the inactivity monitor won't run on its own.
            .put(StatelessCommitService.SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueMinutes(30))
            .put(StatelessCommitService.SHARD_INACTIVITY_DURATION_TIME_SETTING.getKey(), TimeValue.timeValueMillis(1))
            .build();
        startMasterOnlyNode(nodeSettings);
        var indexNode = startIndexNode(nodeSettings);
        var searchNode = startSearchNode(nodeSettings);
        ensureStableCluster(3);

        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);

        Queue<Tuple<NewCommitNotificationRequest, CheckedRunnable<Exception>>> pendingNewCommitOnUploadNotifications =
            new LinkedBlockingQueue<>();
        CountDownLatch commitNotifications = new CountDownLatch(2);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                var newCommitNotificationRequest = (NewCommitNotificationRequest) request;
                if (newCommitNotificationRequest.isUploaded()) {
                    pendingNewCommitOnUploadNotifications.add(
                        Tuple.tuple(newCommitNotificationRequest, () -> handler.messageReceived(request, channel, task))
                    );
                    commitNotifications.countDown();
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        // Accumulate STATELESS_UPLOAD_MAX_AMOUNT_COMMITS to force a BCC upload
        for (int i = 0; i < maxNonUploadedCommits; i++) {
            indexDocs(indexName, randomIntBetween(10, 50));
            refresh(indexName);
        }

        // Now accumulate more commits locally, but not enough to trigger a BCC upload
        for (int i = 0; i < maxNonUploadedCommits - 1; i++) {
            indexDocs(indexName, randomIntBetween(10, 50));
            refresh(indexName);
        }

        boolean searchNodeUsesBothBCCs = randomBoolean();
        if (searchNodeUsesBothBCCs) {
            // Create a scroll that depends on the previous and current BCC
            assertResponse(
                prepareSearch(indexName).setQuery(matchAllQuery()).setSize(1).setScroll(TimeValue.timeValueMinutes(2)),
                response -> {}
            );
        }

        // The latest commit in the BCC will be an independent commit
        client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        // Force a refresh so the index local reader only has a dependency on the latest BCC
        refresh(indexName);

        safeAwait(commitNotifications);

        Set<PrimaryTermAndGeneration> uploadedBCCGenerations = new HashSet<>();
        // Process the new commit notifications
        Tuple<NewCommitNotificationRequest, CheckedRunnable<Exception>> pendingNewCommingOnUploadNotification;
        while ((pendingNewCommingOnUploadNotification = pendingNewCommitOnUploadNotifications.poll()) != null) {
            pendingNewCommingOnUploadNotification.v2().run();
            NewCommitNotificationRequest newCommitNotificationRequest = pendingNewCommingOnUploadNotification.v1();
            uploadedBCCGenerations.add(newCommitNotificationRequest.getLatestUploadedBatchedCompoundCommitTermAndGen());
        }

        long maxUploadedBCCGeneration = uploadedBCCGenerations.stream().mapToLong(PrimaryTermAndGeneration::generation).max().orElse(-1);

        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        final Set<String> expectedBlobsAfterProcessingNewCommitNotifications;
        if (searchNodeUsesBothBCCs) {
            // If the search node opened a scroll, we should not remove any of the BCCs
            expectedBlobsAfterProcessingNewCommitNotifications = uploadedBCCGenerations.stream()
                .map(uploadedBCC -> BatchedCompoundCommit.blobNameFromGeneration(uploadedBCC.generation()))
                .collect(Collectors.toSet());
        } else {
            expectedBlobsAfterProcessingNewCommitNotifications = Set.of(
                BatchedCompoundCommit.blobNameFromGeneration(maxUploadedBCCGeneration)
            );
        }

        assertBusy(
            () -> assertThat(
                shardCommitsContainer.listBlobs(operationPurpose).keySet(),
                is(equalTo(expectedBlobsAfterProcessingNewCommitNotifications))
            )
        );

        internalCluster().stopNode(searchNode);
        StatelessCommitServiceTestUtils.updateCommitUseTrackingForInactiveShards(
            internalCluster().getInstance(StatelessCommitService.class, indexNode),
            () -> Long.MAX_VALUE
        );

        // If all the search nodes leave the cluster we should keep the latest BCC around
        assertBusy(
            () -> assertThat(
                shardCommitsContainer.listBlobs(operationPurpose).keySet(),
                is(equalTo(Set.of(BatchedCompoundCommit.blobNameFromGeneration(maxUploadedBCCGeneration))))
            )
        );

        // Until we fix ES-8335 we should do an explicit flush to release all VBCCs
        flush(indexName);
    }

    public void testUnpromotableRegisterCommitForRecoveryTracksAllBlobsPessimisticallyAndWaitsUntilShardStarts() throws Exception {
        var indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueSeconds(2))
                .put(SHARD_INACTIVITY_DURATION_TIME_SETTING.getKey(), TimeValue.timeValueSeconds(2))
                .build()
        );
        var firstSearchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1).put("index.routing.allocation.include._name", String.join(",", indexNode, firstSearchNode)).build()
        );
        ensureGreen(indexName);

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        var shardCommitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);

        var openPITs = new ArrayList<BytesReference>();
        var searchEngine = getShardEngine(findSearchShard(indexName), SearchEngine.class);
        for (int i = 0; i < 10; i++) {
            indexDocsAndFlush(indexName);

            // We have to wait until the search shard has received the new commit notification
            // for the upload to ensure that we get open the PIT against the uploaded commit.
            var latestUploadedBCC = commitService.getLatestUploadedBcc(shardId);
            assertThat(latestUploadedBCC, is(notNullValue()));
            var latestUploadedBCCPTG = latestUploadedBCC.primaryTermAndGeneration();
            var uploadedBCCRefreshedListener = new SubscribableListener<Long>();
            searchEngine.addPrimaryTermAndGenerationListener(
                latestUploadedBCCPTG.primaryTerm(),
                latestUploadedBCCPTG.generation(),
                uploadedBCCRefreshedListener
            );
            safeAwait(uploadedBCCRefreshedListener);

            // Open a PIT against each commit so we have to retain all of them
            var openPITResponse = client(firstSearchNode).execute(
                TransportOpenPointInTimeAction.TYPE,
                new OpenPointInTimeRequest(indexName).keepAlive(TimeValue.timeValueHours(1)).allowPartialSearchResults(false)
            ).actionGet();
            openPITs.add(openPITResponse.getPointInTimeId());
            assertThat(openPITResponse.getFailedShards(), is(equalTo(0)));
            if (i % 2 == 0) {
                forceMerge(true);
            }
        }

        // Run a force merge so only the open PITs are retaining the previous commits
        forceMerge(true);

        var newSearchNode = startSearchNode();

        var newCommitNotificationReceivedLatch = new CountDownLatch(1);
        MockTransportService.getInstance(newSearchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                newCommitNotificationReceivedLatch.countDown();
                handler.messageReceived(request, channel, task);
            });
        var retainedBlobsBeforeUnpromotableShardMoved = shardCommitsContainer.listBlobs(operationPurpose).keySet();

        var delayShardStartedMessages = new AtomicBoolean(true);
        var delayedShardStartedMessages = new ConcurrentLinkedQueue<CheckedRunnable<Exception>>();
        var shardStartedSentFromNewSearchNode = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(SHARD_STARTED_ACTION_NAME, (handler, request, channel, task) -> {
                shardStartedSentFromNewSearchNode.countDown();
                if (delayShardStartedMessages.get()) {
                    delayedShardStartedMessages.add(() -> handler.messageReceived(request, channel, task));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        updateIndexSettings(
            Settings.builder().put("index.routing.allocation.include._name", String.join(",", indexName, newSearchNode)),
            indexName
        );
        // We'll block the shard started message to ensure that the shard is still initializing while new commits arrive to the
        // new search node

        safeAwait(shardStartedSentFromNewSearchNode);

        // Wait until the new search node has received at least one new commit notification
        safeAwait(newCommitNotificationReceivedLatch);

        // The new search node should have responded back to the new commit notification
        // (and at this point it'll be just using the latest commit), but since it's still initializing
        // the response would be ignored
        assertThat(shardCommitsContainer.listBlobs(operationPurpose).keySet(), is(equalTo(retainedBlobsBeforeUnpromotableShardMoved)));

        // The default waitForActiveShards would wait until the relocating search shard has finished, we don't need to wait for that.
        indexDocs(indexName, randomIntBetween(10, 20), bulkRequest -> bulkRequest.setWaitForActiveShards(ActiveShardCount.NONE));
        var flushResponse = indicesAdmin().prepareFlush(indexName).get();
        assertNoFailures(flushResponse);
        // Up until now we're retaining all commits by the open PITs + the force merged commit and the newly flushed commit

        var blobsAfterFlushWhileSearchShardIsRecovering = shardCommitsContainer.listBlobs(operationPurpose).keySet();

        assertThat(blobsAfterFlushWhileSearchShardIsRecovering.containsAll(retainedBlobsBeforeUnpromotableShardMoved), is(true));
        assertThat(blobsAfterFlushWhileSearchShardIsRecovering, hasSize(retainedBlobsBeforeUnpromotableShardMoved.size() + 1));

        // At this point we should be retaining all commits since the unpromotable shard is still recovering,
        // and we'll be holding all commits in that case until the initialization finishes
        var forceMergeResponse = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertNoFailures(forceMergeResponse);

        var blobsAfterSecondForceMerge = shardCommitsContainer.listBlobs(operationPurpose).keySet();

        assertThat(blobsAfterSecondForceMerge.containsAll(blobsAfterFlushWhileSearchShardIsRecovering), is(true));

        var latestUploadedBcc = commitService.getLatestUploadedBcc(shardId);
        assertThat(latestUploadedBcc, is(notNullValue()));
        var latestUploadedBlob = StatelessCompoundCommit.blobNameFromGeneration(latestUploadedBcc.primaryTermAndGeneration().generation());

        var testScenario = randomFrom(PITRetentionTestScenarios.values());
        switch (testScenario) {
            case SUCCESSFUL_RELOCATION -> {
                // Do nothing
            }
            case FAIL_SOURCE -> failSearchShard(indexName, firstSearchNode);
            case FAIL_TARGET -> {
                // We want to abort the relocation, hence we revert the allocation filter to only include the first search node
                updateIndexSettings(
                    Settings.builder().put("index.routing.allocation.include._name", String.join(",", indexName, firstSearchNode)),
                    indexName
                );
                failSearchShard(indexName, newSearchNode);
            }
            case STOP_SOURCE_NODE -> internalCluster().stopNode(firstSearchNode);
            case STOP_TARGET_NODE -> internalCluster().stopNode(newSearchNode);
            default -> throw new IllegalStateException("Unexpected value: " + testScenario);
        }

        delayShardStartedMessages.set(false);
        // If the shard was marked as failed this will be ignored
        CheckedRunnable<Exception> startedShardMessage;
        while ((startedShardMessage = delayedShardStartedMessages.poll()) != null) {
            startedShardMessage.run();
        }
        ensureGreen(indexName);

        if (testScenario != PITRetentionTestScenarios.STOP_SOURCE_NODE) {
            // TODO: We should remove this once PIT transfers are in place
            // If we close the PITS, the retained commits should be released
            closePITs(openPITs);
        }

        // TODO: This should fail once the PIT hand-off is in place
        assertBusy(() -> assertThat(shardCommitsContainer.listBlobs(operationPurpose).keySet(), is(equalTo(Set.of(latestUploadedBlob)))));
    }

    private static void failSearchShard(String indexName, String searchNode) {
        var indicesService = internalCluster().getInstance(IndicesService.class, searchNode);
        var indexShard = indicesService.indexService(resolveIndex(indexName)).getShard(0);
        ShardStateAction shardStateAction = internalCluster().getInstance(ShardStateAction.class, searchNode);
        ShardRouting shardRouting = indexShard.routingEntry();
        assertThat("Unexpected shard role", shardRouting.role(), equalTo(ShardRouting.Role.SEARCH_ONLY));
        PlainActionFuture<Void> listener = new PlainActionFuture<>();
        shardStateAction.remoteShardFailed(
            indexShard.shardId(),
            shardRouting.allocationId().getId(),
            indexShard.getOperationPrimaryTerm(),
            true,
            "broken",
            new Exception("boom remote"),
            listener
        );
        listener.actionGet();
    }

    private static void closePITs(List<BytesReference> openPITs) throws Exception {
        for (BytesReference pitID : openPITs) {
            var closePITRequest = new ClosePointInTimeRequest(pitID);
            var closePITResponse = client().execute(TransportClosePointInTimeAction.TYPE, closePITRequest).get();
            assertThat(closePITResponse.isSucceeded(), is(true));
        }
    }

    enum PITRetentionTestScenarios {
        SUCCESSFUL_RELOCATION,
        FAIL_SOURCE,
        FAIL_TARGET,
        STOP_TARGET_NODE,
        STOP_SOURCE_NODE,
        // TODO: inject failures into the unpromotable relocation hand-off
    }

    private int indexDocsAndFlush(String indexName) {
        int numDocsBeforeOpenScroll = randomIntBetween(10, 20);
        indexDocs(indexName, numDocsBeforeOpenScroll);
        flush(indexName);
        return numDocsBeforeOpenScroll;
    }

    private static void assertTranslogBlobsExist(
        Set<TranslogReplicator.BlobTranslogFile> shouldExist,
        ObjectStoreService objectStoreService
    ) throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : shouldExist) {
            assertTrue(objectStoreService.getTranslogBlobContainer().blobExists(operationPurpose, translogFile.blobName()));
        }
    }

    private static void assertTranslogBlobsDoNotExist(
        Set<TranslogReplicator.BlobTranslogFile> doNotExist,
        ObjectStoreService objectStoreService
    ) throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : doNotExist) {
            assertFalse(objectStoreService.getTranslogBlobContainer().blobExists(operationPurpose, translogFile.blobName()));
        }
    }
}

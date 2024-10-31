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
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryPlugin;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryStrategy;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessClusterIntegrityStressIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), StatelessMockRepositoryPlugin.class);
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
            .put(StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(), "100ms");
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.shard_files_deletes:debug,"
            + "org.elasticsearch.blobcache.shared.SharedBlobCacheService:warn," // disable logs of "No free regions ..."
            + "co.elastic.elasticsearch.stateless.commits.StatelessCommitService:debug,"
            + "co.elastic.elasticsearch.stateless.recovery:debug,"
            + "org.elasticsearch.indices.recovery:debug",
        reason = "ensure shard file deletion on DEBUG level"
    )
    public void testRandomActivities() throws InterruptedException {
        final var trackedCluster = new TrackedCluster();
        trackedCluster.run();
    }

    /**
     * TrackedNode has a semaphore that can be acquired when performing node wide operation.
     * Currently the semaphore only has one permit which is equivalent to a lock. However, using
     * semaphore here so that future expansion is easier.
     */
    record TrackedNode(String name, boolean isCurrentMaster, boolean isIndexing, Semaphore permit) {
        TrackedNode(String name, boolean isCurrentMaster, boolean isIndexing) {
            this(name, isCurrentMaster, isIndexing, new Semaphore(1));
        }
    }

    /**
     * @param docIds The IDs of docs that should be visible to search. This tracking is approximate
     *               in that all these IDs should be visible but there maybe more IDs that are also
     *               visible but not tracked here.
     */
    record TrackedIndex(Index index, int numberOfShards, int numberOfReplicas, Set<String> docIds) {}

    private class TrackedCluster {
        private final Map<String, TrackedNode> nodes;
        private final Map<String, TrackedIndex> indices;
        private final ThreadPool threadPool;
        private final ExecutorService clientExecutor;
        private final ExecutorService serverExecutor;
        private final StatelessMockRepositoryStrategy statelessMockRepositoryStrategy;
        private final AtomicBoolean metUnexpectedError = new AtomicBoolean(false);
        private final Semaphore clusterPermit = new Semaphore(1);
        private final int targetUploads;
        private final AtomicInteger targetUploadsCounter;
        private final CountDownLatch stopLatch = new CountDownLatch(1);
        // The per mille chance of shard movement, 32 means 32 in 1000, i.e. 32‰
        private final int shardMovementChance = 32;
        private final int flushChance = 32;
        private final int forceMergeChance = 16;
        private final int nodeMovementChance = 8;
        private final int shardFailChance = 4;
        private final int searchChance = 250;
        private final int indexingChance = 1000;
        private final TimeValue defaultTestTimeout = TimeValue.timeValueSeconds(30);

        TrackedCluster() {
            final var numberOfIndexingNodes = between(3, 5);
            final var numberOfSearchNodes = between(2, 3);
            final var numberOfIndices = between(1, 5);
            this.targetUploads = numberOfIndices * 100;
            logger.info(
                "--> running test with [{}] indexing nodes, [{}] search nodes, [{}] indices, [{}] max_commits, [{}] target uploads",
                numberOfIndexingNodes,
                numberOfSearchNodes,
                numberOfIndices,
                getUploadMaxCommits(),
                this.targetUploads
            );
            this.targetUploadsCounter = new AtomicInteger(this.targetUploads);
            this.statelessMockRepositoryStrategy = new StatelessMockRepositoryStrategy() {
                @Override
                public void blobContainerWriteBlobAtomic(
                    CheckedRunnable<IOException> originalRunnable,
                    OperationPurpose purpose,
                    String blobName,
                    InputStream inputStream,
                    long blobSize,
                    boolean failIfAlreadyExists
                ) throws IOException {
                    super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                    if (StatelessCompoundCommit.startsWithBlobPrefix(blobName)) {
                        if (TrackedCluster.this.targetUploadsCounter.decrementAndGet() == 0) {
                            stopLatch.countDown();
                        }
                    }
                }
            };

            this.nodes = ConcurrentCollections.newConcurrentMap();
            IntStream.range(0, numberOfIndexingNodes)
                .forEach(ignore -> startMasterAndIndexNode(Settings.EMPTY, statelessMockRepositoryStrategy));
            startSearchNodes(numberOfSearchNodes).forEach(name -> this.nodes.put(name, new TrackedNode(name, false, false)));
            int totalNodes = numberOfIndexingNodes + numberOfSearchNodes;
            ensureStableCluster(totalNodes);

            final String masterName = internalCluster().getMasterName();
            this.nodes.put(masterName, new TrackedNode(masterName, true, true));
            Arrays.stream(internalCluster().getNodeNames()).forEach(name -> {
                if (this.nodes.containsKey(name) == false) {
                    this.nodes.put(name, new TrackedNode(name, false, true));
                }
            });
            assertThat(this.nodes.size(), equalTo(totalNodes));

            this.indices = new HashMap<>();
            for (int i = 0; i < numberOfIndices; i++) {
                final var indexName = "index-" + i;
                final var numberOfShards = between(1, 5);
                final var numberOfReplicas = between(1, 2);
                logger.info("--> creating index [{}] with [{}] shards [{}] replicas", indexName, numberOfShards, numberOfReplicas);
                createIndex(
                    indexName,
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                        .build()
                );
                this.indices.put(
                    indexName,
                    new TrackedIndex(resolveIndex(indexName), numberOfShards, numberOfReplicas, ConcurrentCollections.newConcurrentSet())
                );
            }
            ensureGreen(this.indices.keySet().toArray(String[]::new));

            this.threadPool = new TestThreadPool(
                "StatelessClusterIntegrityStressIT",
                new FixedExecutorBuilder(Settings.EMPTY, "client", 2, -1, "client", EsExecutors.TaskTrackingConfig.DEFAULT),
                new FixedExecutorBuilder(Settings.EMPTY, "server", 2, -1, "server", EsExecutors.TaskTrackingConfig.DEFAULT)
            );
            this.clientExecutor = this.threadPool.executor("client");
            this.serverExecutor = this.threadPool.executor("server");
        }

        void run() throws InterruptedException {
            final long startTime = threadPool.relativeTimeInMillis();

            runBulkIndex();
            runBulkIndex(); // run 2 streams of indexing
            runSearch();
            runFlush();
            runForceMerge();
            runRelocateIndexingShard();
            // TODO: relocating search shard
            runFailShard();
            runRestartNode();
            runReplaceIndexingNode();
            // TODO: graceful shutdown with shutdown API
            runIsolateIndexingNode();
            // TODO: update/delete docs once ES-7496 is resolved
            // TODO: Create/delete indices
            // TODO: data-stream and rollover
            // TODO: master failure
            // TODO: snapshots
            // TODO: slow/disruptive blobstore

            final int timeoutInSeconds = 60;
            logger.info("--> waiting for test to complete in [{}] seconds", timeoutInSeconds);
            if (stopLatch.await(timeoutInSeconds, TimeUnit.SECONDS)) {
                if (metUnexpectedError.get()) {
                    logger.info("--> test stopped due to unexpected error");
                } else {
                    logger.info(
                        "--> completed target uploads [{}] in [{}] seconds, finishing test",
                        targetUploads,
                        TimeValue.timeValueMillis(threadPool.relativeTimeInMillis() - startTime).getSeconds()
                    );
                    assertThat(targetUploadsCounter.get(), lessThanOrEqualTo(0));
                }
            } else {
                logger.info(
                    "--> did not complete target uploads [{}] in [{}] seconds, giving up remaining [{}]",
                    targetUploads,
                    timeoutInSeconds,
                    targetUploadsCounter.get()
                );
            }

            metUnexpectedError.set(true);
            logger.info("--> terminating thread pool");
            final boolean terminated = ThreadPool.terminate(threadPool, 40, TimeUnit.SECONDS);
            logger.info("--> thread pool terminated [{}]", terminated);

            ensureStableCluster(nodes.size(), masterNodeName());
            ensureGreen(); // All indices should be in good shape

            // All acknowledged data are available
            logger.info("--> refresh before checking indices data");
            refresh();
            for (TrackedIndex trackedIndex : indices.values()) {
                logger.info("--> checking data for index [{}]", trackedIndex.index.getName());
                final Set<String> docIds = Set.copyOf(trackedIndex.docIds); // copy it once in case it changes during search
                assertResponse(
                    prepareSearch(trackedIndex.index.getName()).setQuery(QueryBuilders.idsQuery().addIds(docIds.toArray(String[]::new)))
                        .setSize(0)
                        .setTrackTotalHits(true),
                    searchResponse -> {
                        assertNoFailures(searchResponse);
                        assertThat(searchResponse.getHits().getTotalHits().value(), greaterThanOrEqualTo((long) docIds.size()));
                    }
                );
            }
            logger.info("--> end of test");
        }

        NamedReleasable acquirePermitsForClusterAndIndexingNode() {
            return acquirePermitsForClusterAndNode(this::nonMasterIndexingNodes);
        }

        NamedReleasable acquirePermitForIndexingNode() {
            return acquirePermitForNodeFrom(this::nonMasterIndexingNodes);
        }

        NamedReleasable acquirePermitForSearchNode() {
            return acquirePermitForNodeFrom(this::searchNodes);
        }

        /**
         * Acquire permit for one node from the {@code nodesSupplier}.
         * @param nodesSupplier Provides the candidate nodes for acquiring permit
         * @return A releasable if permit acquired successfully or {@code NamedReleasable.EMPTY} otherwise.
         */
        private NamedReleasable acquirePermitForNodeFrom(Supplier<List<TrackedNode>> nodesSupplier) {
            try (var localReleasable = new TransferableReleasables()) {
                for (TrackedNode trackedNode : nodesSupplier.get()) {
                    if (trackedNode.permit().tryAcquire()) {
                        localReleasable.add(trackedNode.permit::release);
                        return new NamedReleasable(trackedNode.name, localReleasable.transfer());
                    }
                }
            }
            return NamedReleasable.EMPTY;
        }

        /**
         * Similar to {@link TrackedCluster#acquirePermitForNodeFrom} but also acquire permit for the cluster first.
         * @param nodesSupplier Provides the candidate nodes for acquiring permit
         * @return A releasable if permit acquired successfully or {@code NamedReleasable.EMPTY} otherwise.
         */
        private NamedReleasable acquirePermitsForClusterAndNode(Supplier<List<TrackedNode>> nodesSupplier) {
            try (var localReleasable = new TransferableReleasables()) {
                if (clusterPermit.tryAcquire()) {
                    localReleasable.add(clusterPermit::release);
                    for (TrackedNode trackedNode : nodesSupplier.get()) {
                        if (trackedNode.permit.tryAcquire()) {
                            localReleasable.add(trackedNode.permit::release);
                            return new NamedReleasable(trackedNode.name, localReleasable.transfer());
                        }
                    }
                }
            }
            return NamedReleasable.EMPTY;
        }

        TrackedNode masterNode() {
            return nodes.values()
                .stream()
                .filter(TrackedNode::isCurrentMaster)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no master is found"));
        }

        String masterNodeName() {
            return masterNode().name;
        }

        private List<TrackedNode> nonMasterIndexingNodes() {
            return shuffledList(
                nodes.values().stream().filter(trackedNode -> trackedNode.isIndexing && trackedNode.isCurrentMaster == false).toList()
            );
        }

        private List<TrackedNode> searchNodes() {
            return shuffledList(nodes.values().stream().filter(trackedNode -> trackedNode.isIndexing == false).toList());
        }

        public void ensureGreenIgnoreNodeDisconnection(int numNodes, String... indices) throws Exception {
            assertBusy(() -> {
                try {
                    final var clusterHealthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, indices).waitForStatus(
                        ClusterHealthStatus.GREEN
                    ).waitForNoInitializingShards(true).waitForNoRelocatingShards(true).waitForEvents(Priority.LANGUID);
                    if (numNodes != 0) {
                        clusterHealthRequest.waitForNodes(Integer.toString(numNodes));
                    }
                    final ClusterHealthResponse response = client(masterNodeName()).admin()
                        .cluster()
                        .health(clusterHealthRequest)
                        .actionGet(defaultTestTimeout);
                    assertThat(response.getStatus(), is(ClusterHealthStatus.GREEN));
                } catch (Exception e) {
                    // ConnectTransportException and ElasticsearchException: faulty node
                    // retry on connection error since node may fail to ack cluster state update due to restart etc
                    throw new AssertionError(e);
                }
            }, defaultTestTimeout.getSeconds(), TimeUnit.SECONDS);
        }

        private void enqueueAction(Executor executor, CheckedRunnable<Exception> action) {
            if (metUnexpectedError.get()) {
                return;
            }
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(between(1, 500)), executor, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    failTest(e);
                }

                @Override
                protected void doRun() throws Exception {
                    if (metUnexpectedError.get()) {
                        return;
                    }
                    try {
                        action.run();
                    } catch (AssertionError ae) {
                        // AbstractRunnable does not catch AssertionError. We catch it ourselves to ensure the test stop promptly
                        // when encountering any error. Otherwise, it may keep running up to the timeout. It will still report failure
                        // but takes longer.
                        failTest(ae);
                    }
                }

                @Override
                public void onRejection(Exception e) {
                    // ok, shutting down
                }

                @Override
                public void onAfter() {
                    if (metUnexpectedError.get()) {
                        return;
                    }
                    enqueueAction(executor, action);
                }

                private void failTest(Throwable throwable) {
                    if (metUnexpectedError.compareAndSet(false, true)) {
                        final AssertionError assertionError;
                        if (throwable instanceof AssertionError == false) {
                            assertionError = new AssertionError("unexpected", throwable);
                        } else {
                            assertionError = (AssertionError) throwable;
                        }
                        logger.error("--> test failed unexpectedly", assertionError);
                        stopLatch.countDown();
                        throw assertionError;
                    } else {
                        logger.info("--> ignore exception since test is stopping", throwable);
                    }
                }
            });
        }

        private void runBulkIndex() {
            enqueueAction(clientExecutor, () -> {
                if (between(1, 1000) > indexingChance) {
                    return;
                }
                final var numDocs = between(indices.size(), indices.size() * 5);
                try {
                    final var bulkRequest = client().prepareBulk();
                    final WriteRequest.RefreshPolicy refreshPolicy = rarely()
                        ? WriteRequest.RefreshPolicy.WAIT_UNTIL
                        : randomFrom(WriteRequest.RefreshPolicy.IMMEDIATE, WriteRequest.RefreshPolicy.NONE);
                    final Set<String> targetIndices = new HashSet<>();
                    IntStream.range(0, numDocs).forEach(i -> {
                        final String index = randomFrom(indices.keySet());
                        targetIndices.add(index);
                        bulkRequest.add(
                            prepareIndex(index).setSource(
                                "field",
                                randomUnicodeOfLengthBetween(20, 100),
                                "custom",
                                "value",
                                "number",
                                randomLongBetween(0, numDocs)
                            )
                        );
                    });
                    bulkRequest.setRefreshPolicy(refreshPolicy);
                    logger.info(
                        "--> bulk indexing [{}] docs into {} with refresh policy [{}]",
                        numDocs,
                        targetIndices.stream().sorted().toList(),
                        refreshPolicy
                    );
                    final BulkResponse bulkResponse = bulkRequest.get(defaultTestTimeout);

                    // Add docIds that are visible after refresh
                    if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                        Arrays.stream(bulkResponse.getItems()).forEach(bulkItemResponse -> {
                            if (bulkItemResponse.isFailed() == false) {
                                indices.get(bulkItemResponse.getIndex()).docIds.add(bulkItemResponse.getId());
                            }
                        });
                    } else {
                        logger.info("--> refreshing {} separately after bulk indexing", targetIndices);
                        final var refreshResponse = client().admin()
                            .indices()
                            .prepareRefresh(targetIndices.toArray(String[]::new))
                            .get(defaultTestTimeout);
                        logger.info("--> completed refreshing {}", targetIndices);
                        // For simplicity, only track docIds if there is no failed shards
                        if (refreshResponse.getFailedShards() == 0) {
                            Arrays.stream(bulkResponse.getItems()).forEach(bulkItemResponse -> {
                                if (bulkItemResponse.isFailed() == false) {
                                    indices.get(bulkItemResponse.getIndex()).docIds.add(bulkItemResponse.getId());
                                }
                            });
                        }
                    }
                } catch (Exception e) {
                    // Failure can happen the shard is failed or node restart/replace/isolated concurrently. Just ignore them
                    logger.info(Strings.format("--> exception bulk indexing [%s] docs", numDocs), e);
                } finally {
                    logger.info("--> completed bulk indexing [{}] docs", numDocs);
                }
            });
        }

        private void runSearch() {
            enqueueAction(clientExecutor, () -> {
                if (between(1, 1000) > searchChance) {
                    return;
                }
                final TrackedIndex trackedIndex = randomFrom(indices.values());
                final String indexName = trackedIndex.index.getName();
                // Get the expected doc size before search since concurrent indexing may add docs that are not visible to this search
                final int expectedNumberOfDocs = trackedIndex.docIds.size();
                SearchResponse searchResponse = null;
                try {
                    // TODO: ESIntegTestCase randomize `search.low_level_cancellation` which could trip
                    // MockSearchService#assertNoInFlightContext when it is set to `false` and coordinating node is stopped
                    // while a search is ongoing. For now, use the master node as the coordinating node to avoid shutdown
                    // See also https://github.com/elastic/elasticsearch/issues/115199
                    final SearchRequestBuilder searchRequest = client(masterNodeName()).prepareSearch(indexName).setTrackTotalHits(true);
                    if (randomBoolean()) {
                        searchRequest.setSize(between(0, expectedNumberOfDocs));
                    }
                    final var testSearchType = randomFrom(TestSearchType.values());
                    logger.info("--> searching index [{}] with [{}]", indexName, testSearchType);
                    switch (testSearchType) {
                        case MATCH_ALL -> searchRequest.setQuery(QueryBuilders.matchAllQuery());
                        case MATCH_CUSTOM -> searchRequest.setQuery(QueryBuilders.termQuery("custom", "value"));
                        case SUM -> searchRequest.addAggregation(sum("sum").field("number"));
                    }
                    searchResponse = searchRequest.get(defaultTestTimeout);
                    // For simplicity, only assert response size if there is no failed shards
                    if (searchResponse.getFailedShards() == 0) {
                        assertThat(searchResponse.getHits().getTotalHits().value(), greaterThanOrEqualTo((long) expectedNumberOfDocs));
                    }
                } catch (Exception e) {
                    // Failure can happen the shard is failed or node restart/replace/isolated concurrently. Just ignore them
                    logger.info(Strings.format("--> exception searching index [%s]", indexName), e);
                } finally {
                    if (searchResponse != null) {
                        searchResponse.decRef();
                    }
                    logger.info("--> completed searching index [{}]", indexName);
                }
            });
        }

        private void runFlush() {
            enqueueAction(clientExecutor, () -> {
                if (between(1, 1000) > flushChance) {
                    return;
                }
                final String indexName = randomFrom(indices.keySet());
                try {
                    logger.info("--> flushing index [{}]", indexName);
                    final boolean force = randomBoolean();
                    final FlushRequestBuilder flushRequest = client().admin()
                        .indices()
                        .prepareFlush(indexName)
                        .setForce(force)
                        .setWaitIfOngoing(randomBoolean() || force);
                    flushRequest.get(defaultTestTimeout);
                } catch (Exception e) {
                    logger.info(Strings.format("--> exception flushing index [%s]", indexName), e);
                } finally {
                    logger.info("--> completed flushing index [{}]", indexName);
                }
            });
        }

        private void runForceMerge() {
            enqueueAction(clientExecutor, () -> {
                if (between(1, 1000) > forceMergeChance) {
                    return;
                }
                final String indexName = randomFrom(indices.keySet());
                try {
                    logger.info("--> force merging index [{}]", indexName);
                    final ForceMergeRequestBuilder forceMergeRequest = client().admin()
                        .indices()
                        .prepareForceMerge(indexName)
                        .setMaxNumSegments(1);
                    forceMergeRequest.get(defaultTestTimeout);
                } catch (Exception e) {
                    logger.info(Strings.format("--> exception merging index [%s]", indexName), e);
                } finally {
                    logger.info("--> completed force merging index [{}]", indexName);
                }
            });
        }

        private void runRelocateIndexingShard() {
            enqueueAction(serverExecutor, () -> {
                if (between(1, 1000) > shardMovementChance) {
                    return;
                }
                final Releasable clusterReleasable = () -> {};
                try (clusterReleasable) {
                    final TrackedIndex trackedIndex = randomFrom(indices.values());
                    final int shardId = between(0, trackedIndex.numberOfShards - 1);
                    final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, masterNodeName());
                    final ClusterState state = clusterService.state();
                    final String currentNodeId = state.routingTable()
                        .index(trackedIndex.index)
                        .shard(shardId)
                        .primaryShard()
                        .currentNodeId();
                    final DiscoveryNode currentNode = state.getNodes().get(currentNodeId);

                    logger.info("--> relocating indexing shard [{}] away from [{}]", trackedIndex.index.getName(), currentNode.getName());
                    assertBusy(() -> {
                        try {
                            assertAcked(
                                client(masterNodeName()).admin()
                                    .indices()
                                    .prepareUpdateSettings(trackedIndex.index.getName())
                                    .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", currentNode.getName()))
                            );
                        } catch (Exception e) {
                            // ConnectTransportException and ElasticsearchException: faulty node
                            throw new AssertionError(e); // retry on connection error due to node restart etc
                        }
                    }, defaultTestTimeout.getSeconds(), TimeUnit.SECONDS);
                    logger.info("--> updated indexing shard [{}] setting for relocation", trackedIndex.index.getName());
                    ensureGreenIgnoreNodeDisconnection(0, trackedIndex.index.getName());
                    logger.info("--> completed relocating shard [{}][{}]", trackedIndex.index.getName(), shardId);
                }
            });
        }

        private void runFailShard() {
            enqueueAction(serverExecutor, () -> {
                if (between(1, 1000) > shardFailChance) {
                    return;
                }
                final boolean searchShard = randomBoolean() && randomBoolean(); // more likely to fail indexing shard
                final Supplier<NamedReleasable> permitSupplier = searchShard
                    ? this::acquirePermitForSearchNode
                    : this::acquirePermitForIndexingNode;
                try (var namedReleasable = permitSupplier.get()) {
                    if (namedReleasable == NamedReleasable.EMPTY) {
                        return;
                    }
                    final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
                    final var indicesService = internalCluster().getInstance(IndicesService.class, namedReleasable.name);
                    final List<TrackedIndex> candidateIndices = shuffledList(List.copyOf(indices.values()));
                    for (var trackedIndex : candidateIndices) {
                        final var indexService = indicesService.indexService(trackedIndex.index);
                        if (indexService == null) {
                            continue;
                        }
                        final Set<Integer> shardIds = indexService.shardIds();
                        if (shardIds.isEmpty()) {
                            continue;
                        }
                        final IndexShard indexShard = indexService.getShardOrNull(randomFrom(shardIds));
                        // non-started shard may not even have engine and cannot fail
                        if (indexShard == null || indexShard.state() != IndexShardState.STARTED) {
                            continue;
                        }
                        // TODO: This could fail an allocation source shard
                        final String allocationId = indexShard.routingEntry().allocationId().getId();
                        logger.info(
                            "--> failing {} shard {} on node [{}]",
                            (searchShard ? "search" : "indexing"),
                            indexShard.shardId(),
                            namedReleasable.name
                        );
                        final var unassignedLatch = new CountDownLatch(1);
                        final ClusterStateListener clusterStateListener = clusterChangedEvent -> {
                            if (clusterChangedEvent.state().routingTable().getByAllocationId(indexShard.shardId(), allocationId) == null) {
                                unassignedLatch.countDown();
                            }
                        };
                        masterClusterService.addListener(clusterStateListener);
                        indexShard.failShard("broken", new Exception("boom local " + (searchShard ? "search" : "indexing") + " shard"));
                        safeAwait(unassignedLatch);
                        masterClusterService.removeListener(clusterStateListener);
                        ensureGreenIgnoreNodeDisconnection(0, trackedIndex.index.getName());
                        logger.info("--> completed failing {} shard {}", (searchShard ? "search" : "indexing"), indexShard.shardId());
                        break;
                    }
                }
            });
        }

        private void runRestartNode() {
            enqueueAction(serverExecutor, () -> {
                if (between(1, 1000) > nodeMovementChance) {
                    return;
                }
                // See also https://github.com/elastic/elasticsearch/issues/115056
                final boolean restartIndexingNode = true || randomBoolean();
                Supplier<NamedReleasable> permitSupplier = restartIndexingNode
                    ? this::acquirePermitForIndexingNode
                    : this::acquirePermitForSearchNode;
                try (var namedReleasable = permitSupplier.get()) {
                    if (namedReleasable == NamedReleasable.EMPTY) {
                        return;
                    }
                    logger.info("--> restarting {} node [{}]", restartIndexingNode ? "indexing" : "search", namedReleasable.name);
                    internalCluster().restartNode(namedReleasable.name, new InternalTestCluster.RestartCallback() {
                        public boolean validateClusterForming() {
                            return false;
                        }
                    });
                    ensureStableCluster(nodes.size(), masterNodeName());
                    logger.info("--> completed restarting {} node [{}]", restartIndexingNode ? "indexing" : "search", namedReleasable.name);
                }
            });
        }

        private void runReplaceIndexingNode() {
            enqueueAction(serverExecutor, () -> {
                if (between(1, 1000) > nodeMovementChance) {
                    return;
                }
                try (var namedReleasable = acquirePermitsForClusterAndIndexingNode()) {
                    if (namedReleasable == NamedReleasable.EMPTY) {
                        return;
                    }
                    logger.info("--> replacing indexing node [{}]", namedReleasable.name);
                    final String newNodeName = startMasterAndIndexNode(Settings.EMPTY, statelessMockRepositoryStrategy);
                    ensureStableCluster(nodes.size() + 1, masterNodeName());
                    logger.info("--> added new indexing node [{}]", newNodeName);
                    final TrackedNode removed = nodes.remove(namedReleasable.name);
                    assertThat(removed.name, equalTo(namedReleasable.name));
                    assertThat(removed, notNullValue());
                    nodes.put(newNodeName, new TrackedNode(newNodeName, false, true));
                    logger.info("--> stopping old indexing node [{}]", namedReleasable.name);
                    internalCluster().stopNode(namedReleasable.name);
                    ensureStableCluster(nodes.size(), masterNodeName());
                    assertThat(internalCluster().getNodeNames(), not(arrayContaining(removed.name)));
                    logger.info("--> completed replacing indexing node [{}] with [{}]", removed.name, newNodeName);
                }
            });
        }

        private void runIsolateIndexingNode() {
            enqueueAction(serverExecutor, () -> {
                if (between(1, 1000) > nodeMovementChance) {
                    return;
                }
                final boolean isolateFromMaster = randomBoolean();
                final MockTransportService masterTransportService = MockTransportService.getInstance(masterNodeName());
                try (var namedReleasable = acquirePermitsForClusterAndIndexingNode()) {
                    if (namedReleasable == NamedReleasable.EMPTY) {
                        return;
                    }
                    final PlainActionFuture<Void> isolatedFuture = new PlainActionFuture<>();
                    final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
                    masterClusterService.addListener(new ClusterStateListener() {
                        @Override
                        public void clusterChanged(ClusterChangedEvent clusterChangedEvent) {
                            if (isolatedFuture.isDone() == false
                                && clusterChangedEvent.nodesDelta()
                                    .removedNodes()
                                    .stream()
                                    .anyMatch(d -> d.getName().equals(namedReleasable.name))) {
                                isolatedFuture.onResponse(null);
                                masterClusterService.removeListener(this);
                            }
                        }
                    });
                    logger.info(
                        "--> isolating indexing node [{}] from {}",
                        namedReleasable.name,
                        isolateFromMaster ? "master" : "all other nodes"
                    );
                    if (isolateFromMaster) {
                        final MockTransportService nodeToIsolateTransportService = MockTransportService.getInstance(namedReleasable.name);
                        masterTransportService.addUnresponsiveRule(nodeToIsolateTransportService);
                    } else {
                        final var networkDisruption = new NetworkDisruption(
                            new NetworkDisruption.TwoPartitions(
                                Set.of(namedReleasable.name),
                                nodes.keySet()
                                    .stream()
                                    .filter(name -> name.equals(namedReleasable.name) == false)
                                    .collect(Collectors.toUnmodifiableSet())
                            ),
                            NetworkDisruption.UNRESPONSIVE
                        );
                        internalCluster().setDisruptionScheme(networkDisruption);
                        networkDisruption.startDisrupting();
                    }
                    isolatedFuture.actionGet(defaultTestTimeout);
                    logger.info("--> isolated node [{}]", namedReleasable.name);
                    try {
                        final var healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT).waitForStatus(ClusterHealthStatus.YELLOW)
                            .waitForEvents(Priority.LANGUID)
                            .waitForNoRelocatingShards(false)
                            .waitForNoInitializingShards(false)
                            .waitForNodes(Integer.toString(nodes.size() - 1));
                        client(masterNodeName()).admin().cluster().health(healthRequest).actionGet(defaultTestTimeout);
                        logger.info("--> cluster is stable after node [{}] isolated", namedReleasable.name);
                    } finally {
                        logger.info("--> clearing isolation for node [{}]", namedReleasable.name);
                        if (isolateFromMaster) {
                            masterTransportService.clearAllRules();
                            ensureStableCluster(nodes.size(), masterNodeName());
                        } else {
                            internalCluster().clearDisruptionScheme();
                        }
                    }
                    logger.info("--> completed isolating indexing node [{}]", namedReleasable.name);
                }
            });
        }
    }

    // TODO: This class is copied from SnapshotStressTestsIT. Extract it as a common test helper.
    /**
     * Encapsulates a common pattern of trying to acquire a bunch of resources and then transferring ownership elsewhere on success,
     * but releasing them on failure.
     */
    private static class TransferableReleasables implements Releasable {

        private boolean transferred = false;
        private final List<Releasable> releasables = new ArrayList<>();

        <T extends Releasable> T add(T releasable) {
            assert transferred == false : "already transferred";
            releasables.add(releasable);
            return releasable;
        }

        Releasable transfer() {
            assert transferred == false : "already transferred";
            transferred = true;
            Collections.reverse(releasables);
            return () -> Releasables.close(releasables);
        }

        @Override
        public void close() {
            if (transferred == false) {
                Releasables.close(releasables);
            }
        }
    }

    private static class NamedReleasable implements Releasable {
        static final NamedReleasable EMPTY = new NamedReleasable(null, () -> {});

        private final String name;
        private final Releasable releasable;

        private NamedReleasable(String name, Releasable releasable) {
            this.name = name;
            this.releasable = releasable;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }

    private enum TestSearchType {
        MATCH_ALL,
        MATCH_CUSTOM,
        SUM
    }
}

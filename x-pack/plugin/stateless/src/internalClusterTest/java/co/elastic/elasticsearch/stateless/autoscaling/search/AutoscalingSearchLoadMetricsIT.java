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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.autoscaling.ServerlessAutoscalingPlugin;
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.NodeSearchLoadSnapshot;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.PublishNodeSearchLoadRequest;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.SearchLoadProbe;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.SearchLoadSampler;
import co.elastic.elasticsearch.stateless.autoscaling.search.load.TransportPublishSearchLoads;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.AutoscalingIndexingMetricsIT.longAwait;
import static co.elastic.elasticsearch.stateless.autoscaling.search.load.AverageSearchLoadSampler.SHARD_READ_EXECUTOR;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AutoscalingSearchLoadMetricsIT extends AbstractStatelessIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(ShutdownPlugin.class, ServerlessAutoscalingPlugin.class), super.nodePlugins());
    }

    public void testSearchMetricsArePublishedEventually() throws Exception {
        startMasterAndIndexNode();
        startSearchNode(
            Settings.builder()
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueMillis(100))
                .build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        assertBusy(() -> {
            var loadsBeforeSearch = getNodeSearchLoad();
            assertThat(loadsBeforeSearch.size(), equalTo(1));
            assertThat(loadsBeforeSearch.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsBeforeSearch.get(0).load(), equalTo(0.0));
        });

        indexDocs(indexName, 4000);
        refresh(indexName);

        var firstNonZeroPublishSearchLoadLatch = new CountDownLatch(1);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService2 = (MockTransportService) transportService;
            mockTransportService2.addSendBehavior((connection, requestId, action, request, options) -> {
                if (request instanceof PublishNodeSearchLoadRequest publishRequest && publishRequest.getSearchLoad() > 0) {
                    firstNonZeroPublishSearchLoadLatch.countDown();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        for (var i = 0; i < 10; i++) {
            assertNoFailures(prepareSearch(indexName).setQuery(QueryBuilders.termQuery("field", i)));
        }

        safeAwait(firstNonZeroPublishSearchLoadLatch);

        assertBusy(() -> {
            List<NodeSearchLoadSnapshot> loadsAfterSearch = getNodeSearchLoad();
            assertThat(loadsAfterSearch.size(), equalTo(1));
            assertThat(loadsAfterSearch.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterSearch.get(0).load(), greaterThan(0.0));
        });
    }

    public void testMaxTimeToClearQueueDynamicSetting() {
        startMasterAndIndexNode();
        updateClusterSettings(Settings.builder().put(SearchLoadProbe.MAX_TIME_TO_CLEAR_QUEUE.getKey(), TimeValue.timeValueSeconds(1)));
        var getSettingsResponse = clusterAdmin().execute(
            ClusterGetSettingsAction.INSTANCE,
            new ClusterGetSettingsAction.Request(TEST_REQUEST_TIMEOUT)
        ).actionGet();
        assertThat(
            getSettingsResponse.settings().get(SearchLoadProbe.MAX_TIME_TO_CLEAR_QUEUE.getKey()),
            equalTo(TimeValue.timeValueSeconds(1).getStringRep())
        );
    }

    public void testAutoscalingWithQueueSize() throws Exception {
        startMasterAndIndexNode();
        var searchNodeName = startSearchNode(
            // Reduce the time between publications, so we can expect at least one publication per second.
            Settings.builder()
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .put(SearchLoadProbe.MAX_TIME_TO_CLEAR_QUEUE.getKey(), TimeValue.timeValueMillis(1))
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
                .build()
        );

        final AtomicInteger searchLoadPublishSent = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishSearchLoads.NAME)) {
                    searchLoadPublishSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        assertBusy(() -> assertThat(searchLoadPublishSent.get(), equalTo(1)));
        var metrics = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class).getSearchTierMetrics();
        assertThat(metrics.toString(), metrics.getNodesLoad().size(), equalTo(1));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        // some write so that the Search EWMA is not zero
        indexDocs(indexName, randomIntBetween(5000, 10000));
        refresh(indexName);

        assertNoFailures(prepareSearch(indexName).setQuery(QueryBuilders.termQuery("field", "foo")));

        assertBusy(() -> {
            var metricsAfter = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class).getSearchTierMetrics();
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).load(), allOf(greaterThan(0.0), lessThanOrEqualTo(1.0)));
        });

        assertNoFailures(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()));

        // Block the executor workers to pile up searches
        var threadpool = internalCluster().getInstance(ThreadPool.class, searchNodeName);
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.SEARCH);
        final var executorThreads = threadpool.info(ThreadPool.Names.SEARCH).getMax();
        var barrier = new CyclicBarrier(executorThreads + 1);
        for (int i = 0; i < executorThreads; i++) {
            executor.execute(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        var futures = new ArrayList<ActionFuture<SearchResponse>>();
        try {
            var searchRequests = randomIntBetween(1000, 1050);
            for (int i = 0; i < searchRequests; i++) {
                futures.add(prepareSearch(indexName).setQuery(QueryBuilders.termQuery("field", i)).execute());
            }
            // Wait for at least one more publish
            assertBusy(() -> assertThat(searchLoadPublishSent.get(), greaterThan(1)));
            // We'd need an assertBusy since the second publish might still miss the recent load.
            // Eventually just because of queueing, the load will go above the current available threads
            assertBusy(() -> {
                var metricsAfter = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class).getSearchTierMetrics();
                assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
                assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
                assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).load(), greaterThan((double) executorThreads));
            }, 60, TimeUnit.SECONDS);
        } finally {
            barrier.await();
            for (var f : futures) {
                try {
                    var searchResponse = f.get();
                    searchResponse.decRef();
                } catch (Exception e) {
                    logger.info(e);
                }
            }
        }
    }

    public void testOngoingTasksAreReflectedInSearchLoad() throws Exception {
        startMasterAndIndexNode();
        // Reduce the time between publications, so we can expect at least one publication per second.
        var searchNodeName = startSearchNode(
            Settings.builder()
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
        final var metricPublicationBarrier = new CyclicBarrier(2);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishSearchLoads.NAME)) {
                    longAwait(metricPublicationBarrier);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        // Wait for a publication of the metrics
        longAwait(metricPublicationBarrier);
        var metrics = searchMetricsService.getSearchTierMetrics();
        assertThat(metrics.toString(), metrics.getNodesLoad().size(), equalTo(1));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).load(), closeTo(0.0, 0.1));

        // Block the executor workers to simulate long-running write tasks
        var threadpool = internalCluster().getInstance(ThreadPool.class, searchNodeName);
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.SEARCH);
        final var executorThreads = threadpool.info(ThreadPool.Names.SEARCH).getMax();
        var barrier = new CyclicBarrier(executorThreads + 1);
        for (int i = 0; i < executorThreads; i++) {
            executor.execute(() -> longAwait(barrier));
        }

        // Wait for another publication of the metrics
        longAwait(metricPublicationBarrier);

        try {
            // Eventually just because of the "long-running" tasks, the load will go up
            assertBusy(() -> {
                try {
                    var metricsAfter = searchMetricsService.getSearchTierMetrics();
                    assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
                    assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
                    assertThat(
                        metricsAfter.toString(),
                        metricsAfter.getNodesLoad().get(0).load(),
                        allOf(greaterThan(0.0), lessThanOrEqualTo((double) executorThreads))
                    );
                } finally {
                    longAwait(metricPublicationBarrier);
                }
            });
        } finally {
            longAwait(barrier);
        }
    }

    public void testShardReadLoadCausesMinimumAndZeroSearchLoad() throws Exception {
        startMasterAndIndexNode();
        var searchNode = startSearchNode(
            Settings.builder()
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueMillis(200))
                .put(SearchLoadSampler.SAMPLING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueMillis(30))
                .build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final var metricPublicationBarrier = new CyclicBarrier(2);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishSearchLoads.NAME)) {
                    longAwait(metricPublicationBarrier);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        // Wait for a publication of the metrics
        longAwait(metricPublicationBarrier);

        // Check pre-conditions: search load should be EXACT prior to SHARD_READ_EXECUTOR load.
        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
        var metrics = searchMetricsService.getSearchTierMetrics();
        assertThat(metrics.toString(), metrics.getNodesLoad().size(), equalTo(1));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));

        // Block the SHARD_READ_EXECUTOR workers to simulate download from S3.
        var threadPool = internalCluster().getInstance(ThreadPool.class, searchNode);
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadPool.executor(SHARD_READ_EXECUTOR);
        final var executorThreads = threadPool.info(SHARD_READ_EXECUTOR).getMax();
        var barrier = new CyclicBarrier(executorThreads + 1);
        AtomicBoolean loadEnabled = new AtomicBoolean(true);
        for (int i = 0; i < executorThreads; i++) {
            executor.execute(() -> {
                if (loadEnabled.get()) {
                    longAwait(barrier);
                }
            });
        }

        // Wait for another publication of the metrics.
        longAwait(metricPublicationBarrier);

        try {
            // Eventually just because of the "long-running" tasks in the SHARD_READ_EXECUTOR, the load will go up.
            assertBusy(() -> {
                try {
                    var metricsAfter = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class).getSearchTierMetrics();
                    assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
                    assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.MINIMUM));
                    assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).load(), closeTo(0.0, 0.01));
                } finally {
                    longAwait(metricPublicationBarrier);
                }
            });
        } finally {
            loadEnabled.set(false);
            longAwait(barrier);
        }

        // With subsequent sampling iterations, the EWMA of the SHARD_READ_EXECUTOR will go down, and search load will be reported as EXACT.
        assertBusy(() -> {
            try {
                var metricsAfter = searchMetricsService.getSearchTierMetrics();
                assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
                assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            } finally {
                longAwait(metricPublicationBarrier);
            }
        });
    }

    public void testMetricsAreRepublishedAfterMasterFailover() throws Exception {
        for (int i = 0; i < 2; i++) {
            startMasterNode();
        }
        startIndexNode();

        startSearchNode(
            Settings.builder()
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }
        refresh(indexName);

        assertNoFailures(prepareSearch(indexName).setQuery(QueryBuilders.termQuery("field", "foo")));

        assertBusy(() -> {
            var loadsAfterSearch = getNodeSearchLoad();
            assertThat(loadsAfterSearch.size(), equalTo(1));
            assertThat(loadsAfterSearch.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterSearch.get(0).load(), greaterThan(0.0));
        });

        internalCluster().stopCurrentMasterNode();

        assertBusy(() -> {
            var loadsAfterSearch = getNodeSearchLoad();
            assertThat(loadsAfterSearch.size(), equalTo(1));
            assertThat(loadsAfterSearch.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterSearch.get(0).load(), greaterThan(0.0));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1657")
    public void testMasterFailoverWithOnGoingMetricPublication() throws Exception {
        for (int i = 0; i < 2; i++) {
            startMasterNode();
        }
        startIndexNode();
        startSearchNode(
            Settings.builder()
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueMillis(100))
                .build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        assertBusy(() -> {
            var loadsBeforeSearch = getNodeSearchLoad();
            assertThat(loadsBeforeSearch.size(), equalTo(1));
            assertThat(loadsBeforeSearch.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsBeforeSearch.get(0).load(), equalTo(0.0));
        });

        var firstNonZeroPublishSearchLoadLatch = new CountDownLatch(1);
        MockTransportService mockTransportService = (MockTransportService) internalCluster().getCurrentMasterNodeInstance(
            TransportService.class
        );

        indexDocs(indexName, 4000);
        refresh(indexName);

        mockTransportService.addRequestHandlingBehavior(TransportPublishSearchLoads.NAME, (handler, request, channel, task) -> {
            if (request instanceof PublishNodeSearchLoadRequest publishRequest && publishRequest.getSearchLoad() > 0) {
                firstNonZeroPublishSearchLoadLatch.countDown();
            }
        });

        for (var i = 0; i < 10; i++) {
            assertNoFailures(prepareSearch(indexName).setQuery(QueryBuilders.termQuery("field", i)));
        }

        safeAwait(firstNonZeroPublishSearchLoadLatch);
        internalCluster().stopCurrentMasterNode();

        assertBusy(() -> {
            List<NodeSearchLoadSnapshot> loadsAfterSearch = getNodeSearchLoad();
            assertThat(loadsAfterSearch.size(), equalTo(1));
            assertThat(loadsAfterSearch.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterSearch.get(0).load(), greaterThan(0.0));
        });
    }

    public void testMetricsAreRepublishedAfterMasterNodeHasToRecoverStateFromStore() throws Exception {
        var masterNode = startMasterNode();
        startIndexNode();
        startSearchNode(
            Settings.builder()
                .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }
        refresh(indexName);

        assertNoFailures(prepareSearch(indexName).setQuery(QueryBuilders.termQuery("field", "foo")));

        assertBusy(() -> {
            var loadsAfterSearch = getNodeSearchLoad();
            assertThat(loadsAfterSearch.size(), equalTo(1));
            assertThat(loadsAfterSearch.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterSearch.get(0).load(), greaterThan(0.0));
        });

        internalCluster().restartNode(masterNode);

        // After the master node is restarted the index load is re-populated from the search node
        assertBusy(() -> {
            var loadsAfterSearch = getNodeSearchLoad();
            assertThat(loadsAfterSearch.size(), equalTo(1));
            assertThat(loadsAfterSearch.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterSearch.get(0).load(), greaterThan(0.0));
        });
    }

    public void testRemoveSearchLoadWhenNodeRemoved() throws Exception {
        startMasterAndIndexNode();
        final int numSearchNodes = between(2, 5);
        final int totalNodes = numSearchNodes + 1;
        List<String> searchNodes = new ArrayList<>();
        IntStream.range(0, numSearchNodes).forEach(i -> {
            searchNodes.add(
                startSearchNode(
                    Settings.builder()
                        .put(SearchLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueMillis(10))
                        .build()
                )
            );
        });

        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        assertBusy(() -> {
            final List<NodeSearchLoadSnapshot> nodeSearchLoad = getNodeSearchLoad();
            assertThat(nodeSearchLoad, hasSize(numSearchNodes));
            assertTrue(nodeSearchLoad.stream().allMatch(searchLoad -> searchLoad.metricQuality() == MetricQuality.EXACT));
        });

        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final List<DiscoveryNode> shuttingDownNodes = randomNonEmptySubsetOf(
            clusterService.state().nodes().getDataNodes().values().stream().filter(node -> searchNodes.contains(node.getName())).toList()
        );
        logger.info("--> Marking nodes {} for removal", shuttingDownNodes);
        markNodesForShutdown(
            shuttingDownNodes,
            Arrays.stream(SingleNodeShutdownMetadata.Type.values()).filter(SingleNodeShutdownMetadata.Type::isRemovalType).toList()
        );

        // all nodes, even if marked for removal, should still be reporting search load
        final List<NodeSearchLoadSnapshot> nodeSearchLoad = getNodeSearchLoad();
        assertThat(nodeSearchLoad, hasSize(numSearchNodes));
        assertTrue(nodeSearchLoad.stream().allMatch(searchLoad -> searchLoad.metricQuality() == MetricQuality.EXACT));

        for (var node : shuttingDownNodes) {
            logger.info("--> stopping node {}", node.getName());
            internalCluster().stopNode(node.getName());
        }
        ensureStableCluster(totalNodes - shuttingDownNodes.size());

        assertBusy(() -> {
            final List<NodeSearchLoadSnapshot> searchLoadWithoutShutDownNodes = getNodeSearchLoad();
            assertThat(searchLoadWithoutShutDownNodes, hasSize(numSearchNodes - shuttingDownNodes.size()));
            assertTrue(searchLoadWithoutShutDownNodes.stream().allMatch(searchLoad -> searchLoad.metricQuality() == MetricQuality.EXACT));
        });
    }

    private String startMasterNode() {
        return internalCluster().startMasterOnlyNode(
            nodeSettings().put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
    }

    private static List<NodeSearchLoadSnapshot> getNodeSearchLoad() {
        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
        var loadsAfterSearch = searchMetricsService.getSearchTierMetrics().getNodesLoad();
        return loadsAfterSearch;
    }

    private static void markNodesForShutdown(List<DiscoveryNode> shuttingDownNodes, List<SingleNodeShutdownMetadata.Type> shutdownTypes) {
        shuttingDownNodes.forEach(node -> {
            final var type = randomFrom(shutdownTypes);
            assertAcked(
                client().execute(
                    PutShutdownNodeAction.INSTANCE,
                    new PutShutdownNodeAction.Request(
                        TEST_REQUEST_TIMEOUT,
                        TEST_REQUEST_TIMEOUT,
                        node.getId(),
                        type,
                        "Shutdown for test",
                        null,
                        type.equals(SingleNodeShutdownMetadata.Type.REPLACE) ? randomIdentifier() : null,
                        type.equals(SingleNodeShutdownMetadata.Type.SIGTERM) ? TimeValue.timeValueMinutes(randomIntBetween(1, 5)) : null
                    )
                )
            );
        });
    }
}

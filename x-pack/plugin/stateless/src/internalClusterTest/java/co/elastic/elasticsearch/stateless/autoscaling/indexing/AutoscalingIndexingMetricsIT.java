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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import co.elastic.elasticsearch.serverless.autoscaling.ServerlessAutoscalingPlugin;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetIndexTierMetrics;
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.shutdown.DeleteShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AutoscalingIndexingMetricsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(
            List.of(TestTelemetryPlugin.class, ShutdownPlugin.class, ServerlessAutoscalingPlugin.class),
            super.nodePlugins()
        );
    }

    public void testIndexingMetricsArePublishedEventually() throws Exception {
        startMasterOnlyNode();
        // Reduce the time between publications, so we can expect at least one publication per second.
        startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        final AtomicInteger ingestLoadPublishSent = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishNodeIngestLoadMetric.NAME)) {
                    ingestLoadPublishSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        assertBusy(() -> assertThat(ingestLoadPublishSent.get(), equalTo(1)));
        var metrics = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class)
            .getIndexTierMetrics(ClusterState.EMPTY_STATE);
        assertThat(metrics.toString(), metrics.getNodesLoad().size(), equalTo(1));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).load(), equalTo(0.0));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        int bulkRequests = randomIntBetween(10, 20);
        for (int i = 0; i < bulkRequests; i++) {
            indexDocs(indexName, randomIntBetween(100, 1000));
        }
        // Wait for at least one more publish
        assertBusy(() -> assertThat(ingestLoadPublishSent.get(), greaterThan(1)));
        // We'd need an assertBusy since the second publish might still miss the recent load.
        assertBusy(() -> {
            var metricsAfter = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class)
                .getIndexTierMetrics(ClusterState.EMPTY_STATE);
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).load(), greaterThan(0.0));
        });
    }

    public void testMaxTimeToClearQueueDynamicSetting() {
        startMasterAndIndexNode();
        admin().cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Map.of(IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE.getKey(), TimeValue.timeValueSeconds(1)))
            .get();
        var getSettingsResponse = clusterAdmin().execute(ClusterGetSettingsAction.INSTANCE, new ClusterGetSettingsAction.Request())
            .actionGet();
        assertThat(
            getSettingsResponse.settings().get(IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE.getKey()),
            equalTo(TimeValue.timeValueSeconds(1).getStringRep())
        );
    }

    public void testAutoscalingWithQueueSize() throws Exception {
        startMasterOnlyNode();
        // Reduce the time between publications, so we can expect at least one publication per second.
        var indexNodeName = startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .put(IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        final AtomicInteger ingestLoadPublishSent = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishNodeIngestLoadMetric.NAME)) {
                    ingestLoadPublishSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        assertBusy(() -> assertThat(ingestLoadPublishSent.get(), equalTo(1)));
        var metrics = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class)
            .getIndexTierMetrics(ClusterState.EMPTY_STATE);
        assertThat(metrics.toString(), metrics.getNodesLoad().size(), equalTo(1));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).load(), equalTo(0.0));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // some write so that the WRITE EWMA is not zero
        indexDocs(indexName, randomIntBetween(5000, 10000));
        assertBusy(() -> {
            var metricsAfter = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class)
                .getIndexTierMetrics(ClusterState.EMPTY_STATE);
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).load(), allOf(greaterThan(0.0), lessThanOrEqualTo(1.0)));
        });
        // Block the executor workers to pile up writes
        var threadpool = internalCluster().getInstance(ThreadPool.class, indexNodeName);
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.WRITE);
        final var executorThreads = threadpool.info(ThreadPool.Names.WRITE).getMax();
        var barrier = new CyclicBarrier(executorThreads + 1);
        for (int i = 0; i < executorThreads; i++) {
            executor.execute(() -> {
                try {
                    barrier.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        var writeRequests = randomIntBetween(100, 200);
        for (int i = 0; i < writeRequests; i++) {
            client().prepareBulk().add(new IndexRequest(indexName).source("field", i)).execute();
        }
        // Wait for at least one more publish
        assertBusy(() -> assertThat(ingestLoadPublishSent.get(), greaterThan(1)));
        // We'd need an assertBusy since the second publish might still miss the recent load.
        // Eventually just because of queueing, the load will go above the current available threads
        assertBusy(() -> {
            var metricsAfter = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class)
                .getIndexTierMetrics(ClusterState.EMPTY_STATE);
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).load(), greaterThan((double) executorThreads));
        });
        barrier.await(30, TimeUnit.SECONDS);
    }

    public void testOngoingTasksAreReflectedInIngestionLoad() throws Exception {
        startMasterOnlyNode();
        // Reduce the time between publications, so we can expect at least one publication per second.
        var indexNodeName = startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        var ingestMetricsService = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class);
        final var metricPublicationBarrier = new CyclicBarrier(2);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishNodeIngestLoadMetric.NAME)) {
                    longAwait(metricPublicationBarrier);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        // Wait for a publication of the metrics
        longAwait(metricPublicationBarrier);
        var metrics = ingestMetricsService.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        assertThat(metrics.toString(), metrics.getNodesLoad().size(), equalTo(1));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).load(), equalTo(0.0));
        // Block the executor workers to simulate long-running write tasks
        var threadpool = internalCluster().getInstance(ThreadPool.class, indexNodeName);
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.WRITE);
        final var executorThreads = threadpool.info(ThreadPool.Names.WRITE).getMax();
        var barrier = new CyclicBarrier(executorThreads + 1);
        for (int i = 0; i < executorThreads; i++) {
            executor.execute(() -> longAwait(barrier));
        }
        // Wait for another publication of the metrics
        longAwait(metricPublicationBarrier);
        // Eventually just because of the "long-running" tasks, the load will go up
        assertBusy(() -> {
            var metricsAfter = ingestMetricsService.getIndexTierMetrics(ClusterState.EMPTY_STATE);
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(
                metricsAfter.toString(),
                metricsAfter.getNodesLoad().get(0).load(),
                allOf(greaterThan(0.0), lessThanOrEqualTo((double) executorThreads))
            );
        });
        longAwait(barrier);
    }

    public void testAverageWriteLoadSamplerDynamicEwmaAlphaSetting() throws Exception {
        var master = startMasterOnlyNode();
        // Reduce the time between publications, so we can expect at least one publication per second.
        startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .put(AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey(), 0.0)
                .build()
        );
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        var ingestMetricsService = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class);
        final var publicationsProcessed = new Semaphore(0);
        MockTransportService.getInstance(master)
            .addRequestHandlingBehavior(TransportPublishNodeIngestLoadMetric.NAME, (handler, request, channel, task) -> {
                var testChannel = new TestTransportChannel(new ChannelActionListener<>(channel).delegateFailure((l, r) -> {
                    // Increment processed publications only upon response. This makes sure that any read via `ingestMetricsService`
                    // reflects the processed metrics.
                    publicationsProcessed.release();
                    l.onResponse(r);
                }));
                handler.messageReceived(request, testChannel, task);
            });

        // Wait for a new round of publication of the metrics
        publicationsProcessed.drainPermits();
        safeAcquire(publicationsProcessed);
        assertThat(
            ingestMetricsService.getIndexTierMetrics(ClusterState.EMPTY_STATE).getNodesLoad(),
            equalTo(List.of(new NodeIngestLoadSnapshot(0.0, MetricQuality.EXACT)))
        );

        // As initial value of the EWMA is 0 and Alpha is 0, the EWMA should not change as we index documents.
        logger.info("--> Indexing documents with {}=0.0", AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey());
        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }
        // wait for a new round of publication of the metrics
        publicationsProcessed.drainPermits();
        safeAcquire(publicationsProcessed);

        assertThat(
            ingestMetricsService.getIndexTierMetrics(ClusterState.EMPTY_STATE).getNodesLoad(),
            equalTo(List.of(new NodeIngestLoadSnapshot(0.0, MetricQuality.EXACT)))
        );

        // Updating Alpha means the EWMA would reflect task execution time of new tasks.
        logger.info("--> Updating {} to 0.5", AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey());

        assertAcked(
            admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Map.of(AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey(), 0.5))
                .get()
        );

        logger.info("--> Indexing documents with {}=0.5", AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey());
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }
        // Eventually, we'd see the indexing load reflected in the new metrics. This might not immediately happen upon the first
        // publication after the indexing activity, therefore, we'd use assertBusy.
        assertBusy(() -> {
            var loadsAfterIndexing2 = ingestMetricsService.getIndexTierMetrics(ClusterState.EMPTY_STATE).getNodesLoad();
            assertThat(loadsAfterIndexing2.size(), equalTo(1));
            assertThat(loadsAfterIndexing2.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing2.get(0).load(), greaterThan(0.0));
        });
    }

    public void testMetricsAreRepublishedAfterMasterFailover() throws Exception {
        for (int i = 0; i < 2; i++) {
            startMasterNode(Settings.EMPTY);
        }

        startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }

        assertBusy(() -> {
            var loadsAfterIndexing = getNodeIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });

        shutdownMasterNodeGracefully();

        assertBusy(() -> {
            var loadsAfterIndexing = getNodeIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });
    }

    public void testMasterFailoverWithOnGoingMetricPublication() throws Exception {
        for (int i = 0; i < 2; i++) {
            startMasterNode(Settings.EMPTY);
        }
        startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        assertBusy(() -> {
            var loadsBeforeIndexing = getNodeIngestLoad();
            assertThat(loadsBeforeIndexing.size(), equalTo(1));
            assertThat(loadsBeforeIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsBeforeIndexing.get(0).load(), equalTo(0.0));
        });

        var firstNonZeroPublishIndexLoadLatch = new CountDownLatch(1);
        MockTransportService mockTransportService = (MockTransportService) internalCluster().getCurrentMasterNodeInstance(
            TransportService.class
        );
        mockTransportService.addRequestHandlingBehavior(TransportPublishNodeIngestLoadMetric.NAME, (handler, request, channel, task) -> {
            if (request instanceof PublishNodeIngestLoadRequest publishRequest && publishRequest.getIngestionLoad() > 0) {
                firstNonZeroPublishIndexLoadLatch.countDown();
            }
            handler.messageReceived(request, channel, task);
        });

        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }

        safeAwait(firstNonZeroPublishIndexLoadLatch);
        shutdownMasterNodeGracefully();

        assertBusy(() -> {
            List<NodeIngestLoadSnapshot> loadsAfterIndexing = getNodeIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });
    }

    public void testMetricsAreRepublishedAfterMasterNodeHasToRecoverStateFromStore() throws Exception {
        var masterNode = startMasterNode(
            Settings.builder()
                // MAX_MISSED_HEARTBEATS x HEARTBEAT_FREQUENCY is how long it takes for the last master heartbeat to expire. Speed up the
                // time to master takeover/election after full cluster restart.
                // The intention of the test is to reload from the remote blob store, so graceful shutdown (via abdication) will not do so.
                .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
        startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }

        assertBusy(() -> {
            var loadsAfterIndexing = getNodeIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });

        internalCluster().restartNode(masterNode);

        // After the master node is restarted the index load is re-populated from the indexing node
        assertBusy(() -> {
            var loadsAfterIndexing = getNodeIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });
    }

    public void testAutoscalingExecutorIngestionLoadMetrics() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        // Make sure all metrics are there
        plugin.collect();
        for (String executor : AverageWriteLoadSampler.WRITE_EXECUTORS) {
            assertFalse(
                plugin.getDoubleGaugeMeasurement("es.autoscaling.indexing.thread_pool." + executor + ".average_write_load.current")
                    .isEmpty()
            );
            assertFalse(
                plugin.getDoubleGaugeMeasurement("es.autoscaling.indexing.thread_pool." + executor + ".average_task_execution_time.current")
                    .isEmpty()
            );
            assertFalse(
                plugin.getDoubleGaugeMeasurement(
                    "es.autoscaling.indexing.thread_pool." + executor + ".threads_needed_to_handle_queue.current"
                ).isEmpty()
            );
            assertFalse(
                plugin.getLongGaugeMeasurement("es.autoscaling.indexing.thread_pool." + executor + ".queue_size.current").isEmpty()
            );
        }

        assertBusy(() -> {
            // Reset so there is only one measurement
            plugin.resetMeter();
            // Create some load and collect metric values
            indexDocsAndRefresh(indexName, randomIntBetween(100, 1000));
            plugin.collect();
            var measurements = plugin.getDoubleGaugeMeasurement("es.autoscaling.indexing.thread_pool.write.average_write_load.current");
            assertThat(measurements.size(), equalTo(1));
            assertThat(measurements.get(0).value().doubleValue(), greaterThan(0.0));
            measurements = plugin.getDoubleGaugeMeasurement(
                "es.autoscaling.indexing.thread_pool.write.average_task_execution_time.current"
            );
            assertThat(measurements.size(), equalTo(1));
            assertThat(measurements.get(0).value().doubleValue(), greaterThan(0.0));
        });
    }

    public void testIngestLoadsMetricsWithShutdownMetadata() throws Exception {
        final int numNodes = between(1, 6);
        final Settings nodeSettings = Settings.builder()
            .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        IntStream.range(0, numNodes).forEach(i -> startMasterAndIndexNode(nodeSettings));

        final String indexName = randomIdentifier();
        createIndex(indexName, numNodes, 0);
        ensureGreen(indexName);

        for (int i = 0; i < numNodes; i++) {
            indexDocsAndRefresh(indexName, between(10, 50));
        }
        // Ensure meaningful metrics have been published for each node
        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getIngestNodesLoad();
            assertThat(ingestNodesLoad, hasSize(numNodes));
            assertThat(
                ingestNodesLoad.stream()
                    .allMatch(ingestNodeLoad -> ingestNodeLoad.load() > 0.0 && ingestNodeLoad.metricQuality() == MetricQuality.EXACT),
                is(true)
            );
        });

        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final List<DiscoveryNode> shuttingDownNodes = randomSubsetOf(
            Math.min(between(1, 3), numNodes),
            clusterService.state().nodes().getDataNodes().values()
        );
        putShutdownMetadataForNodes(shuttingDownNodes);

        // Ingest load metrics are not impacted by shutdown metadata because the setting is not enabled
        assertThat(getIngestNodesLoad(), hasSize(numNodes));

        // Enable the setting to see ingest load metrics attenuated for the shutdown metadata
        updateClusterSettings(Settings.builder().put(IngestMetricsService.SHUTDOWN_ATTENUATION_ENABLED.getKey(), true));
        final var nodesLoadAttenuated = getIngestNodesLoad();
        final int expectedNumLoadMetrics = numNodes - shuttingDownNodes.size();
        assertThat(nodesLoadAttenuated, hasSize(expectedNumLoadMetrics));
        // Not comparing the exact metric values since new values may have been published.
        // In addition, the more granular comparison is exercised in IngestMetricsServiceTests.

        // Remove shutdown metadata and ingest load metrics will be back to normal
        deleteShutdownMetadataForNodes(shuttingDownNodes);
        assertThat(getIngestNodesLoad(), hasSize(numNodes));
    }

    private static void putShutdownMetadataForNodes(List<DiscoveryNode> shuttingDownNodes) {
        shuttingDownNodes.forEach(
            node -> assertAcked(
                client().execute(
                    PutShutdownNodeAction.INSTANCE,
                    new PutShutdownNodeAction.Request(
                        TEST_REQUEST_TIMEOUT,
                        TEST_REQUEST_TIMEOUT,
                        node.getId(),
                        SingleNodeShutdownMetadata.Type.SIGTERM,
                        "Shutdown for test",
                        null,
                        null,
                        TimeValue.timeValueMinutes(randomIntBetween(1, 5))
                    )
                )
            )
        );
    }

    private static void deleteShutdownMetadataForNodes(List<DiscoveryNode> shuttingDownNodes) {
        shuttingDownNodes.forEach(
            node -> assertAcked(
                client().execute(
                    DeleteShutdownNodeAction.INSTANCE,
                    new DeleteShutdownNodeAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, node.getId())
                )
            )
        );
    }

    private static List<NodeIngestLoadSnapshot> getIngestNodesLoad() {
        return safeGet(
            client().execute(GetIndexTierMetrics.INSTANCE, new GetIndexTierMetrics.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        ).getMetrics().getNodesLoad();
    }

    private String startMasterNode(Settings extraSettings) {
        return internalCluster().startMasterOnlyNode(nodeSettings().put(extraSettings).build());
    }

    private static List<NodeIngestLoadSnapshot> getNodeIngestLoad() {
        var ingestMetricsService = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class);
        var loadsAfterIndexing = ingestMetricsService.getIndexTierMetrics(ClusterState.EMPTY_STATE).getNodesLoad();
        return loadsAfterIndexing;
    }

    public static void longAwait(CyclicBarrier barrier) {
        try {
            barrier.await(30, TimeUnit.SECONDS);
        } catch (BrokenBarrierException | TimeoutException e) {
            throw new AssertionError(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}

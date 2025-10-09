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
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.shutdown.DeleteShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.ACCURATE_LOAD_WINDOW;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.IngestMetricType.ADJUSTED;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.IngestMetricType.SINGLE;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.groupIndexNodesByShutdownStatus;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class AutoscalingIndexingMetricsIT extends AbstractStatelessIntegTestCase {
    public static final double EPSILON = 0.0000001;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(
            List.of(TestTelemetryPlugin.class, ShutdownPlugin.class, ServerlessAutoscalingPlugin.class, CustomIngestTestPlugin.class),
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
        var metrics = getNodesIngestLoad();
        assertThat(metrics.toString(), metrics.size(), equalTo(1));
        assertThat(metrics.toString(), metrics.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.get(0).load(), equalTo(0.0));

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
            var metricsAfter = getNodesIngestLoad();
            assertThat(metricsAfter.toString(), metricsAfter.size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.get(0).load(), greaterThan(0.0));
        });
    }

    public void testMaxTimeToClearQueueDynamicSetting() {
        startMasterAndIndexNode();
        admin().cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Map.of(IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE.getKey(), TimeValue.timeValueSeconds(1)))
            .get();
        var getSettingsResponse = clusterAdmin().execute(
            ClusterGetSettingsAction.INSTANCE,
            new ClusterGetSettingsAction.Request(TEST_REQUEST_TIMEOUT)
        ).actionGet();
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
                // Use a higher alpha than the default so that the observed ingestion load converges faster towards the expected value.
                .put(ThreadPool.WRITE_THREAD_POOLS_EWMA_ALPHA_SETTING.getKey(), 0.1)
                // Make sure initially queue contribution is not ignored
                .put(IngestLoadProbe.INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION.getKey(), TimeValue.ZERO)
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
        var metrics = getNodesIngestLoad();
        assertThat(metrics.toString(), metrics.size(), equalTo(1));
        assertThat(metrics.toString(), metrics.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.get(0).load(), equalTo(0.0));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // some write so that the WRITE EWMA is not zero
        indexDocs(indexName, randomIntBetween(5000, 6000));
        assertBusy(() -> {
            var metricsAfter = getNodesIngestLoad();
            assertThat(metricsAfter.toString(), metricsAfter.size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.get(0).load(), allOf(greaterThan(0.0), lessThanOrEqualTo(1.0)));
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
            var metricsAfter = getNodesIngestLoad();
            assertThat(metricsAfter.toString(), metricsAfter.size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.get(0).load(), greaterThan((double) executorThreads));
        });
        // Update the setting to check that the queue contribution is ignored
        updateClusterSettings(
            Settings.builder().put(IngestLoadProbe.INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION.getKey(), TimeValue.ONE_HOUR)
        );
        assertBusy(() -> {
            var metricsAfter = getNodesIngestLoad();
            assertThat(metricsAfter.toString(), metricsAfter.size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.get(0).load(), lessThanOrEqualTo((double) executorThreads));
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

        assertBusy(() -> {
            var ingestLoads = getNodesIngestLoad();
            assertThat(ingestLoads.toString(), ingestLoads.size(), equalTo(1));
            assertThat(ingestLoads.toString(), ingestLoads.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(ingestLoads.toString(), ingestLoads.get(0).load(), equalTo(0.0));
        });
        // Block the executor workers to simulate long-running write tasks
        var threadpool = internalCluster().getInstance(ThreadPool.class, indexNodeName);
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.WRITE);
        final var executorThreads = threadpool.info(ThreadPool.Names.WRITE).getMax();
        var barrier = new CyclicBarrier(executorThreads + 1);
        for (int i = 0; i < executorThreads; i++) {
            executor.execute(() -> longAwait(barrier));
        }

        // Eventually just because of the "long-running" tasks, the load will go up
        assertBusy(() -> {
            var ingestLoadsAfter = getNodesIngestLoad();
            assertThat(ingestLoadsAfter.toString(), ingestLoadsAfter.size(), equalTo(1));
            assertThat(ingestLoadsAfter.toString(), ingestLoadsAfter.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(
                ingestLoadsAfter.toString(),
                ingestLoadsAfter.get(0).load(),
                allOf(greaterThan(0.0), lessThanOrEqualTo((double) executorThreads))
            );
        });
        longAwait(barrier);
    }

    public void testAverageWriteLoadSamplerDynamicEwmaAlphaSetting() throws Exception {
        var master = startMasterOnlyNode();
        // Reduce the time between publications, so we can expect at least one publication per second.
        final String nodeName = startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .put(AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey(), 0.0)
                .build()
        );
        final String nodeId = getNodeId(nodeName);
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
        assertThat(getNodesIngestLoad(), equalTo(List.of(new NodeIngestLoadSnapshot(nodeId, nodeName, 0.0, MetricQuality.EXACT))));

        // As initial value of the EWMA is 0 and Alpha is 0, the EWMA should not change as we index documents.
        logger.info("--> Indexing documents with {}=0.0", AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey());
        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }
        // wait for a new round of publication of the metrics
        publicationsProcessed.drainPermits();
        safeAcquire(publicationsProcessed);

        assertThat(getNodesIngestLoad(), equalTo(List.of(new NodeIngestLoadSnapshot(nodeId, nodeName, 0.0, MetricQuality.EXACT))));

        // Updating Alpha means the EWMA would reflect task execution time of new tasks.
        logger.info("--> Updating {} to 0.5", AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey());

        assertAcked(
            admin().cluster()
                .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
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
            var loadsAfterIndexing2 = getNodesIngestLoad();
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
            var loadsAfterIndexing = getNodesIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });

        shutdownMasterNodeGracefully();

        assertBusy(() -> {
            var loadsAfterIndexing = getNodesIngestLoad();
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
            var loadsBeforeIndexing = getNodesIngestLoad();
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
            List<NodeIngestLoadSnapshot> loadsAfterIndexing = getNodesIngestLoad();
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
            var loadsAfterIndexing = getNodesIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });

        internalCluster().restartNode(masterNode);

        // After the master node is restarted the index load is re-populated from the indexing node
        assertBusy(() -> {
            var loadsAfterIndexing = getNodesIngestLoad();
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

        final TestTelemetryPlugin plugin = findPlugin(indexNode, TestTelemetryPlugin.class);

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
                plugin.getDoubleGaugeMeasurement("es.autoscaling.indexing.thread_pool." + executor + ".average_queue_size.current")
                    .isEmpty()
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
        final TimeValue adjustmentAfterScalingWindow = TimeValue.timeValueSeconds(between(0, 2));
        final Settings nodeSettings = Settings.builder()
            .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(IngestMetricsService.LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW.getKey(), adjustmentAfterScalingWindow)
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
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getNodesIngestLoad();
            assertThat(ingestNodesLoad, hasSize(numNodes));
            assertThat(
                ingestNodesLoad.stream()
                    .allMatch(ingestNodeLoad -> ingestNodeLoad.load() > 0.0 && ingestNodeLoad.metricQuality() == MetricQuality.EXACT),
                is(true)
            );
        });

        TestTelemetryPlugin plugin = findPlugin(internalCluster().getMasterName(), TestTelemetryPlugin.class);
        plugin.collect();
        List<Measurement> measurements = plugin.getDoubleGaugeMeasurement(NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME);
        assertThat(measurements.size(), equalTo(numNodes));
        measurements.forEach(measurement -> assertThat(measurement.attributes().get("type"), is(SINGLE.key())));
        measurements.forEach(measurement -> assertThat(measurement.attributes().get("quality"), is(MetricQuality.EXACT.getLabel())));

        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final List<DiscoveryNode> shuttingDownNodes = randomSubsetOf(
            Math.min(between(1, 3), numNodes),
            clusterService.state().nodes().getDataNodes().values()
        );
        logger.info("--> creating shutdown records for [{}]", shuttingDownNodes.stream().map(DiscoveryNode::getName).toList());
        markNodesForShutdown(shuttingDownNodes, List.of(SingleNodeShutdownMetadata.Type.SIGTERM));

        // Ingest load metrics are not impacted by shutdown metadata because the setting is not enabled
        assertBusy(() -> assertThat(getNodesIngestLoad(), hasSize(numNodes)));

        // Enable the setting to see ingest load metrics ignored for the nodes with shutdown metadata
        updateClusterSettings(Settings.builder().put(IngestMetricsService.HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), 0.0));
        updateClusterSettings(Settings.builder().put(IngestMetricsService.LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), 1.0));

        // Not comparing the exact metric values since new values may have been published.
        // In addition, the more granular comparison is exercised in IngestMetricsServiceTests.
        assertBusy(() -> {
            final var ingestionLoadMetrics = getNodesIngestLoad();
            assertThat(ingestionLoadMetrics, hasSize(numNodes));
            assertTrue(ingestionLoadMetrics.stream().allMatch(l -> l.metricQuality().equals(MetricQuality.MINIMUM)));
            // Expect at least shuttingDownNodes.size() ingestion loads with value 0.0
            assertThat(
                ingestionLoadMetrics.stream().filter(l -> l.load() < EPSILON).count(),
                greaterThanOrEqualTo((long) shuttingDownNodes.size())
            );
        });

        // Master may have changed due to shutdown marker, we need check the metrics on the current master
        plugin = findPlugin(internalCluster().getMasterName(), TestTelemetryPlugin.class);
        plugin.resetMeter();
        plugin.collect();
        measurements = plugin.getDoubleGaugeMeasurement(NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME);
        assertThat(measurements.size(), equalTo(numNodes * 2));
        final Map<Object, List<Measurement>> groupedMeasurements = measurements.stream()
            .collect(Collectors.groupingBy(m -> ADJUSTED.key().equals(m.attributes().get("type"))));
        assertThat(groupedMeasurements.get(false), hasSize(numNodes));
        groupedMeasurements.get(false).forEach(m -> assertThat(m.attributes().get("quality"), is(MetricQuality.EXACT.getLabel())));
        assertThat(groupedMeasurements.get(true), hasSize(numNodes));
        groupedMeasurements.get(true).forEach(m -> assertThat(m.attributes().get("quality"), is(MetricQuality.MINIMUM.getLabel())));

        // Add replacement nodes to the cluster and ensure master is not shutting down
        final List<String> newNodeNames = IntStream.range(0, shuttingDownNodes.size())
            .mapToObj(i -> startMasterAndIndexNode(nodeSettings))
            .toList();
        ensureStableCluster(numNodes + shuttingDownNodes.size());
        assertBusy(
            () -> assertFalse(shuttingDownNodes.stream().map(DiscoveryNode::getName).toList().contains(internalCluster().getMasterName()))
        );

        // Stop the shutting down nodes and ingest load metrics will be back to normal after adjustmentAfterScaling window
        for (DiscoveryNode node : shuttingDownNodes) {
            internalCluster().stopNode(node.getName());
        }
        ensureStableCluster(numNodes);
        // There should be no shutting down nodes
        assertThat(
            groupIndexNodesByShutdownStatus(internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state()).get(true),
            equalTo(0)
        );
        // Whether the shutdown markers are removed should have no impact
        if (randomBoolean()) {
            deleteShutdownMetadataForNodes(shuttingDownNodes);
        }

        final var ingestionLoadMetrics = getNodesIngestLoad();
        assertThat(ingestionLoadMetrics, hasSize(numNodes));
        final long afterScalingBaseTimeInNanos = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class)
            .getLoadAdjustmentAfterScalingBaseTimeInNanos();

        // Check for after-scaling adjustment if it is enabled and the metrics are retrieved within the adjustmentAfterScaling window
        if (adjustmentAfterScalingWindow.equals(TimeValue.ZERO) == false
            && System.nanoTime() - afterScalingBaseTimeInNanos < adjustmentAfterScalingWindow.getNanos()) {
            assertTrue(ingestionLoadMetrics.stream().allMatch(l -> l.metricQuality().equals(MetricQuality.MINIMUM)));
            // Expect at least 1 ingestion load with value 0.0
            assertThat(ingestionLoadMetrics.stream().filter(l -> l.load() < EPSILON).count(), greaterThanOrEqualTo(1L));
            // Metrics should be eventually back to exact once we are outside the window of after-scaling adjustment
            assertBusy(() -> {
                final var ingestionLoadMetrics2 = getNodesIngestLoad();
                assertThat(ingestionLoadMetrics2, hasSize(numNodes));
                assertTrue(ingestionLoadMetrics2.stream().allMatch(l -> l.metricQuality().equals(MetricQuality.EXACT)));
            });
        } else {
            // If after-scaling is not enable, the metric quality should be back to Exact immediately for old nodes.
            // It can be Minimum or Missing for new nodes depending on how quickly they join the cluster and publish update.
            assertTrue(
                "ingestLoads: " + ingestionLoadMetrics,
                ingestionLoadMetrics.stream()
                    .allMatch(l -> l.metricQuality().equals(MetricQuality.EXACT) || newNodeNames.contains(l.nodeName()))
            );
        }
    }

    public void testOnlyMasterNodePublishesIngestLoadMetrics() throws Exception {
        final Settings nodeSettings = Settings.builder()
            .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        final int numNodes = between(2, 4);
        IntStream.range(0, numNodes).forEach(i -> startMasterAndIndexNode(nodeSettings));
        ensureStableCluster(numNodes);
        final String indexName = randomIdentifier();
        createIndex(indexName, numNodes, 0);
        ensureGreen(indexName);

        for (int i = 0; i < numNodes; i++) {
            indexDocsAndRefresh(indexName, between(10, 50));
        }

        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getNodesIngestLoad();
            assertThat(ingestNodesLoad, hasSize(numNodes));
        });
        assertOnlyMasterNodePublishesIngestLoadMetrics();

        for (int i = 0; i < numNodes; i++) {
            final DiscoveryNode currentMasterNode = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).localNode();
            markNodesForShutdown(List.of(currentMasterNode), List.of(SingleNodeShutdownMetadata.Type.SIGTERM));
            assertBusy(() -> assertThat(internalCluster().getMasterName(), is(not(currentMasterNode.getName()))));
            getNodesIngestLoad(); // trigger metrics recording
            assertOnlyMasterNodePublishesIngestLoadMetrics();
            deleteShutdownMetadataForNodes(List.of(currentMasterNode));
        }
    }

    private void assertOnlyMasterNodePublishesIngestLoadMetrics() {
        final String masterName = internalCluster().getMasterName();
        for (String nodeName : internalCluster().getNodeNames()) {
            final TestTelemetryPlugin plugin = findPlugin(nodeName, TestTelemetryPlugin.class);
            // reset to clear previous records, a new collect call should see metrics only on the current master
            plugin.resetMeter();
            plugin.collect();
            final List<Measurement> measurements = plugin.getDoubleGaugeMeasurement(NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME);
            if (nodeName.equals(masterName)) {
                assertThat(nodeName, measurements, not(empty()));
            } else {
                assertThat(nodeName, measurements, empty());
            }
        }
    }

    public void testLogWarnForIngestionLoadsOlderThanAccurateWindow() throws Exception {
        var masterNode = startMasterOnlyNode(Settings.builder().put(ACCURATE_LOAD_WINDOW.getKey(), TimeValue.timeValueSeconds(2)).build());
        startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        // It is possible for the index node's first ingestion load publication to get processed by the master
        // when the node is not yet known to the master, and therefore its received ingestion load is marked with
        // Minimum quality initially.
        assertBusy(() -> {
            var ingestionLoads = getNodesIngestLoad();
            assertThat(ingestionLoads.size(), equalTo(1));
            assertThat(ingestionLoads.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        });

        MockTransportService.getInstance(masterNode)
            .addRequestHandlingBehavior(TransportPublishNodeIngestLoadMetric.NAME, (handler, request, channel, task) -> {
                // respond so that new publications happen
                logger.info("--> Dropping publication");
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            });

        try (var mockLog = MockLog.capture(IngestMetricsService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "outdated ingest load",
                    IngestMetricsService.class.getCanonicalName(),
                    Level.WARN,
                    "reported node ingest load is older than *"
                )
            );
            // dropping the publications leads to marking the ingest load as minimum after accurate_load_window seconds
            // so we use that to wait for the warning message
            assertBusy(() -> {
                var ingestionLoads2 = getNodesIngestLoad();
                assertThat(ingestionLoads2.size(), equalTo(1));
                assertThat(ingestionLoads2.get(0).metricQuality(), equalTo(MetricQuality.MINIMUM));
            });
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testRemoveIngestionLoadWhenNodeRemoved() throws Exception {
        final int numNodes = between(2, 5);
        final Settings nodeSettings = Settings.builder()
            .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(IngestMetricsService.LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        IntStream.range(0, numNodes).forEach(i -> startMasterAndIndexNode(nodeSettings));

        final String indexName = randomIdentifier();
        createIndex(indexName, numNodes, 0);
        ensureGreen(indexName);

        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getNodesIngestLoad();
            assertThat(ingestNodesLoad, hasSize(numNodes));
            assertTrue(ingestNodesLoad.stream().allMatch(ingestNodeLoad -> ingestNodeLoad.metricQuality() == MetricQuality.EXACT));
        });

        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final List<DiscoveryNode> shuttingDownNodes = randomSubsetOf(
            between(1, numNodes - 1),
            clusterService.state().nodes().getDataNodes().values()
        );
        logger.info("--> Marking nodes {} for removal", shuttingDownNodes);
        markNodesForShutdown(
            shuttingDownNodes,
            Arrays.stream(SingleNodeShutdownMetadata.Type.values()).filter(SingleNodeShutdownMetadata.Type::isRemovalType).toList()
        );

        // Needs assertBusy since we might have marked the current master for shutdown which results in it abdicating to a new
        // master-eligible node which might return only a subset of the nodes (temporarily). Even after a new master is elected
        // AND all nodes have rejoined the cluster, there may be a delay in the data nodes publishing fresh ingestion load metrics.
        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getNodesIngestLoad();
            assertThat(ingestNodesLoad.toString(), ingestNodesLoad, hasSize(numNodes));
            assertTrue(
                ingestNodesLoad.toString(),
                ingestNodesLoad.stream().allMatch(ingestNodeLoad -> ingestNodeLoad.metricQuality() == MetricQuality.MINIMUM)
            );
        });

        final var newMasterName = waitForAnyShuttingDownMasterNodesToAbdicateAndElectANewMaster(true);
        for (var node : shuttingDownNodes) {
            assertThat(node.getName(), not(equalTo(newMasterName)));
            logger.info("--> stopping node {}", node.getName());
            internalCluster().stopNode(node.getName());
        }
        ensureStableCluster(numNodes - shuttingDownNodes.size());

        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad2 = getNodesIngestLoad();
            assertThat(ingestNodesLoad2, hasSize(numNodes - shuttingDownNodes.size()));
            assertTrue(ingestNodesLoad2.stream().allMatch(ingestNodeLoad -> ingestNodeLoad.metricQuality() == MetricQuality.EXACT));
        });
    }

    public void testAbruptShutdownNodeIngestionLoadIsNotRemoved() throws Exception {
        final Settings nodeSettings = Settings.builder()
            .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
            .put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
            .put(IngestMetricsService.LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        var masterNode = startMasterOnlyNode(nodeSettings);
        var indexNode = startIndexNode(nodeSettings);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode).build());
        ensureGreen(indexName);
        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getNodesIngestLoad();
            assertThat(ingestNodesLoad, hasSize(1));
            assertTrue(ingestNodesLoad.stream().allMatch(ingestNodeLoad -> ingestNodeLoad.metricQuality() == MetricQuality.EXACT));
        });
        if (randomBoolean()) {
            markNodesForShutdown(
                clusterService().state().nodes().getAllNodes().stream().filter(n -> n.getName().equals(indexNode)).toList(),
                Arrays.stream(SingleNodeShutdownMetadata.Type.values()).filter(SingleNodeShutdownMetadata.Type::isRemovalType).toList()
            );
        }
        assertTrue(internalCluster().stopNode(indexNode));
        ensureRed(indexName);
        // The shard is unassigned so the ingestion load from the stopped indexing node should keep being reported
        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getNodesIngestLoad();
            assertThat(ingestNodesLoad, hasSize(1));
            assertTrue(ingestNodesLoad.stream().allMatch(ingestNodeLoad -> ingestNodeLoad.metricQuality() == MetricQuality.MINIMUM));
        });
        startIndexNode(nodeSettings);
        // The shard is unassigned so the ingestion load from the stopped indexing node should keep being reported
        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getNodesIngestLoad();
            assertThat(ingestNodesLoad, hasSize(2));
            assertTrue(ingestNodesLoad.stream().anyMatch(ingestNodeLoad -> ingestNodeLoad.metricQuality() == MetricQuality.EXACT));
            assertTrue(ingestNodesLoad.stream().anyMatch(ingestNodeLoad -> ingestNodeLoad.metricQuality() == MetricQuality.MINIMUM));
        });
        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.require._name"), indexName);
        ensureGreen(indexName);
        // The MINIMUM ingestion load is removed once there are no unassigned shards
        assertBusy(() -> {
            final List<NodeIngestLoadSnapshot> ingestNodesLoad = getNodesIngestLoad();
            assertThat(ingestNodesLoad, hasSize(1));
            assertTrue(ingestNodesLoad.stream().anyMatch(ingestNodeLoad -> ingestNodeLoad.metricQuality() == MetricQuality.EXACT));
        });
    }

    public void testMissingIngestionLoadMetricsAreNotRemovedImmediately() {
        var masterNode = startMasterOnlyNode();
        // drop all updates
        MockTransportService.getInstance(masterNode)
            .addRequestHandlingBehavior(TransportPublishNodeIngestLoadMetric.NAME, (handler, request, channel, task) -> {
                // respond so that new publications happen
                logger.info("--> Dropping publication");
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            });
        var setting = Settings.builder()
            .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        var numberOfIndexNodes = between(1, 5);
        IntStream.range(0, numberOfIndexNodes).forEach(i -> startIndexNode(setting));
        ensureStableCluster(numberOfIndexNodes + 1);
        final String indexName = randomIdentifier();
        createIndex(indexName, numberOfIndexNodes, 0);
        ensureGreen(indexName);
        var ingestionLoads = getNodesIngestLoad();
        assertThat(ingestionLoads.size(), equalTo(numberOfIndexNodes));
        assertTrue(ingestionLoads.toString(), ingestionLoads.stream().allMatch(load -> load.metricQuality().equals(MetricQuality.MISSING)));
    }

    public void testAverageQueueSizeDynamicEwmaAlphaSetting() throws Exception {
        var indexNodeName = startMasterAndIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                // Use 1.0 to ensure the ingestion load converges quickly to the write thread pool size
                .put(AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey(), 1.0)
                // Initially avoid taking into account the queue size by using 0.0
                .put(AverageWriteLoadSampler.QUEUE_SIZE_SAMPLER_EWMA_ALPHA_SETTING.getKey(), 0.0)
                // Make sure queue size is always considered
                .put(IngestLoadProbe.INITIAL_INTERVAL_TO_IGNORE_QUEUE_CONTRIBUTION.getKey(), TimeValue.ZERO)
                .build()
        );
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        assertBusy(() -> {
            var ingestLoads = getNodesIngestLoad();
            assertThat(ingestLoads.toString(), ingestLoads.size(), equalTo(1));
            assertThat(ingestLoads.toString(), ingestLoads.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(ingestLoads.toString(), ingestLoads.get(0).load(), equalTo(0.0));
        });
        // Finish some bulk requests to ensure the average task execution time of the executor is not zero since that leads to a queue
        // contribution of zero.
        indexDocs(indexName, between(1, 10));
        // Block the executor workers to simulate long-running write tasks
        var threadpool = internalCluster().getInstance(ThreadPool.class, indexNodeName);
        var executor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) threadpool.executor(ThreadPool.Names.WRITE);
        final var executorThreads = threadpool.info(ThreadPool.Names.WRITE).getMax();
        var barrier = new CyclicBarrier(executorThreads + 1);
        for (int i = 0; i < executorThreads; i++) {
            executor.execute(() -> longAwait(barrier));
        }
        var writeRequests = randomIntBetween(100, 200);
        for (int i = 0; i < writeRequests; i++) {
            client().prepareBulk().add(new IndexRequest(indexName).source("field", i)).execute();
        }
        assertBusy(() -> {
            assertThat(executor.getCurrentQueueSize(), equalTo(writeRequests));
            var ingestLoadsAfter = getNodesIngestLoad();
            assertThat(ingestLoadsAfter.toString(), ingestLoadsAfter.size(), equalTo(1));
            assertThat(ingestLoadsAfter.toString(), ingestLoadsAfter.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertEquals(ingestLoadsAfter.toString(), ingestLoadsAfter.get(0).load(), executorThreads, EPSILON);
        });
        // Any increase in the ingestion load beyond the size of the thread pool requires take into account the queued tasks
        updateClusterSettings(
            Settings.builder()
                .put(AverageWriteLoadSampler.QUEUE_SIZE_SAMPLER_EWMA_ALPHA_SETTING.getKey(), randomDoubleBetween(0.01, 1.0, true))
        );
        assertBusy(() -> {
            var ingestLoadsAfter = getNodesIngestLoad();
            assertThat(ingestLoadsAfter.toString(), ingestLoadsAfter.size(), equalTo(1));
            assertThat(ingestLoadsAfter.toString(), ingestLoadsAfter.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(ingestLoadsAfter.toString(), ingestLoadsAfter.get(0).load(), greaterThan((double) executorThreads));
        });
        longAwait(barrier);
    }

    public void testIndexingMetricsAreInexactWithShardsInUndesiredLocations() {
        final var masterName = startMasterOnlyNode(
            Settings.builder()
                // block scale-down on any unassigned shards
                .put(IngestMetricsService.MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN.getKey(), 0.0)
                // only move one shard at once, so the other shard remains in state STARTED and counts as undesired
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
                .build()
        );
        // Reduce the time between publications, so we can expect at least one publication per second.
        final var frequentPublicationSettings = Settings.builder()
            .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        final var dataNode0 = startIndexNode(frequentPublicationSettings);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(2, 0).put(
                IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                dataNode0
            ).build()
        );
        ensureGreen(indexName);
        int bulkRequests = randomIntBetween(10, 20);
        for (int i = 0; i < bulkRequests; i++) {
            indexDocs(indexName, randomIntBetween(100, 1000));
        }

        final var ingestLoadReceivedLatch = new CountDownLatch(1);
        final var masterTransportService = MockTransportService.getInstance(masterName);
        masterTransportService.addRequestHandlingBehavior(
            TransportPublishNodeIngestLoadMetric.NAME,
            (handler, request, channel, task) -> handler.messageReceived(
                request,
                request instanceof PublishNodeIngestLoadRequest publishNodeIngestLoadRequest
                    && publishNodeIngestLoadRequest.getIngestionLoad() > 0.0 ? new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            ingestLoadReceivedLatch.countDown();
                            channel.sendResponse(response);
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            ingestLoadReceivedLatch.countDown();
                            channel.sendResponse(exception);
                        }

                        @Override
                        public TransportVersion getVersion() {
                            return channel.getVersion();
                        }
                    } : channel,
                task
            )
        );

        safeAwait(ingestLoadReceivedLatch);
        masterTransportService.clearAllRules();

        var metrics = getNodesIngestLoad();
        assertThat(metrics.toString(), metrics.size(), equalTo(1));
        assertThat(metrics.toString(), metrics.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.get(0).load(), greaterThan(0.0));

        final var dataNode0TransportService = MockTransportService.getInstance(dataNode0);
        final var unblockStartRecoveriesListener = new SubscribableListener<>();
        final var recoveriesStartedLatch = new CountDownLatch(1);
        dataNode0TransportService.addRequestHandlingBehavior(
            TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME,
            (handler, request, channel, task) -> {
                recoveriesStartedLatch.countDown();
                unblockStartRecoveriesListener.addListener(
                    ActionTestUtils.assertNoFailureListener(ignored -> handler.messageReceived(request, channel, task))
                );
            }
        );

        final var dataNode1 = startIndexNode(frequentPublicationSettings);
        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNode1),
            indexName
        );
        safeAwait(recoveriesStartedLatch);

        try {
            var metricsWithUndesiredShards = getNodesIngestLoad();
            assertThat(metricsWithUndesiredShards.toString(), metricsWithUndesiredShards.size(), greaterThanOrEqualTo(1));
            for (final var nodeIngestLoadSnapshot : metricsWithUndesiredShards) {
                assertThat(metricsWithUndesiredShards.toString(), nodeIngestLoadSnapshot.metricQuality(), not(MetricQuality.EXACT));
            }
        } finally {
            unblockStartRecoveriesListener.onResponse(null);
        }
    }

    public void testIngestionLoadFromWriteCoordinationExecutor() throws Exception {
        final var masterNode = startMasterOnlyNode();
        startIndexNode(
            settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.INGEST_ROLE).put(
                IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(),
                TimeValue.timeValueSeconds(1)
            ).build()
        );
        startSearchNode();
        ensureStableCluster(3);
        final var dropIndex = "drop-index";
        createIndex(dropIndex, indexSettings(1, 1).build());
        ensureGreen();

        final var dropPipeline = "drop-pipeline";
        putJsonPipeline(dropPipeline, """
            {
              "processors": [
                {
                  "drop": {}
                }
              ]
            }""");

        assertThat(
            safeGet(
                client().prepareIndex(dropIndex)
                    .setPipeline(dropPipeline)
                    .setSource("foo", "bar")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute()
            ).getResult(),
            equalTo(DocWriteResponse.Result.NOOP)
        );
        assertThat(
            safeGet(client().admin().indices().prepareStats(dropIndex).setDocs(true).execute()).getTotal().getDocs().getCount(),
            equalTo(0L)
        );

        final AtomicReference<Consumer<PublishNodeIngestLoadRequest>> afterSendResponseRef = new AtomicReference<>();
        MockTransportService.getInstance(masterNode)
            .addRequestHandlingBehavior(TransportPublishNodeIngestLoadMetric.NAME, (handler, request, channel, task) -> {
                final var afterSendResponse = afterSendResponseRef.get();
                handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        channel.sendResponse(response);
                        if (afterSendResponse != null) {
                            afterSendResponse.accept(asInstanceOf(PublishNodeIngestLoadRequest.class, request));
                        }
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        fail("unexpected failure: " + exception);
                    }
                }, task);
            });

        // Ingestion loads include stats from write coordination executor by default
        {
            final var publicationLatch = new CountDownLatch(1);
            afterSendResponseRef.set(request -> {
                if (request.getIngestionLoad() > 0.0) {
                    publicationLatch.countDown();
                }
            });
            safeAwait(publicationLatch);
            final var ingestionLoad = getNodesIngestLoad();
            assertThat(ingestionLoad.size(), equalTo(1));
            assertThat(ingestionLoad.getFirst().load(), greaterThan(0.0));
        }

        // Exclude load from write coordination executor and the reported load should be zero since write executor did no work
        {
            updateClusterSettings(Settings.builder().put(IngestLoadProbe.INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED.getKey(), false));
            final var publicationLatch = new CountDownLatch(1);
            afterSendResponseRef.set(request -> {
                if (request.getIngestionLoad() == 0.0) {
                    publicationLatch.countDown();
                }
            });
            safeAwait(publicationLatch);
            final var ingestionLoad = getNodesIngestLoad();
            assertThat(ingestionLoad.size(), equalTo(1));
            assertThat(ingestionLoad.getFirst().load(), equalTo(0.0));
        }
    }

    public static class CustomIngestTestPlugin extends IngestTestPlugin {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> processors = new HashMap<>();
            processors.put(
                "drop",
                (processorFactories, tag, description, config, projectId) -> new TestProcessor(tag, "drop", description, ingestDocument -> {
                    final TimeValue expectedElapsed = TimeValue.timeValueMillis(50);
                    final var start = System.nanoTime();
                    do {
                        // Sleep to ensure the task takes long enough register thread pool load
                        // Also checks system nanos actually advanced since certain CI machine can have infrequent update
                        safeSleep(expectedElapsed);
                    } while (System.nanoTime() - start < expectedElapsed.nanos());
                    return "drop-index".equals(ingestDocument.getCtxMap().get("_index")) ? null : ingestDocument;
                })
            );
            return processors;
        }
    }

    public static void markNodesForShutdown(List<DiscoveryNode> shuttingDownNodes, List<SingleNodeShutdownMetadata.Type> shutdownTypes) {
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

    private static List<NodeIngestLoadSnapshot> getNodesIngestLoad() {
        return safeGet(
            client().execute(GetIndexTierMetrics.INSTANCE, new GetIndexTierMetrics.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        ).getMetrics().getNodesLoad();
    }

    private String startMasterNode(Settings extraSettings) {
        return internalCluster().startMasterOnlyNode(nodeSettings().put(extraSettings).build());
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

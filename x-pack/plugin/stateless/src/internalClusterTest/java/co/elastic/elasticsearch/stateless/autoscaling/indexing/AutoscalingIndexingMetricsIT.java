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

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AutoscalingIndexingMetricsIT extends AbstractStatelessIntegTestCase {
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
        var metrics = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class).getIndexTierMetrics();
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
            var metricsAfter = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class).getIndexTierMetrics();
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
        var metrics = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class).getIndexTierMetrics();
        assertThat(metrics.toString(), metrics.getNodesLoad().size(), equalTo(1));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.getNodesLoad().get(0).load(), equalTo(0.0));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // some write so that the WRITE EWMA is not zero
        indexDocs(indexName, randomIntBetween(5000, 10000));
        assertBusy(() -> {
            var metricsAfter = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class).getIndexTierMetrics();
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
            var metricsAfter = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class).getIndexTierMetrics();
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
        var metrics = ingestMetricsService.getIndexTierMetrics();
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
            var metricsAfter = ingestMetricsService.getIndexTierMetrics();
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

    @TestIssueLogging(
        issueUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1532",
        value = "co.elastic.elasticsearch.stateless.autoscaling.indexing:TRACE"
    )
    public void testAverageWriteLoadSamplerDynamicEwmaAlphaSetting() throws Exception {
        startMasterOnlyNode();
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
        final var barrier = new CyclicBarrier(2);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishNodeIngestLoadMetric.NAME)) {
                    longAwait(barrier);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        // Wait for a publication of the metrics
        longAwait(barrier);
        var loadsBeforeIndexing = ingestMetricsService.getIndexTierMetrics().getNodesLoad();
        assertThat(loadsBeforeIndexing.size(), equalTo(1));
        assertThat(loadsBeforeIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(loadsBeforeIndexing.get(0).load(), equalTo(0.0));

        // As initial value of the EWMA is 0 and Alpha is 0, the EWMA should not change as we index documents.
        logger.info("--> Indexing documents with {}=0.0", AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey());
        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
            longAwait(barrier);
        }
        assertBusy(() -> {
            var loadsAfterIndexing = ingestMetricsService.getIndexTierMetrics().getNodesLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), equalTo(0.0));
        }, 30, TimeUnit.SECONDS);

        // Updating Alpha means the EWMA would reflect task execution time of new tasks.
        logger.info("--> Updating {} to 1.0", AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey());

        assertAcked(
            admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Map.of(AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey(), 1.0))
                .get()
        );

        logger.info("--> Indexing documents with {}=1.0", AverageWriteLoadSampler.WRITE_LOAD_SAMPLER_EWMA_ALPHA_SETTING.getKey());
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
            longAwait(barrier);
        }
        assertBusy(() -> {
            var loadsAfterIndexing = ingestMetricsService.getIndexTierMetrics().getNodesLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        }, 30, TimeUnit.SECONDS);
    }

    public void testMetricsAreRepublishedAfterMasterFailover() throws Exception {
        for (int i = 0; i < 2; i++) {
            startMasterNode();
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

        internalCluster().stopCurrentMasterNode();

        assertBusy(() -> {
            var loadsAfterIndexing = getNodeIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });
    }

    public void testMasterFailoverWithOnGoingMetricPublication() throws Exception {
        for (int i = 0; i < 2; i++) {
            startMasterNode();
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
        });

        int bulks = randomIntBetween(3, 5);
        for (int i = 0; i < bulks; i++) {
            indexDocs(indexName, randomIntBetween(10, 100));
        }

        safeAwait(firstNonZeroPublishIndexLoadLatch);
        internalCluster().stopCurrentMasterNode();

        assertBusy(() -> {
            List<NodeIngestLoadSnapshot> loadsAfterIndexing = getNodeIngestLoad();
            assertThat(loadsAfterIndexing.size(), equalTo(1));
            assertThat(loadsAfterIndexing.get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(loadsAfterIndexing.get(0).load(), greaterThan(0.0));
        });
    }

    public void testMetricsAreRepublishedAfterMasterNodeHasToRecoverStateFromStore() throws Exception {
        var masterNode = startMasterNode();
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

    private String startMasterNode() {
        return internalCluster().startMasterOnlyNode(
            nodeSettings().put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
    }

    private static List<NodeIngestLoadSnapshot> getNodeIngestLoad() {
        var ingestMetricsService = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class);
        var loadsAfterIndexing = ingestMetricsService.getIndexTierMetrics().getNodesLoad();
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

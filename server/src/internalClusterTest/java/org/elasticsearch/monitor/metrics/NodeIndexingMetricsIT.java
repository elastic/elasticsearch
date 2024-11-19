/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.elasticsearch.index.IndexingPressure.MAX_COORDINATING_BYTES;
import static org.elasticsearch.index.IndexingPressure.MAX_PRIMARY_BYTES;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeIndexingMetricsIT extends ESIntegTestCase {

    public static class TestAPMInternalSettings extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting("telemetry.agent.metrics_interval", TimeValue.timeValueSeconds(0), Setting.Property.NodeScope)
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class, TestAPMInternalSettings.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("telemetry.agent.metrics_interval", TimeValue.timeValueSeconds(0)) // disable metrics cache refresh delay
            .build();
    }

    public void testNodeIndexingMetricsArePublishing() {

        final String dataNode = internalCluster().startNode();
        ensureStableCluster(1);

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, dataNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        assertAcked(prepareCreate("test").get());

        // index some documents
        final int docsCount = randomIntBetween(500, 1000);
        for (int i = 0; i < docsCount; i++) {
            var indexResponse = client(dataNode).index(new IndexRequest("test").id("doc_" + i).source(Map.of("key", i, "val", i)))
                .actionGet();
            // check that all documents were created successfully since metric counters below assume that
            assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
        }

        // delete documents
        final int deletesCount = randomIntBetween(1, 50);
        for (int i = 0; i < deletesCount; i++) {
            client(dataNode).delete(new DeleteRequest().index("test").id("doc_" + i)).actionGet();
        }

        // simulate async apm `polling` call for metrics
        plugin.collect();

        var indexingTotal = getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.docs.total");
        assertThat(indexingTotal.getLong(), equalTo((long) docsCount));

        var indexingCurrent = getSingleRecordedMetric(plugin::getLongGaugeMeasurement, "es.indexing.docs.current.total");
        assertThat(indexingCurrent.getLong(), equalTo(0L));

        var indexingFailedTotal = getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.indexing.failed.total");
        assertThat(indexingFailedTotal.getLong(), equalTo(0L));

        var deletionTotal = getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.deletion.docs.total");
        assertThat(deletionTotal.getLong(), equalTo((long) deletesCount));

        var deletionCurrent = getSingleRecordedMetric(plugin::getLongGaugeMeasurement, "es.indexing.deletion.docs.current.total");
        assertThat(deletionCurrent.getLong(), equalTo(0L));

        var indexingTime = getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.time");
        assertThat(indexingTime.getLong(), greaterThan(0L));

        var deletionTime = getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.deletion.time");
        assertThat(deletionTime.getLong(), greaterThanOrEqualTo(0L));

        var throttleTime = getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indices.throttle.time");
        assertThat(throttleTime.getLong(), equalTo(0L));

        var noopTotal = getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indices.noop.total");
        assertThat(noopTotal.getLong(), equalTo(0L));

        var coordinatingOperationsSize = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.coordinating_operations.size"
        );
        assertThat(coordinatingOperationsSize.getLong(), greaterThan(0L));

        var coordinatingOperationsTotal = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.coordinating_operations.total"
        );
        // Note: `delete` request goes thru `TransportBulkAction` invoking coordinating/primary limit checks
        assertThat(coordinatingOperationsTotal.getLong(), equalTo((long) docsCount + deletesCount));

        var coordinatingOperationsCurrentSize = getSingleRecordedMetric(
            plugin::getLongGaugeMeasurement,
            "es.indexing.coordinating_operations.current.size"
        );
        assertThat(coordinatingOperationsCurrentSize.getLong(), equalTo(0L));

        var coordinatingOperationsCurrentTotal = getSingleRecordedMetric(
            plugin::getLongGaugeMeasurement,
            "es.indexing.coordinating_operations.current.total"
        );
        assertThat(coordinatingOperationsCurrentTotal.getLong(), equalTo(0L));

        var coordinatingOperationsRejectionsTotal = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.coordinating_operations.rejections.total"
        );
        assertThat(coordinatingOperationsRejectionsTotal.getLong(), equalTo(0L));

        var coordinatingOperationsRejectionsRatio = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.coordinating_operations.requests.total"
        );
        // Note: `delete` request goes thru `TransportBulkAction` invoking coordinating/primary limit checks
        assertThat(coordinatingOperationsRejectionsRatio.getLong(), equalTo((long) docsCount + deletesCount));

        var primaryOperationsSize = getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.size");
        assertThat(primaryOperationsSize.getLong(), greaterThan(0L));

        var primaryOperationsTotal = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.primary_operations.total"
        );
        // Note: `delete` request goes thru `TransportBulkAction` invoking coordinating/primary limit checks
        assertThat(primaryOperationsTotal.getLong(), equalTo((long) docsCount + deletesCount));

        var primaryOperationsCurrentSize = getSingleRecordedMetric(
            plugin::getLongGaugeMeasurement,
            "es.indexing.primary_operations.current.size"
        );
        assertThat(primaryOperationsCurrentSize.getLong(), equalTo(0L));

        var primaryOperationsCurrentTotal = getSingleRecordedMetric(
            plugin::getLongGaugeMeasurement,
            "es.indexing.primary_operations.current.total"
        );
        assertThat(primaryOperationsCurrentTotal.getLong(), equalTo(0L));

        var primaryOperationsRejectionsTotal = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.primary_operations.rejections.total"
        );
        assertThat(primaryOperationsRejectionsTotal.getLong(), equalTo(0L));

        var primaryOperationsDocumentRejectionsRatio = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.primary_operations.document.rejections.total"
        );
        assertThat(primaryOperationsDocumentRejectionsRatio.getLong(), equalTo(0L));

    }

    public void testCoordinatingRejectionMetricsArePublishing() {

        // lower Indexing Pressure limits to trigger coordinating rejections
        final String dataNode = internalCluster().startNode(Settings.builder().put(MAX_COORDINATING_BYTES.getKey(), "1KB"));
        ensureStableCluster(1);

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, dataNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        assertAcked(prepareCreate("test").get());

        final BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client(dataNode));
        final int batchCount = randomIntBetween(100, 1000);
        for (int i = 0; i < batchCount; i++) {
            bulkRequestBuilder.add(new IndexRequest("test").source("field", randomAlphaOfLength(100)));
        }

        // big batch should not pass thru coordinating limit check
        expectThrows(EsRejectedExecutionException.class, bulkRequestBuilder);

        // simulate async apm `polling` call for metrics
        plugin.collect();

        // this bulk request is too big to pass coordinating limit check, it has to be reported towards `rejections` total metric
        var coordinatingOperationsRejectionsTotal = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.coordinating_operations.rejections.total"
        );
        assertThat(coordinatingOperationsRejectionsTotal.getLong(), equalTo(1L));

        // `requests` metric should remain to `0`
        var coordinatingOperationsRequestsTotal = getSingleRecordedMetric(
            plugin::getLongAsyncCounterMeasurement,
            "es.indexing.coordinating_operations.requests.total"
        );
        assertThat(coordinatingOperationsRequestsTotal.getLong(), equalTo(0L));
    }

    public void testCoordinatingRejectionMetricsSpiking() throws Exception {

        // lower Indexing Pressure limits to trigger coordinating rejections
        final String dataNode = internalCluster().startNode(Settings.builder().put(MAX_COORDINATING_BYTES.getKey(), "1KB"));
        ensureStableCluster(1);

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, dataNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        assertAcked(prepareCreate("test").get());

        // simulate steady processing of bulk requests
        // every request should pass thru coordinating limit check
        int successfulBulkCount = randomIntBetween(10, 200);
        for (int bulk = 0; bulk < successfulBulkCount; bulk++) {
            final BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client(dataNode));
            final int batchSize = randomIntBetween(1, 5);
            for (int i = 0; i < batchSize; i++) {
                bulkRequestBuilder.add(new IndexRequest("test").source("field", randomAlphaOfLength(10)));
            }
            BulkResponse bulkResponse = bulkRequestBuilder.get();
            assertFalse(bulkResponse.hasFailures());
        }

        // simulate async apm `polling` call for metrics
        plugin.collect();

        // assert no rejections were reported
        assertThat(
            getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.coordinating_operations.rejections.total")
                .getLong(),
            equalTo(0L)
        );
        assertThat(
            getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.coordinating_operations.requests.total").getLong(),
            equalTo((long) successfulBulkCount)
        );

        // simulate spike of rejected coordinating operations after steady processing
        int rejectedBulkCount = randomIntBetween(1, 20);
        for (int bulk = 0; bulk < rejectedBulkCount; bulk++) {
            final BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client(dataNode));
            final int batchSize = randomIntBetween(100, 1000);
            for (int i = 0; i < batchSize; i++) {
                bulkRequestBuilder.add(new IndexRequest("test").source("field", randomAlphaOfLength(100)));
            }
            // big batch should not pass thru coordinating limit check
            expectThrows(EsRejectedExecutionException.class, bulkRequestBuilder);
        }

        // simulate async apm `polling` call for metrics
        plugin.collect();

        assertThat(
            getLatestRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.coordinating_operations.rejections.total")
                .getLong(),
            equalTo((long) rejectedBulkCount)
        );
        // number of successfully processed coordinating requests should remain as seen before
        assertThat(
            getLatestRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.coordinating_operations.requests.total").getLong(),
            equalTo((long) successfulBulkCount)
        );

    }

    public void testPrimaryDocumentRejectionMetricsArePublishing() {

        // setting low Indexing Pressure limits to trigger primary rejections
        final String dataNode = internalCluster().startNode(Settings.builder().put(MAX_PRIMARY_BYTES.getKey(), "2KB").build());
        // setting high Indexing Pressure limits to pass coordinating checks
        final String coordinatingNode = internalCluster().startCoordinatingOnlyNode(
            Settings.builder().put(MAX_COORDINATING_BYTES.getKey(), "10MB").build()
        );
        ensureStableCluster(2);

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, dataNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        final int numberOfShards = randomIntBetween(1, 5);
        assertAcked(prepareCreate("test-one", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)).get());
        assertAcked(prepareCreate("test-two", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)).get());

        final BulkRequest bulkRequestOne = new BulkRequest();
        final int batchCountOne = randomIntBetween(50, 100);
        for (int i = 0; i < batchCountOne; i++) {
            bulkRequestOne.add(new IndexRequest("test-one").source("field", randomAlphaOfLength(3096)));
        }

        final BulkRequest bulkRequestTwo = new BulkRequest();
        final int batchCountTwo = randomIntBetween(1, 5);
        for (int i = 0; i < batchCountTwo; i++) {
            bulkRequestTwo.add(new IndexRequest("test-two").source("field", randomAlphaOfLength(1)));
        }

        // big batch should pass through coordinating gate but trip on primary gate
        // note the bulk request is sent to coordinating node
        final BulkResponse bulkResponseOne = client(coordinatingNode).bulk(bulkRequestOne).actionGet();
        assertThat(bulkResponseOne.hasFailures(), equalTo(true));
        assertThat(
            Arrays.stream(bulkResponseOne.getItems()).allMatch(item -> item.status() == RestStatus.TOO_MANY_REQUESTS),
            equalTo(true)
        );
        // small bulk request is expected to pass through primary indexing pressure gate
        final BulkResponse bulkResponseTwo = client(coordinatingNode).bulk(bulkRequestTwo).actionGet();
        assertThat(bulkResponseTwo.hasFailures(), equalTo(false));

        // simulate async apm `polling` call for metrics
        plugin.collect();

        // this bulk request is too big to pass coordinating limit check
        assertThat(
            getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.rejections.total").getLong(),
            equalTo((long) numberOfShards)
        );

        // all unsuccessful indexing operations (aka documents) should be reported towards `.document.rejections.total` metric
        assertThat(
            getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.document.rejections.total")
                .getLong(),
            equalTo((long) batchCountOne)
        );

        // all successful indexing operations (aka documents) should be reported towards `.primary_operations.total` metric
        assertThat(
            getSingleRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.total").getLong(),
            equalTo((long) batchCountTwo)
        );
    }

    public void testPrimaryDocumentRejectionMetricsFluctuatingOverTime() throws Exception {

        // setting low Indexing Pressure limits to trigger primary rejections
        final String dataNode = internalCluster().startNode(Settings.builder().put(MAX_PRIMARY_BYTES.getKey(), "4KB").build());
        // setting high Indexing Pressure limits to pass coordinating checks
        final String coordinatingNode = internalCluster().startCoordinatingOnlyNode(
            Settings.builder().put(MAX_COORDINATING_BYTES.getKey(), "100MB").build()
        );
        ensureStableCluster(2);

        // for simplicity do not mix small and big documents in single index/shard
        assertAcked(prepareCreate("test-index-one", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)).get());
        assertAcked(prepareCreate("test-index-two", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)).get());

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, dataNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        plugin.resetMeter();

        // how many times are we going to gauge metrics
        // simulate time flow and assert that results of previous calls do not impact current metric readings
        int numberOfMetricCollectionRounds = randomIntBetween(2, 10);
        logger.debug("--> running {} rounds of gauging metrics", numberOfMetricCollectionRounds);

        // to simulate cumulative property of underneath metric counters
        int prevRejectedDocumentsNumber = 0;
        int prevAcceptedDocumentsNumber = 0;

        for (int i = 0; i < numberOfMetricCollectionRounds; i++) {

            final BulkRequest bulkRequestOne = new BulkRequest();

            // construct bulk request of small and big documents (big are not supposed to pass thru a primary memory limit gate)
            int acceptedDocumentsNumber = randomIntBetween(1, 5);
            for (int j = 0; j < acceptedDocumentsNumber; j++) {
                bulkRequestOne.add(new IndexRequest("test-index-one").source("field", randomAlphaOfLength(1)));
            }

            final BulkRequest bulkRequestTwo = new BulkRequest();
            int rejectedDocumentsNumber = randomIntBetween(1, 20);
            for (int j = 0; j < rejectedDocumentsNumber; j++) {
                bulkRequestTwo.add(new IndexRequest("test-index-two").source("field", randomAlphaOfLength(5120)));
            }

            logger.debug("--> round: {}, small docs: {}, big docs: {}", i, acceptedDocumentsNumber, rejectedDocumentsNumber);

            // requests are sent thru coordinating node

            final BulkResponse bulkResponseOne = client(coordinatingNode).bulk(bulkRequestOne).actionGet();
            assertThat(bulkResponseOne.hasFailures(), equalTo(false));

            final BulkResponse bulkResponseTwo = client(coordinatingNode).bulk(bulkRequestTwo).actionGet();
            assertThat(bulkResponseTwo.hasFailures(), equalTo(true));
            assertThat(
                Arrays.stream(bulkResponseTwo.getItems()).filter(r -> r.status() == RestStatus.TOO_MANY_REQUESTS).count(),
                equalTo((long) rejectedDocumentsNumber)
            );

            // simulate async apm `polling` call for metrics
            plugin.collect();

            // all unsuccessful indexing operations (aka documents) should be reported towards `.document.rejections.total` metric
            assertThat(
                getLatestRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.document.rejections.total")
                    .getLong(),
                equalTo((long) rejectedDocumentsNumber + prevRejectedDocumentsNumber)
            );
            prevRejectedDocumentsNumber += rejectedDocumentsNumber;

            // all successful indexing operations (aka documents) should be reported towards `.primary_operations.total` metric
            assertThat(
                getLatestRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.total").getLong(),
                equalTo((long) acceptedDocumentsNumber + prevAcceptedDocumentsNumber)
            );
            prevAcceptedDocumentsNumber += acceptedDocumentsNumber;

        }
    }

    // Borrowed this test from IncrementalBulkIT and added test for metrics to it
    public void testIncrementalBulkLowWatermarkSplitMetrics() throws Exception {
        final String nodeName = internalCluster().startNode(
            Settings.builder()
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), "512B")
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE.getKey(), "2048B")
                .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.getKey(), "4KB")
                .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK_SIZE.getKey(), "1024B")
                .build()
        );
        ensureStableCluster(1);

        String index = "test";
        createIndex(index);

        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, nodeName);
        IndexingPressure indexingPressure = internalCluster().getInstance(IndexingPressure.class, nodeName);
        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        AtomicBoolean nextPage = new AtomicBoolean(false);

        IndexRequest indexRequest = indexRequest(index);
        long total = indexRequest.ramBytesUsed();
        while (total < 2048) {
            refCounted.incRef();
            handler.addItems(List.of(indexRequest), refCounted::decRef, () -> nextPage.set(true));
            assertTrue(nextPage.get());
            nextPage.set(false);
            indexRequest = indexRequest(index);
            total += indexRequest.ramBytesUsed();
        }

        assertThat(indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), greaterThan(0L));
        assertThat(indexingPressure.stats().getLowWaterMarkSplits(), equalTo(0L));

        testTelemetryPlugin.collect();
        assertThat(
            getSingleRecordedMetric(
                testTelemetryPlugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating.low_watermark_splits.total"
            ).getLong(),
            equalTo(0L)
        );
        assertThat(
            getSingleRecordedMetric(
                testTelemetryPlugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating.high_watermark_splits.total"
            ).getLong(),
            equalTo(0L)
        );

        refCounted.incRef();
        handler.addItems(List.of(indexRequest(index)), refCounted::decRef, () -> nextPage.set(true));

        assertBusy(() -> assertThat(indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), equalTo(0L)));
        assertBusy(() -> assertThat(indexingPressure.stats().getLowWaterMarkSplits(), equalTo(1L)));
        assertThat(indexingPressure.stats().getHighWaterMarkSplits(), equalTo(0L));

        testTelemetryPlugin.collect();
        assertThat(
            getLatestRecordedMetric(
                testTelemetryPlugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating.low_watermark_splits.total"
            ).getLong(),
            equalTo(1L)
        );
        assertThat(
            getLatestRecordedMetric(
                testTelemetryPlugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating.high_watermark_splits.total"
            ).getLong(),
            equalTo(0L)
        );

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        handler.lastItems(List.of(indexRequest), refCounted::decRef, future);

        BulkResponse bulkResponse = safeGet(future);
        assertNoFailures(bulkResponse);
        assertFalse(refCounted.hasReferences());
    }

    // Borrowed this test from IncrementalBulkIT and added test for metrics to it
    public void testIncrementalBulkHighWatermarkSplitMetrics() throws Exception {
        final String nodeName = internalCluster().startNode(
            Settings.builder()
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), "512B")
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE.getKey(), "2048B")
                .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.getKey(), "4KB")
                .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK_SIZE.getKey(), "1024B")
                .build()
        );
        ensureStableCluster(1);

        String index = "test";
        createIndex(index);

        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, nodeName);
        IndexingPressure indexingPressure = internalCluster().getInstance(IndexingPressure.class, nodeName);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
        final TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        testTelemetryPlugin.resetMeter();

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        AtomicBoolean nextPage = new AtomicBoolean(false);

        ArrayList<IncrementalBulkService.Handler> handlers = new ArrayList<>();
        for (int i = 0; i < 4; ++i) {
            ArrayList<DocWriteRequest<?>> requests = new ArrayList<>();
            add512BRequests(requests, index);
            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
            handlers.add(handler);
            refCounted.incRef();
            handler.addItems(requests, refCounted::decRef, () -> nextPage.set(true));
            assertTrue(nextPage.get());
            nextPage.set(false);
        }

        // Test that a request smaller than SPLIT_BULK_HIGH_WATERMARK_SIZE (1KB) is not throttled
        ArrayList<DocWriteRequest<?>> requestsNoThrottle = new ArrayList<>();
        add512BRequests(requestsNoThrottle, index);
        IncrementalBulkService.Handler handlerNoThrottle = incrementalBulkService.newBulkRequest();
        handlers.add(handlerNoThrottle);
        refCounted.incRef();
        handlerNoThrottle.addItems(requestsNoThrottle, refCounted::decRef, () -> nextPage.set(true));
        assertTrue(nextPage.get());
        nextPage.set(false);
        assertThat(indexingPressure.stats().getHighWaterMarkSplits(), equalTo(0L));

        testTelemetryPlugin.collect();
        assertThat(
            getSingleRecordedMetric(
                testTelemetryPlugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating.low_watermark_splits.total"
            ).getLong(),
            equalTo(0L)
        );
        assertThat(
            getSingleRecordedMetric(
                testTelemetryPlugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating.high_watermark_splits.total"
            ).getLong(),
            equalTo(0L)
        );

        ArrayList<DocWriteRequest<?>> requestsThrottle = new ArrayList<>();
        // Test that a request larger than SPLIT_BULK_HIGH_WATERMARK_SIZE (1KB) is throttled
        add512BRequests(requestsThrottle, index);
        add512BRequests(requestsThrottle, index);

        CountDownLatch finishLatch = new CountDownLatch(1);
        blockWritePool(threadPool, finishLatch);
        IncrementalBulkService.Handler handlerThrottled = incrementalBulkService.newBulkRequest();
        refCounted.incRef();
        handlerThrottled.addItems(requestsThrottle, refCounted::decRef, () -> nextPage.set(true));
        assertFalse(nextPage.get());
        finishLatch.countDown();

        handlers.add(handlerThrottled);

        // Wait until we are ready for the next page
        assertBusy(() -> assertTrue(nextPage.get()));
        assertBusy(() -> assertThat(indexingPressure.stats().getHighWaterMarkSplits(), equalTo(1L)));
        assertThat(indexingPressure.stats().getLowWaterMarkSplits(), equalTo(0L));

        testTelemetryPlugin.collect();
        assertThat(
            getLatestRecordedMetric(
                testTelemetryPlugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating.low_watermark_splits.total"
            ).getLong(),
            equalTo(0L)
        );
        assertThat(
            getLatestRecordedMetric(
                testTelemetryPlugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating.high_watermark_splits.total"
            ).getLong(),
            equalTo(1L)
        );

        for (IncrementalBulkService.Handler h : handlers) {
            refCounted.incRef();
            PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
            h.lastItems(List.of(indexRequest(index)), refCounted::decRef, future);
            BulkResponse bulkResponse = safeGet(future);
            assertNoFailures(bulkResponse);
        }

        assertBusy(() -> assertThat(indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), equalTo(0L)));
        refCounted.decRef();
        assertFalse(refCounted.hasReferences());
        testTelemetryPlugin.collect();
    }

    private static Measurement getSingleRecordedMetric(Function<String, List<Measurement>> metricGetter, String name) {
        final List<Measurement> measurements = metricGetter.apply(name);
        assertFalse("Indexing metric is not recorded", measurements.isEmpty());
        assertThat(measurements.size(), equalTo(1));
        return measurements.get(0);
    }

    private static Measurement getLatestRecordedMetric(Function<String, List<Measurement>> metricGetter, String name) {
        final List<Measurement> measurements = metricGetter.apply(name);
        assertFalse("Indexing metric is not recorded", measurements.isEmpty());
        return measurements.get(measurements.size() - 1);
    }

    private static boolean doublesEquals(double expected, double actual) {
        final double eps = .0000001;
        return Math.abs(expected - actual) < eps;
    }

    private static IndexRequest indexRequest(String index) {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.source(Map.of("field", randomAlphaOfLength(10)));
        return indexRequest;
    }

    private static void add512BRequests(ArrayList<DocWriteRequest<?>> requests, String index) {
        long total = 0;
        while (total < 512) {
            IndexRequest indexRequest = indexRequest(index);
            requests.add(indexRequest);
            total += indexRequest.ramBytesUsed();
        }
        assertThat(total, lessThan(1024L));
    }

    private static void blockWritePool(ThreadPool threadPool, CountDownLatch finishLatch) {
        final var threadCount = threadPool.info(ThreadPool.Names.WRITE).getMax();
        final var startBarrier = new CyclicBarrier(threadCount + 1);
        final var blockingTask = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            protected void doRun() {
                safeAwait(startBarrier);
                safeAwait(finishLatch);
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        };
        for (int i = 0; i < threadCount; i++) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(blockingTask);
        }
        safeAwait(startBarrier);
    }
}

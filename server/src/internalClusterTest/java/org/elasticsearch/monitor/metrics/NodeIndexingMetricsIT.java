/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.IndexingPressure.MAX_COORDINATING_BYTES;
import static org.elasticsearch.index.IndexingPressure.MAX_PRIMARY_BYTES;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

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
}

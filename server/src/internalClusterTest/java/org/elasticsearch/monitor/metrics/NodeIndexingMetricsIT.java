/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
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

import static org.elasticsearch.index.IndexingPressure.MAX_INDEXING_BYTES;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeIndexingMetricsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class);
    }

    public void testNodeIndexingMetricsArePublishing() throws Exception {

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

        assertBusy(() -> {
            var indexingTotal = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.docs.total");
            assertThat(indexingTotal.getLong(), equalTo((long) docsCount));

            var indexingCurrent = getRecordedMetric(plugin::getLongGaugeMeasurement, "es.indexing.docs.current.total");
            assertThat(indexingCurrent.getLong(), equalTo(0L));

            var indexingFailedTotal = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.indexing.failed.total");
            assertThat(indexingFailedTotal.getLong(), equalTo(0L));

            var deletionTotal = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.deletion.docs.total");
            assertThat(deletionTotal.getLong(), equalTo((long) deletesCount));

            var deletionCurrent = getRecordedMetric(plugin::getLongGaugeMeasurement, "es.indexing.deletion.docs.current.total");
            assertThat(deletionCurrent.getLong(), equalTo(0L));

            var indexingTime = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.time");
            assertThat(indexingTime.getLong(), greaterThan(0L));

            var deletionTime = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.deletion.time");
            assertThat(deletionTime.getLong(), greaterThanOrEqualTo(0L));

            var throttleTime = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indices.throttle.time");
            assertThat(throttleTime.getLong(), equalTo(0L));

            var noopTotal = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indices.noop.total");
            assertThat(noopTotal.getLong(), equalTo(0L));

            var coordinatingOperationsSize = getRecordedMetric(
                plugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating_operations.size"
            );
            assertThat(coordinatingOperationsSize.getLong(), greaterThan(0L));

            var coordinatingOperationsTotal = getRecordedMetric(
                plugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating_operations.total"
            );
            // Note: `delete` request goes thru `TransportBulkAction` invoking coordinating/primary limit checks
            assertThat(coordinatingOperationsTotal.getLong(), equalTo((long) docsCount + deletesCount));

            var coordinatingOperationsCurrentSize = getRecordedMetric(
                plugin::getLongGaugeMeasurement,
                "es.indexing.coordinating_operations.current.size"
            );
            assertThat(coordinatingOperationsCurrentSize.getLong(), equalTo(0L));

            var coordinatingOperationsCurrentTotal = getRecordedMetric(
                plugin::getLongGaugeMeasurement,
                "es.indexing.coordinating_operations.current.total"
            );
            assertThat(coordinatingOperationsCurrentTotal.getLong(), equalTo(0L));

            var coordinatingOperationsRejectionsTotal = getRecordedMetric(
                plugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating_operations.rejections.total"
            );
            assertThat(coordinatingOperationsRejectionsTotal.getLong(), equalTo(0L));

            var coordinatingOperationsRejectionsRatio = getRecordedMetric(
                plugin::getDoubleGaugeMeasurement,
                "es.indexing.coordinating_operations.request.rejections.ratio"
            );
            assertThat(coordinatingOperationsRejectionsRatio.getDouble(), equalTo(0.0));

            var primaryOperationsSize = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.size");
            assertThat(primaryOperationsSize.getLong(), greaterThan(0L));

            var primaryOperationsTotal = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.total");
            // Note: `delete` request goes thru `TransportBulkAction` invoking coordinating/primary limit checks
            assertThat(primaryOperationsTotal.getLong(), equalTo((long) docsCount + deletesCount));

            var primaryOperationsCurrentSize = getRecordedMetric(
                plugin::getLongGaugeMeasurement,
                "es.indexing.primary_operations.current.size"
            );
            assertThat(primaryOperationsCurrentSize.getLong(), equalTo(0L));

            var primaryOperationsCurrentTotal = getRecordedMetric(
                plugin::getLongGaugeMeasurement,
                "es.indexing.primary_operations.current.total"
            );
            assertThat(primaryOperationsCurrentTotal.getLong(), equalTo(0L));

            var primaryOperationsRejectionsTotal = getRecordedMetric(
                plugin::getLongAsyncCounterMeasurement,
                "es.indexing.primary_operations.rejections.total"
            );
            assertThat(primaryOperationsRejectionsTotal.getLong(), equalTo(0L));

            var primaryOperationsDocumentRejectionsRatio = getRecordedMetric(
                plugin::getDoubleGaugeMeasurement,
                "es.indexing.primary_operations.document.rejections.ratio"
            );
            assertThat(primaryOperationsDocumentRejectionsRatio.getDouble(), equalTo(0.0));

        });

    }

    public void testCoordinatingRejectionMetricsArePublishing() throws Exception {

        // lower Indexing Pressure limits to trigger coordinating rejections
        final String dataNode = internalCluster().startNode(Settings.builder().put(MAX_INDEXING_BYTES.getKey(), "1KB"));
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

        // this bulk request is too big to pass coordinating limit check
        assertBusy(() -> {
            var coordinatingOperationsRejectionsTotal = getRecordedMetric(
                plugin::getLongAsyncCounterMeasurement,
                "es.indexing.coordinating_operations.rejections.total"
            );
            assertThat(coordinatingOperationsRejectionsTotal.getLong(), equalTo(1L));

            var coordinatingOperationsRejectionsRatio = getRecordedMetric(
                plugin::getDoubleGaugeMeasurement,
                "es.indexing.coordinating_operations.request.rejections.ratio"
            );
            assertThat(coordinatingOperationsRejectionsRatio.getDouble(), equalTo(1.0));
        });
    }

    public void testPrimaryDocumentRejectionMetricsArePublishing() throws Exception {

        // setting low Indexing Pressure limits to trigger primary rejections
        final String dataNode = internalCluster().startNode(Settings.builder().put(MAX_INDEXING_BYTES.getKey(), "2KB").build());
        // setting high Indexing Pressure limits to pass coordinating checks
        final String coordinatingNode = internalCluster().startCoordinatingOnlyNode(
            Settings.builder().put(MAX_INDEXING_BYTES.getKey(), "10MB").build()
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
        assertBusy(() -> {
            var primaryOperationsRejectionsTotal = getRecordedMetric(
                plugin::getLongAsyncCounterMeasurement,
                "es.indexing.primary_operations.rejections.total"
            );
            assertThat(primaryOperationsRejectionsTotal.getLong(), equalTo((long) numberOfShards));

            var primaryOperationsDocumentRejectionsRatio = getRecordedMetric(
                plugin::getDoubleGaugeMeasurement,
                "es.indexing.primary_operations.document.rejections.ratio"
            );
            // ratio of rejected documents vs all indexing documents
            assertThat(
                equals(primaryOperationsDocumentRejectionsRatio.getDouble(), (double) batchCountOne / (batchCountOne + batchCountTwo)),
                equalTo(true)
            );
        });

    }

    private static Measurement getRecordedMetric(Function<String, List<Measurement>> metricGetter, String name) {
        final List<Measurement> measurements = metricGetter.apply(name);
        assertFalse("Indexing metric is not recorded", measurements.isEmpty());
        assertThat(measurements.size(), equalTo(1));
        return measurements.get(0);
    }

    private static boolean equals(double expected, double actual) {
        final double eps = .0000001;
        return Math.abs(expected - actual) < eps;
    }
}

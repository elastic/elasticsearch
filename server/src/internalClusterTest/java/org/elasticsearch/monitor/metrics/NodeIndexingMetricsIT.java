/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeIndexingMetricsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class);
    }

    public void testNodeIndexingMetricsArePublishing() throws Exception {

        final String node = internalCluster().startNode();
        ensureStableCluster(1);

        final TestTelemetryPlugin plugin = internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        plugin.resetMeter();

        assertAcked(prepareCreate("test", 1).get());

        final int docsCount = randomIntBetween(500, 1000);
        for (int i = 0; i < docsCount; i++) {
            var indexResponse = client().index(new IndexRequest("test").id("doc_" + i).source(Map.of("key", i, "val", i))).actionGet();
            // check that all documents were created successfully since metric counters below assume that
            assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
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
            assertThat(deletionTotal.getLong(), equalTo(0L));

            var deletionCurrent = getRecordedMetric(plugin::getLongGaugeMeasurement, "es.indexing.deletion.docs.current.total");
            assertThat(deletionCurrent.getLong(), equalTo(0L));

            var indexingTime = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.time");
            assertThat(indexingTime.getLong(), greaterThan(0L));

            var deletionTime = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.deletion.time");
            assertThat(deletionTime.getLong(), equalTo(0L));

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
            assertThat(coordinatingOperationsTotal.getLong(), equalTo((long) docsCount));

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
                "es.indexing.coordinating_operations.rejections.ratio"
            );
            assertThat(coordinatingOperationsRejectionsRatio.getDouble(), equalTo(0.0));

            var primaryOperationsSize = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.size");
            assertThat(primaryOperationsSize.getLong(), greaterThan(0L));

            var primaryOperationsTotal = getRecordedMetric(plugin::getLongAsyncCounterMeasurement, "es.indexing.primary_operations.total");
            assertThat(primaryOperationsTotal.getLong(), equalTo((long) docsCount));

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

            var primaryOperationsRejectionsRatio = getRecordedMetric(
                plugin::getDoubleGaugeMeasurement,
                "es.indexing.primary_operations.rejections.ratio"
            );
            assertThat(primaryOperationsRejectionsRatio.getDouble(), equalTo(0.0));

        });

    }

    private static Measurement getRecordedMetric(Function<String, List<Measurement>> metricGetter, String name) {
        final List<Measurement> measurements = metricGetter.apply(name);
        assertFalse("Indexing metric is not recorded", measurements.isEmpty());
        assertThat(measurements.size(), equalTo(1));
        return measurements.get(0);
    }
}

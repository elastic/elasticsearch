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

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesOCCMetricsIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    public void testOCCOpsAreTrackedOnTimeSeriesIndex() {
        var indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        var telemetry = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        telemetry.resetMeter();

        var indexName = randomIndexName();
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings(1, 0).put("mode", "time_series").putList("routing_path", List.of("host")).build())
            .setMapping(
                "@timestamp",
                "type=date",
                "host",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=long,time_series_metric=gauge"
            )
            .get();
        ensureGreen(indexName);

        // Index some docs without an OCC constraint
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        int numDocs = randomIntBetween(1, 5);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName).setSource("@timestamp", timestamp, "host", "prod-" + i, "cpu", i).get();
        }

        telemetry.collect();
        assertThat(
            telemetry.getLongAsyncCounterMeasurement(TimeSeriesOCCMetrics.TSDB_OPERATIONS_TOTAL_METRIC).getLast().getLong(),
            equalTo((long) numDocs)
        );
        assertThat(
            telemetry.getLongAsyncCounterMeasurement(TimeSeriesOCCMetrics.TSDB_OCC_OPERATIONS_TOTAL_METRIC).getLast().getLong(),
            equalTo(0L)
        );

        // OCC index op: index a doc, then re-index it using OCC
        timestamp += 1000L;
        var baseDocResponse = client().prepareIndex(indexName).setSource("@timestamp", timestamp, "host", "prod", "cpu", 99).get();
        var occResponse = client().prepareIndex(indexName)
            .setId(baseDocResponse.getId())
            .setSource("@timestamp", timestamp, "host", "prod", "cpu", 100)
            .setIfSeqNo(baseDocResponse.getSeqNo())
            .setIfPrimaryTerm(baseDocResponse.getPrimaryTerm())
            .get();

        telemetry.collect();
        assertThat(
            telemetry.getLongAsyncCounterMeasurement(TimeSeriesOCCMetrics.TSDB_OPERATIONS_TOTAL_METRIC).getLast().getLong(),
            equalTo((long) numDocs + 2)
        );
        assertThat(
            telemetry.getLongAsyncCounterMeasurement(TimeSeriesOCCMetrics.TSDB_OCC_OPERATIONS_TOTAL_METRIC).getLast().getLong(),
            equalTo(1L)
        );

        // OCC delete op: delete the doc using OCC
        client().prepareDelete(indexName, baseDocResponse.getId())
            .setIfSeqNo(occResponse.getSeqNo())
            .setIfPrimaryTerm(occResponse.getPrimaryTerm())
            .get();

        telemetry.collect();
        assertThat(
            telemetry.getLongAsyncCounterMeasurement(TimeSeriesOCCMetrics.TSDB_OPERATIONS_TOTAL_METRIC).getLast().getLong(),
            equalTo((long) numDocs + 3)
        );
        assertThat(
            telemetry.getLongAsyncCounterMeasurement(TimeSeriesOCCMetrics.TSDB_OCC_OPERATIONS_TOTAL_METRIC).getLast().getLong(),
            equalTo(2L)
        );
    }

    public void testOCCOpsAreNotTrackedOnStandardIndex() {
        var indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        var telemetry = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        telemetry.resetMeter();

        var indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        // Plain index ops
        int numDocs = randomIntBetween(1, 5);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(indexName, Integer.toString(i), "field", "value");
        }

        // OCC index op
        var baseDocResponse = client().prepareIndex(indexName).setSource("field", "base").get();
        var occResponse = client().prepareIndex(indexName)
            .setId(baseDocResponse.getId())
            .setSource("field", "occ")
            .setIfSeqNo(baseDocResponse.getSeqNo())
            .setIfPrimaryTerm(baseDocResponse.getPrimaryTerm())
            .get();

        // OCC delete op
        client().prepareDelete(indexName, baseDocResponse.getId())
            .setIfSeqNo(occResponse.getSeqNo())
            .setIfPrimaryTerm(occResponse.getPrimaryTerm())
            .get();

        // The listener is not registered for standard indices: both counters must read zero
        telemetry.collect();
        assertThat(
            telemetry.getLongAsyncCounterMeasurement(TimeSeriesOCCMetrics.TSDB_OPERATIONS_TOTAL_METRIC).getLast().getLong(),
            equalTo(0L)
        );
        assertThat(
            telemetry.getLongAsyncCounterMeasurement(TimeSeriesOCCMetrics.TSDB_OCC_OPERATIONS_TOTAL_METRIC).getLast().getLong(),
            equalTo(0L)
        );
    }
}

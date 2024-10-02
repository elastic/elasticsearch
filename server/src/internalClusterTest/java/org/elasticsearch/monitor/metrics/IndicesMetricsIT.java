/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class IndicesMetricsIT extends ESIntegTestCase {

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

    static final String STANDARD_INDEX_COUNT = "es.indices.standard.total";
    static final String STANDARD_BYTES_SIZE = "es.indices.standard.size";
    static final String STANDARD_DOCS_COUNT = "es.indices.standard.docs.total";
    static final String STANDARD_QUERY_COUNT = "es.indices.standard.query.total";
    static final String STANDARD_QUERY_TIME = "es.indices.standard.query.time";
    static final String STANDARD_FETCH_COUNT = "es.indices.standard.fetch.total";
    static final String STANDARD_FETCH_TIME = "es.indices.standard.fetch.time";

    static final String TIME_SERIES_INDEX_COUNT = "es.indices.time_series.total";
    static final String TIME_SERIES_BYTES_SIZE = "es.indices.time_series.size";
    static final String TIME_SERIES_DOCS_COUNT = "es.indices.time_series.docs.total";
    static final String TIME_SERIES_QUERY_COUNT = "es.indices.time_series.query.total";
    static final String TIME_SERIES_QUERY_TIME = "es.indices.time_series.query.time";
    static final String TIME_SERIES_FETCH_COUNT = "es.indices.time_series.fetch.total";
    static final String TIME_SERIES_FETCH_TIME = "es.indices.time_series.fetch.time";

    static final String LOGSDB_INDEX_COUNT = "es.indices.logsdb.total";
    static final String LOGSDB_BYTES_SIZE = "es.indices.logsdb.size";
    static final String LOGSDB_DOCS_COUNT = "es.indices.logsdb.docs.total";
    static final String LOGSDB_QUERY_COUNT = "es.indices.logsdb.query.total";
    static final String LOGSDB_QUERY_TIME = "es.indices.logsdb.query.time";
    static final String LOGSDB_FETCH_COUNT = "es.indices.logsdb.fetch.total";
    static final String LOGSDB_FETCH_TIME = "es.indices.logsdb.fetch.time";

    public void testIndicesMetrics() {
        String node = internalCluster().startNode();
        ensureStableCluster(1);
        final TestTelemetryPlugin telemetry = internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        telemetry.resetMeter();
        long numStandardIndices = randomIntBetween(1, 5);
        long numStandardDocs = populateStandardIndices(numStandardIndices);
        collectThenAssertMetrics(
            telemetry,
            MetricType.GAUGE,
            1,
            Map.of(
                STANDARD_INDEX_COUNT,
                equalTo(numStandardIndices),
                STANDARD_DOCS_COUNT,
                equalTo(numStandardDocs),
                STANDARD_BYTES_SIZE,
                greaterThan(0L),

                TIME_SERIES_INDEX_COUNT,
                equalTo(0L),
                TIME_SERIES_DOCS_COUNT,
                equalTo(0L),
                TIME_SERIES_BYTES_SIZE,
                equalTo(0L),

                LOGSDB_INDEX_COUNT,
                equalTo(0L),
                LOGSDB_DOCS_COUNT,
                equalTo(0L),
                LOGSDB_BYTES_SIZE,
                equalTo(0L)
            )
        );

        long numTimeSeriesIndices = randomIntBetween(1, 5);
        long numTimeSeriesDocs = populateTimeSeriesIndices(numTimeSeriesIndices);
        collectThenAssertMetrics(
            telemetry,
            MetricType.GAUGE,
            2,
            Map.of(
                STANDARD_INDEX_COUNT,
                equalTo(numStandardIndices),
                STANDARD_DOCS_COUNT,
                equalTo(numStandardDocs),
                STANDARD_BYTES_SIZE,
                greaterThan(0L),

                TIME_SERIES_INDEX_COUNT,
                equalTo(numTimeSeriesIndices),
                TIME_SERIES_DOCS_COUNT,
                equalTo(numTimeSeriesDocs),
                TIME_SERIES_BYTES_SIZE,
                greaterThan(20L),

                LOGSDB_INDEX_COUNT,
                equalTo(0L),
                LOGSDB_DOCS_COUNT,
                equalTo(0L),
                LOGSDB_BYTES_SIZE,
                equalTo(0L)
            )
        );

        long numLogsdbIndices = randomIntBetween(1, 5);
        long numLogsdbDocs = populateLogsdbIndices(numLogsdbIndices);
        collectThenAssertMetrics(
            telemetry,
            MetricType.GAUGE,
            3,
            Map.of(
                STANDARD_INDEX_COUNT,
                equalTo(numStandardIndices),
                STANDARD_DOCS_COUNT,
                equalTo(numStandardDocs),
                STANDARD_BYTES_SIZE,
                greaterThan(0L),

                TIME_SERIES_INDEX_COUNT,
                equalTo(numTimeSeriesIndices),
                TIME_SERIES_DOCS_COUNT,
                equalTo(numTimeSeriesDocs),
                TIME_SERIES_BYTES_SIZE,
                greaterThan(20L),

                LOGSDB_INDEX_COUNT,
                equalTo(numLogsdbIndices),
                LOGSDB_DOCS_COUNT,
                equalTo(numLogsdbDocs),
                LOGSDB_BYTES_SIZE,
                greaterThan(0L)
            )
        );
        telemetry.resetMeter();

        // search and fetch
        client().prepareSearch("standard*").setSize(100).get().decRef();
        var nodeStats1 = indicesService.stats(CommonStatsFlags.ALL, false).getSearch().getTotal();
        collectThenAssertMetrics(
            telemetry,
            MetricType.COUNTER,
            1,
            Map.of(
                STANDARD_QUERY_COUNT,
                equalTo(numStandardIndices),
                STANDARD_QUERY_TIME,
                equalTo(nodeStats1.getQueryTimeInMillis()),
                STANDARD_FETCH_COUNT,
                equalTo(nodeStats1.getFetchCount()),
                STANDARD_FETCH_TIME,
                equalTo(nodeStats1.getFetchTimeInMillis()),

                TIME_SERIES_QUERY_COUNT,
                equalTo(0L),
                TIME_SERIES_QUERY_TIME,
                equalTo(0L),

                LOGSDB_QUERY_COUNT,
                equalTo(0L),
                LOGSDB_QUERY_TIME,
                equalTo(0L)
            )
        );

        client().prepareSearch("time*").setSize(100).get().decRef();
        var nodeStats2 = indicesService.stats(CommonStatsFlags.ALL, false).getSearch().getTotal();
        collectThenAssertMetrics(
            telemetry,
            MetricType.COUNTER,
            2,
            Map.of(
                STANDARD_QUERY_COUNT,
                equalTo(numStandardIndices),
                STANDARD_QUERY_TIME,
                equalTo(nodeStats1.getQueryTimeInMillis()),

                TIME_SERIES_QUERY_COUNT,
                equalTo(numTimeSeriesIndices),
                TIME_SERIES_QUERY_TIME,
                equalTo(nodeStats2.getQueryTimeInMillis() - nodeStats1.getQueryTimeInMillis()),
                TIME_SERIES_FETCH_COUNT,
                equalTo(nodeStats2.getFetchCount() - nodeStats1.getFetchCount()),
                TIME_SERIES_FETCH_TIME,
                equalTo(nodeStats2.getFetchTimeInMillis() - nodeStats1.getFetchTimeInMillis()),

                LOGSDB_QUERY_COUNT,
                equalTo(0L),
                LOGSDB_QUERY_TIME,
                equalTo(0L)
            )
        );
        client().prepareSearch("logs*").setSize(100).get().decRef();
        var nodeStats3 = indicesService.stats(CommonStatsFlags.ALL, false).getSearch().getTotal();
        collectThenAssertMetrics(
            telemetry,
            MetricType.COUNTER,
            3,
            Map.of(
                STANDARD_QUERY_COUNT,
                equalTo(numStandardIndices),
                STANDARD_QUERY_TIME,
                equalTo(nodeStats1.getQueryTimeInMillis()),

                TIME_SERIES_QUERY_COUNT,
                equalTo(numTimeSeriesIndices),
                TIME_SERIES_QUERY_TIME,
                equalTo(nodeStats2.getQueryTimeInMillis() - nodeStats1.getQueryTimeInMillis()),

                LOGSDB_QUERY_COUNT,
                equalTo(numLogsdbIndices),
                LOGSDB_QUERY_TIME,
                equalTo(nodeStats3.getQueryTimeInMillis() - nodeStats2.getQueryTimeInMillis()),
                LOGSDB_FETCH_COUNT,
                equalTo(nodeStats3.getFetchCount() - nodeStats2.getFetchCount()),
                LOGSDB_FETCH_TIME,
                equalTo(nodeStats3.getFetchTimeInMillis() - nodeStats2.getFetchTimeInMillis())
            )
        );
    }

    enum MetricType {
        COUNTER,
        GAUGE
    }

    void collectThenAssertMetrics(TestTelemetryPlugin telemetry, MetricType metricType, int times, Map<String, Matcher<Long>> matchers) {
        telemetry.collect();
        for (Map.Entry<String, Matcher<Long>> e : matchers.entrySet()) {
            String name = e.getKey();
            List<Measurement> measurements = metricType == MetricType.COUNTER
                ? telemetry.getLongAsyncCounterMeasurement(name)
                : telemetry.getLongGaugeMeasurement(name);
            assertThat(name, measurements, hasSize(times));
            assertThat(name, measurements.getLast().getLong(), e.getValue());
        }
    }

    int populateStandardIndices(long numIndices) {
        int totalDocs = 0;
        for (int i = 0; i < numIndices; i++) {
            String indexName = "standard-" + i;
            createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
            int numDocs = between(1, 5);
            for (int d = 0; d < numDocs; d++) {
                indexDoc(indexName, Integer.toString(d), "f", Integer.toString(d));
            }
            totalDocs += numDocs;
            flush(indexName);
        }
        return totalDocs;
    }

    int populateTimeSeriesIndices(long numIndices) {
        int totalDocs = 0;
        for (int i = 0; i < numIndices; i++) {
            String indexName = "time_series-" + i;
            Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put("mode", "time_series")
                .putList("routing_path", List.of("host"))
                .build();
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=long,time_series_metric=gauge"
                )
                .get();
            long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
            int numDocs = between(1, 5);
            for (int d = 0; d < numDocs; d++) {
                timestamp += between(1, 5) * 1000L;
                client().prepareIndex(indexName)
                    .setSource("@timestamp", timestamp, "host", randomFrom("prod", "qa"), "cpu", randomIntBetween(1, 100))
                    .get();
            }
            totalDocs += numDocs;
            flush(indexName);
        }
        return totalDocs;
    }

    int populateLogsdbIndices(long numIndices) {
        int totalDocs = 0;
        for (int i = 0; i < numIndices; i++) {
            String indexName = "logsdb-" + i;
            Settings settings = Settings.builder().put("mode", "logsdb").put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(settings)
                .setMapping("@timestamp", "type=date", "host.name", "type=keyword", "cpu", "type=long")
                .get();
            long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
            int numDocs = between(1, 5);
            for (int d = 0; d < numDocs; d++) {
                timestamp += between(1, 5) * 1000L;
                client().prepareIndex(indexName)
                    .setSource("@timestamp", timestamp, "host.name", randomFrom("prod", "qa"), "cpu", randomIntBetween(1, 100))
                    .get();
            }
            totalDocs += numDocs;
            flush(indexName);
        }
        return totalDocs;
    }
}

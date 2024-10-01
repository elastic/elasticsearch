/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
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
    static final String STANDARD_DOCS_COUNT = "es.indices.standard.docs.total";
    static final String STANDARD_BYTES_SIZE = "es.indices.standard.bytes.total";

    static final String TIME_SERIES_INDEX_COUNT = "es.indices.time_series.total";
    static final String TIME_SERIES_DOCS_COUNT = "es.indices.time_series.docs.total";
    static final String TIME_SERIES_BYTES_SIZE = "es.indices.time_series.bytes.total";

    static final String LOGSDB_INDEX_COUNT = "es.indices.logsdb.total";
    static final String LOGSDB_DOCS_COUNT = "es.indices.logsdb.docs.total";
    static final String LOGSDB_BYTES_SIZE = "es.indices.logsdb.bytes.total";

    public void testIndicesMetrics() {
        String node = internalCluster().startNode();
        ensureStableCluster(1);
        final TestTelemetryPlugin telemetry = internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        telemetry.resetMeter();
        long numStandardIndices = randomIntBetween(1, 5);
        long numStandardDocs = populateStandardIndices(numStandardIndices);
        collectThenAssertMetrics(
            telemetry,
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
    }

    void collectThenAssertMetrics(TestTelemetryPlugin telemetry, int times, Map<String, Matcher<Long>> matchers) {
        telemetry.collect();
        for (Map.Entry<String, Matcher<Long>> e : matchers.entrySet()) {
            String name = e.getKey();
            List<Measurement> measurements = telemetry.getLongGaugeMeasurement(name);
            assertThat(name, measurements, hasSize(times));
            assertThat(name, measurements.getLast().getLong(), e.getValue());
        }
    }

    int populateStandardIndices(long numIndices) {
        int totalDocs = 0;
        for (int i = 0; i < numIndices; i++) {
            String indexName = "standard-" + i;
            createIndex(indexName);
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
            Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host")).build();
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
            Settings settings = Settings.builder().put("mode", "logsdb").build();
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

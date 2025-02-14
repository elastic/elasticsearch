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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
        return List.of(TestTelemetryPlugin.class, TestAPMInternalSettings.class, FailingFieldPlugin.class);
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
    static final String STANDARD_QUERY_FAILURE = "es.indices.standard.query.failure.total";
    static final String STANDARD_FETCH_COUNT = "es.indices.standard.fetch.total";
    static final String STANDARD_FETCH_TIME = "es.indices.standard.fetch.time";
    static final String STANDARD_FETCH_FAILURE = "es.indices.standard.fetch.failure.total";
    static final String STANDARD_INDEXING_COUNT = "es.indices.standard.indexing.total";
    static final String STANDARD_INDEXING_TIME = "es.indices.standard.indexing.time";
    static final String STANDARD_INDEXING_FAILURE = "es.indices.standard.indexing.failure.total";
    static final String STANDARD_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT = "es.indices.standard.indexing.failure.version_conflict.total";

    static final String TIME_SERIES_INDEX_COUNT = "es.indices.time_series.total";
    static final String TIME_SERIES_BYTES_SIZE = "es.indices.time_series.size";
    static final String TIME_SERIES_DOCS_COUNT = "es.indices.time_series.docs.total";
    static final String TIME_SERIES_QUERY_COUNT = "es.indices.time_series.query.total";
    static final String TIME_SERIES_QUERY_TIME = "es.indices.time_series.query.time";
    static final String TIME_SERIES_QUERY_FAILURE = "es.indices.time_series.query.failure.total";
    static final String TIME_SERIES_FETCH_COUNT = "es.indices.time_series.fetch.total";
    static final String TIME_SERIES_FETCH_TIME = "es.indices.time_series.fetch.time";
    static final String TIME_SERIES_FETCH_FAILURE = "es.indices.time_series.fetch.failure.total";
    static final String TIME_SERIES_INDEXING_COUNT = "es.indices.time_series.indexing.total";
    static final String TIME_SERIES_INDEXING_TIME = "es.indices.time_series.indexing.time";
    static final String TIME_SERIES_INDEXING_FAILURE = "es.indices.time_series.indexing.failure.total";
    static final String TIME_SERIES_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT =
        "es.indices.time_series.indexing.failure.version_conflict.total";

    static final String LOGSDB_INDEX_COUNT = "es.indices.logsdb.total";
    static final String LOGSDB_BYTES_SIZE = "es.indices.logsdb.size";
    static final String LOGSDB_DOCS_COUNT = "es.indices.logsdb.docs.total";
    static final String LOGSDB_QUERY_COUNT = "es.indices.logsdb.query.total";
    static final String LOGSDB_QUERY_TIME = "es.indices.logsdb.query.time";
    static final String LOGSDB_QUERY_FAILURE = "es.indices.logsdb.query.failure.total";
    static final String LOGSDB_FETCH_COUNT = "es.indices.logsdb.fetch.total";
    static final String LOGSDB_FETCH_TIME = "es.indices.logsdb.fetch.time";
    static final String LOGSDB_FETCH_FAILURE = "es.indices.logsdb.fetch.failure.total";
    static final String LOGSDB_INDEXING_COUNT = "es.indices.logsdb.indexing.total";
    static final String LOGSDB_INDEXING_TIME = "es.indices.logsdb.indexing.time";
    static final String LOGSDB_INDEXING_FAILURE = "es.indices.logsdb.indexing.failure.total";
    static final String LOGSDB_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT = "es.indices.logsdb.indexing.failure.version_conflict.total";

    public void testIndicesMetrics() {
        String indexNode = internalCluster().startNode();
        ensureStableCluster(1);
        TestTelemetryPlugin telemetry = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        var indexing0 = indicesService.stats(CommonStatsFlags.ALL, false).getIndexing().getTotal();
        telemetry.resetMeter();
        long numStandardIndices = randomIntBetween(1, 5);
        long numStandardDocs = populateStandardIndices(numStandardIndices);
        var indexing1 = indicesService.stats(CommonStatsFlags.ALL, false).getIndexing().getTotal();
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

                STANDARD_INDEXING_COUNT,
                equalTo(numStandardDocs),
                STANDARD_INDEXING_TIME,
                greaterThanOrEqualTo(0L),
                STANDARD_INDEXING_FAILURE,
                equalTo(indexing1.getIndexFailedCount() - indexing0.getIndexFailedCount()),
                STANDARD_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT,
                equalTo(indexing1.getIndexFailedDueToVersionConflictCount() - indexing0.getIndexFailedDueToVersionConflictCount())
            )
        );

        long numTimeSeriesIndices = randomIntBetween(1, 5);
        long numTimeSeriesDocs = populateTimeSeriesIndices(numTimeSeriesIndices);
        var indexing2 = indicesService.stats(CommonStatsFlags.ALL, false).getIndexing().getTotal();
        collectThenAssertMetrics(
            telemetry,
            2,
            Map.of(
                TIME_SERIES_INDEX_COUNT,
                equalTo(numTimeSeriesIndices),
                TIME_SERIES_DOCS_COUNT,
                equalTo(numTimeSeriesDocs),
                TIME_SERIES_BYTES_SIZE,
                greaterThan(20L),

                TIME_SERIES_INDEXING_COUNT,
                equalTo(numTimeSeriesDocs),
                TIME_SERIES_INDEXING_TIME,
                greaterThanOrEqualTo(0L),
                TIME_SERIES_INDEXING_FAILURE,
                equalTo(indexing1.getIndexFailedCount() - indexing0.getIndexFailedCount()),
                TIME_SERIES_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT,
                equalTo(indexing1.getIndexFailedDueToVersionConflictCount() - indexing0.getIndexFailedDueToVersionConflictCount())
            )
        );

        long numLogsdbIndices = randomIntBetween(1, 5);
        long numLogsdbDocs = populateLogsdbIndices(numLogsdbIndices);
        var indexing3 = indicesService.stats(CommonStatsFlags.ALL, false).getIndexing().getTotal();
        collectThenAssertMetrics(
            telemetry,
            3,
            Map.of(
                LOGSDB_INDEX_COUNT,
                equalTo(numLogsdbIndices),
                LOGSDB_DOCS_COUNT,
                equalTo(numLogsdbDocs),
                LOGSDB_BYTES_SIZE,
                greaterThan(0L),
                LOGSDB_INDEXING_COUNT,
                equalTo(numLogsdbDocs),
                LOGSDB_INDEXING_TIME,
                greaterThanOrEqualTo(0L),
                LOGSDB_INDEXING_FAILURE,
                equalTo(indexing3.getIndexFailedCount() - indexing2.getIndexFailedCount()),
                LOGSDB_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT,
                equalTo(indexing3.getIndexFailedDueToVersionConflictCount() - indexing2.getIndexFailedDueToVersionConflictCount())
            )
        );
        // already collected indexing stats
        Map<String, Matcher<Long>> zeroMatchers = new HashMap<>();
        zeroMatchers.putAll(
            Map.of(
                STANDARD_INDEXING_COUNT,
                equalTo(0L),
                STANDARD_INDEXING_TIME,
                equalTo(0L),
                STANDARD_INDEXING_FAILURE,
                equalTo(0L),
                STANDARD_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT,
                equalTo(0L)
            )
        );
        zeroMatchers.putAll(
            Map.of(
                TIME_SERIES_INDEXING_COUNT,
                equalTo(0L),
                TIME_SERIES_INDEXING_TIME,
                equalTo(0L),
                TIME_SERIES_INDEXING_FAILURE,
                equalTo(0L),
                TIME_SERIES_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT,
                equalTo(0L)
            )
        );
        zeroMatchers.putAll(
            Map.of(
                LOGSDB_INDEXING_COUNT,
                equalTo(0L),
                LOGSDB_INDEXING_TIME,
                equalTo(0L),
                LOGSDB_INDEXING_FAILURE,
                equalTo(0L),
                LOGSDB_INDEXING_FAILURE_DUE_TO_VERSION_CONFLICT,
                equalTo(0L)
            )
        );
        collectThenAssertMetrics(telemetry, 4, zeroMatchers);
        String searchNode = internalCluster().startDataOnlyNode();
        indicesService = internalCluster().getInstance(IndicesService.class, searchNode);
        telemetry = internalCluster().getInstance(PluginsService.class, searchNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        ensureGreen("st*", "log*", "time*");
        // search and fetch
        String preference = "_only_local";
        client(searchNode).prepareSearch("standard*").setPreference(preference).setSize(100).get().decRef();
        var search1 = indicesService.stats(CommonStatsFlags.ALL, false).getSearch().getTotal();
        collectThenAssertMetrics(
            telemetry,
            1,
            Map.of(
                STANDARD_QUERY_COUNT,
                equalTo(numStandardIndices),
                STANDARD_QUERY_TIME,
                equalTo(search1.getQueryTimeInMillis()),
                STANDARD_FETCH_COUNT,
                equalTo(search1.getFetchCount()),
                STANDARD_FETCH_TIME,
                equalTo(search1.getFetchTimeInMillis()),

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

        client(searchNode).prepareSearch("time*").setPreference(preference).setSize(100).get().decRef();
        var search2 = indicesService.stats(CommonStatsFlags.ALL, false).getSearch().getTotal();
        collectThenAssertMetrics(
            telemetry,
            2,
            Map.of(
                STANDARD_QUERY_COUNT,
                equalTo(0L),
                STANDARD_QUERY_TIME,
                equalTo(0L),

                TIME_SERIES_QUERY_COUNT,
                equalTo(numTimeSeriesIndices),
                TIME_SERIES_QUERY_TIME,
                equalTo(search2.getQueryTimeInMillis() - search1.getQueryTimeInMillis()),
                TIME_SERIES_FETCH_COUNT,
                equalTo(search2.getFetchCount() - search1.getFetchCount()),
                TIME_SERIES_FETCH_TIME,
                equalTo(search2.getFetchTimeInMillis() - search1.getFetchTimeInMillis()),

                LOGSDB_QUERY_COUNT,
                equalTo(0L),
                LOGSDB_QUERY_TIME,
                equalTo(0L)
            )
        );
        client(searchNode).prepareSearch("logs*").setPreference(preference).setSize(100).get().decRef();
        var search3 = indicesService.stats(CommonStatsFlags.ALL, false).getSearch().getTotal();
        collectThenAssertMetrics(
            telemetry,
            3,
            Map.of(
                STANDARD_QUERY_COUNT,
                equalTo(0L),
                STANDARD_QUERY_TIME,
                equalTo(0L),

                TIME_SERIES_QUERY_COUNT,
                equalTo(0L),
                TIME_SERIES_QUERY_TIME,
                equalTo(0L),

                LOGSDB_QUERY_COUNT,
                equalTo(numLogsdbIndices),
                LOGSDB_QUERY_TIME,
                equalTo(search3.getQueryTimeInMillis() - search2.getQueryTimeInMillis()),
                LOGSDB_FETCH_COUNT,
                equalTo(search3.getFetchCount() - search2.getFetchCount()),
                LOGSDB_FETCH_TIME,
                equalTo(search3.getFetchTimeInMillis() - search2.getFetchTimeInMillis())
            )
        );
        // search failures
        expectThrows(
            Exception.class,
            () -> { client(searchNode).prepareSearch("logs*").setPreference(preference).setRuntimeMappings(parseMapping("""
                {
                    "fail_me": {
                        "type": "long",
                        "script": {"source": "<>", "lang": "failing_field"}
                    }
                }
                """)).setQuery(new RangeQueryBuilder("fail_me").gte(0)).setAllowPartialSearchResults(true).get(); }
        );
        collectThenAssertMetrics(
            telemetry,
            4,
            Map.of(
                STANDARD_QUERY_FAILURE,
                equalTo(0L),
                STANDARD_FETCH_FAILURE,
                equalTo(0L),
                TIME_SERIES_QUERY_FAILURE,
                equalTo(0L),
                TIME_SERIES_FETCH_FAILURE,
                equalTo(0L),
                LOGSDB_QUERY_FAILURE,
                equalTo(numLogsdbIndices),
                LOGSDB_FETCH_FAILURE,
                equalTo(0L)
            )
        );

        verifyStatsPerIndexMode(
            Map.of(IndexMode.STANDARD, numStandardDocs, IndexMode.LOGSDB, numLogsdbDocs, IndexMode.TIME_SERIES, numTimeSeriesDocs)
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
            refresh(indexName);
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
            int numFailures = between(0, 2);
            for (int d = 0; d < numFailures; d++) {
                expectThrows(Exception.class, () -> {
                    client().prepareIndex(indexName)
                        .setSource(
                            "@timestamp",
                            "malformed-timestamp",
                            "host.name",
                            randomFrom("prod", "qa"),
                            "cpu",
                            randomIntBetween(1, 100)
                        )
                        .get();
                });
            }
            totalDocs += numDocs;
            flush(indexName);
            refresh(indexName);
        }
        return totalDocs;
    }

    private void verifyStatsPerIndexMode(Map<IndexMode, Long> expectedDocs) {
        var nodes = clusterService().state().nodes().stream().toArray(DiscoveryNode[]::new);
        var request = new IndexModeStatsActionType.StatsRequest(nodes);
        var resp = client().execute(IndexModeStatsActionType.TYPE, request).actionGet();
        var stats = resp.stats();
        for (Map.Entry<IndexMode, Long> e : expectedDocs.entrySet()) {
            assertThat(stats.get(e.getKey()).numDocs(), equalTo(e.getValue()));
        }
    }

    private Map<String, Object> parseMapping(String mapping) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, mapping)) {
            return parser.map();
        }
    }
}

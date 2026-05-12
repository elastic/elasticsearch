/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestAttributesExtractor;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class SearchPhaseCoordinatorAPMMetricsTests extends ESSingleNodeTestCase {
    private static final String indexName = "test_coordinator_search_phase_metrics";
    private static final String secondIndexName = "test_coordinator_search_phase_metrics_2";
    private final int num_primaries = randomIntBetween(2, 7);

    private static final String CAN_MATCH_SEARCH_PHASE_METRIC = "es.search_response.took_durations.can_match.histogram";
    private static final String DFS_QUERY_SEARCH_PHASE_METRIC = "es.search_response.took_durations.dfs_query.histogram";
    private static final String DFS_SEARCH_PHASE_METRIC = "es.search_response.took_durations.dfs.histogram";
    private static final String FETCH_SEARCH_PHASE_METRIC = "es.search_response.took_durations.fetch.histogram";
    private static final String OPEN_PIT_SEARCH_PHASE_METRIC = "es.search_response.took_durations.open_pit.histogram";
    private static final String QUERY_SEARCH_PHASE_METRIC = "es.search_response.took_durations.query.histogram";
    private static final String TOOK_DURATION_TOTAL_HISTOGRAM = SearchResponseMetrics.TOOK_DURATION_TOTAL_HISTOGRAM_NAME;

    private static final String vectorIndexName = "test_vector_search_metrics";

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    private void setUpIndex() throws Exception {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource("body", "doc1", "@timestamp", "2024-11-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "doc2", "@timestamp", "2024-12-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("3").setSource("body", "doc3", "@timestamp", "2025-01-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("4").setSource("body", "doc4", "@timestamp", "2025-02-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("5").setSource("body", "doc5", "@timestamp", "2025-03-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("6").setSource("body", "doc6", "@timestamp", "2025-04-01").setRefreshPolicy(IMMEDIATE).get();

        createIndex(
            secondIndexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(secondIndexName);

        prepareIndex(secondIndexName).setId("7").setSource("body", "doc1", "@timestamp", "2025-11-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(secondIndexName).setId("8").setSource("body", "doc2", "@timestamp", "2025-12-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(secondIndexName).setId("9").setSource("body", "doc3", "@timestamp", "2026-01-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(secondIndexName).setId("10").setSource("body", "doc4", "@timestamp", "2026-02-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(secondIndexName).setId("11").setSource("body", "doc5", "@timestamp", "2026-03-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(secondIndexName).setId("12").setSource("body", "doc6", "@timestamp", "2026-04-01").setRefreshPolicy(IMMEDIATE).get();

        XContentBuilder mappings = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("my_vector")
            .field("type", "dense_vector")
            .field("dims", 3)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "flat")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex(
            vectorIndexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(),
            mappings
        );
        ensureGreen(vectorIndexName);
        prepareIndex(vectorIndexName).setId("v1")
            .setSource("my_vector", new float[] { 1.0f, 2.0f, 3.0f })
            .setRefreshPolicy(IMMEDIATE)
            .get();
    }

    @After
    private void afterTest() {
        resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class);
    }

    public void testSearchQueryThenFetch() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        assertMeasurements(List.of(QUERY_SEARCH_PHASE_METRIC, FETCH_SEARCH_PHASE_METRIC), 1);
        assertNotMeasured(
            List.of(CAN_MATCH_SEARCH_PHASE_METRIC, DFS_SEARCH_PHASE_METRIC, DFS_QUERY_SEARCH_PHASE_METRIC, OPEN_PIT_SEARCH_PHASE_METRIC)
        );
    }

    public void testDfsSearch() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        assertMeasurements(List.of(DFS_SEARCH_PHASE_METRIC, DFS_QUERY_SEARCH_PHASE_METRIC, FETCH_SEARCH_PHASE_METRIC), 1);
        assertNotMeasured(List.of(CAN_MATCH_SEARCH_PHASE_METRIC, QUERY_SEARCH_PHASE_METRIC, OPEN_PIT_SEARCH_PHASE_METRIC));
    }

    public void testPointInTime() {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indexName).keepAlive(TimeValue.timeValueMinutes(10));
        request.indexFilter(simpleQueryStringQuery("doc1"));
        OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        BytesReference pointInTimeId = response.getPointInTimeId();

        try {
            assertSearchHitsWithoutFailures(
                client().prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pointInTimeId))
                    .setSize(1)
                    .setQuery(simpleQueryStringQuery("doc1")),
                "1"
            );
            assertMeasurements(List.of(OPEN_PIT_SEARCH_PHASE_METRIC, QUERY_SEARCH_PHASE_METRIC, FETCH_SEARCH_PHASE_METRIC), 1);
            assertNotMeasured(List.of(DFS_SEARCH_PHASE_METRIC, DFS_QUERY_SEARCH_PHASE_METRIC));
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pointInTimeId)).actionGet();
        }
    }

    public void testPointInTimeWithPreFiltering() {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indexName, secondIndexName).keepAlive(TimeValue.timeValueMinutes(10));
        request.indexFilter(new RangeQueryBuilder("@timestamp").gte("2025-07-01"));
        OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        BytesReference pointInTimeId = response.getPointInTimeId();

        try {
            assertSearchHitsWithoutFailures(
                client().prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pointInTimeId))
                    .setSize(1)
                    .setPreFilterShardSize(1)
                    .setQuery(simpleQueryStringQuery("doc3")),
                "9"
            );
            assertMeasurements(List.of(OPEN_PIT_SEARCH_PHASE_METRIC, QUERY_SEARCH_PHASE_METRIC, FETCH_SEARCH_PHASE_METRIC), 1);
            assertMeasurements(
                List.of(CAN_MATCH_SEARCH_PHASE_METRIC),
                2 // one during open PIT, one during can-match phase of search
            );
            assertNotMeasured(List.of(DFS_SEARCH_PHASE_METRIC, DFS_QUERY_SEARCH_PHASE_METRIC));
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pointInTimeId)).actionGet();
        }
    }

    public void testCanMatchSearch() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreFilterShardSize(1)
                .setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );

        assertMeasurements(List.of(CAN_MATCH_SEARCH_PHASE_METRIC, FETCH_SEARCH_PHASE_METRIC, QUERY_SEARCH_PHASE_METRIC), 1);
        assertNotMeasured(List.of(DFS_SEARCH_PHASE_METRIC, DFS_QUERY_SEARCH_PHASE_METRIC, OPEN_PIT_SEARCH_PHASE_METRIC));
    }

    public void testSearchShards() {
        var request = new SearchShardsRequest(
            new String[] { indexName },
            SearchRequest.DEFAULT_INDICES_OPTIONS,
            simpleQueryStringQuery("doc1"),
            null,
            null,
            randomBoolean(),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
        var resp = client().execute(TransportSearchShardsAction.TYPE, request).actionGet();
        assertThat(resp.getGroups(), hasSize(num_primaries));
        assertMeasurements(List.of(CAN_MATCH_SEARCH_PHASE_METRIC), 1);
    }

    public void testVectorSearchSetsVectorIndexTypeAttribute() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(vectorIndexName)
                .setKnnSearch(
                    List.of(
                        new KnnSearchBuilder(
                            "my_vector",
                            org.elasticsearch.search.vectors.VectorData.fromFloats(new float[] { 1.0f, 2.0f, 3.0f }),
                            1,
                            10,
                            null,
                            null,
                            null
                        )
                    )
                ),
            "v1"
        );
        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        Map<String, Object> attributes = measurements.getFirst().attributes();
        assertEquals("flat", attributes.get(SearchRequestAttributesExtractor.VECTOR_INDEX_TYPE_ATTRIBUTE));
        assertEquals(true, attributes.get(SearchRequestAttributesExtractor.KNN_ATTRIBUTE));
    }

    public void testNonVectorSearchDoesNotSetVectorIndexTypeAttribute() {
        assertSearchHitsWithoutFailures(client().prepareSearch(indexName).setQuery(simpleQueryStringQuery("doc1")), "1");
        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM);
        assertThat(measurements, hasSize(1));
        assertFalse(measurements.getFirst().attributes().containsKey(SearchRequestAttributesExtractor.VECTOR_INDEX_TYPE_ATTRIBUTE));
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    private void assertNotMeasured(Collection<String> metricNames) {
        for (var metricName : metricNames) {
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(metricName);
            assertThat(metricName, measurements, hasSize(0));
        }
    }

    private void assertMeasurements(Collection<String> metricNames, int numberOfMeasurements) {
        for (var metricName : metricNames) {
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(metricName);
            assertThat(metricName, measurements, hasSize(numberOfMeasurements));
            assertThat(metricName, measurements.getFirst().getLong(), greaterThanOrEqualTo(0L));
            for (var measurement : measurements) {
                var attributes = measurement.attributes();
                assertThat(metricName, attributes.entrySet(), hasSize(greaterThanOrEqualTo(3)));
                assertEquals(metricName, "user", attributes.get("target"));
                assertEquals(metricName, "hits_only", attributes.get("query_type"));
                assertEquals(metricName, "_score", attributes.get("sort"));
            }
        }
    }
}

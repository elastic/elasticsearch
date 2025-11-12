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
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

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

        createIndex(
            secondIndexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(secondIndexName);

        prepareIndex(secondIndexName).setId("4").setSource("body", "doc1", "@timestamp", "2025-11-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(secondIndexName).setId("5").setSource("body", "doc2", "@timestamp", "2025-12-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(secondIndexName).setId("6").setSource("body", "doc3", "@timestamp", "2026-01-01").setRefreshPolicy(IMMEDIATE).get();
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
                "6"
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

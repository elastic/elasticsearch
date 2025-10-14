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
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
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
    private final int num_primaries = randomIntBetween(2, 7);

    // es.search_response.coordinator_phases.%s.duration.histogram
    // es.search_response.took_durations.
    private static final String QUERY_SEARCH_PHASE_METRIC = "es.search_response.took_durations.query.histogram";
    private static final String DFS_SEARCH_PHASE_METRIC = "es.search_response.took_durations.dfs.histogram";
    private static final String OPEN_PIT_SEARCH_PHASE_METRIC = "es.search_response.took_durations.open_pit.histogram";

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

        prepareIndex(indexName).setId("1").setSource("body", "doc1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "doc2").setRefreshPolicy(IMMEDIATE).get();
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
        assertMeasurements(List.of(QUERY_SEARCH_PHASE_METRIC));
    }

    public void testDfsSearch() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        assertMeasurements(List.of(DFS_SEARCH_PHASE_METRIC));
    }

    public void testPointInTime() {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indexName).keepAlive(TimeValue.timeValueMinutes(10));
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
            assertMeasurements(List.of(OPEN_PIT_SEARCH_PHASE_METRIC, QUERY_SEARCH_PHASE_METRIC));
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pointInTimeId)).actionGet();
        }
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    private void assertMeasurements(Collection<String> metricNames) {
        for (var metricName : metricNames) {
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(metricName);
            assertThat(measurements, hasSize(1));
            assertThat(measurements.getFirst().getLong(), greaterThanOrEqualTo(0L));
        }
    }
}

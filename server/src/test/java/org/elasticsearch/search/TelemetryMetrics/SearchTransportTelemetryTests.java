/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.search.SearchTransportAPMMetrics.DFS_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FETCH_ID_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FETCH_ID_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FREE_CONTEXT_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_ID_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;

public class SearchTransportTelemetryTests extends ESSingleNodeTestCase {

    private static final String indexName = "test_search_metrics2";
    private final int num_primaries = randomIntBetween(2, 7);

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

    public void testSearchTransportMetricsDfsQueryThenFetch() throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        assertEquals(num_primaries, getNumberOfMeasurements(DFS_ACTION_METRIC));
        assertEquals(num_primaries, getNumberOfMeasurements(QUERY_ID_ACTION_METRIC));
        assertNotEquals(0, getNumberOfMeasurements(FETCH_ID_ACTION_METRIC));
        resetMeter();
    }

    public void testSearchTransportMetricsQueryThenFetch() throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );
        assertEquals(num_primaries, getNumberOfMeasurements(QUERY_ACTION_METRIC));
        assertNotEquals(0, getNumberOfMeasurements(FETCH_ID_ACTION_METRIC));
        resetMeter();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/103810")
    public void testSearchTransportMetricsScroll() throws InterruptedException {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(indexName)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(1)
                .setQuery(simpleQueryStringQuery("doc1 doc2")),
            2,
            (respNum, response) -> {
                if (respNum == 1) {
                    assertEquals(num_primaries, getNumberOfMeasurements(DFS_ACTION_METRIC));
                    assertEquals(num_primaries, getNumberOfMeasurements(QUERY_ID_ACTION_METRIC));
                    assertNotEquals(0, getNumberOfMeasurements(FETCH_ID_ACTION_METRIC));
                } else if (respNum == 2) {
                    assertEquals(num_primaries, getNumberOfMeasurements(QUERY_SCROLL_ACTION_METRIC));
                    assertNotEquals(0, getNumberOfMeasurements(FETCH_ID_SCROLL_ACTION_METRIC));
                }
                resetMeter();
            }
        );

        assertEquals(num_primaries, getNumberOfMeasurements(FREE_CONTEXT_SCROLL_ACTION_METRIC));
        resetMeter();
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    private long getNumberOfMeasurements(String attributeValue) {
        final List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(
            org.elasticsearch.action.search.SearchTransportAPMMetrics.SEARCH_ACTION_LATENCY_BASE_METRIC
        );
        return measurements.stream()
            .filter(
                m -> m.attributes().get(org.elasticsearch.action.search.SearchTransportAPMMetrics.ACTION_ATTRIBUTE_NAME) == attributeValue
            )
            .count();
    }
}

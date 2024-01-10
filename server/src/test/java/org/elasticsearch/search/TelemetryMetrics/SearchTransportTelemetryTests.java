/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.search.SearchTransportAPMMetrics.DFS_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FETCH_ID_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FETCH_ID_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FREE_CONTEXT_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_ID_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_SCROLL_ACTION_METRIC;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SearchTransportTelemetryTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestTelemetryPlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 7;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/103781")
    public void testSearchTransportMetricsDfsQueryThenFetch() throws InterruptedException {
        var indexName = "test1";
        createIndex(indexName);
        indexRandom(true, false, prepareIndex(indexName).setId("1").setSource("body", "foo"));

        assertSearchHitsWithoutFailures(
            prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("foo")),
            "1"
        );
        assertEquals(getNumShards(indexName).numPrimaries, getNumberOfMeasurements(DFS_ACTION_METRIC));
        assertEquals(getNumShards(indexName).numPrimaries, getNumberOfMeasurements(QUERY_ID_ACTION_METRIC));
        assertNotEquals(0, getNumberOfMeasurements(FETCH_ID_ACTION_METRIC));
        resetMeter();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/104184")
    public void testSearchTransportMetricsQueryThenFetch() throws InterruptedException {
        var indexName = "test2";
        createIndex(indexName);
        indexRandom(true, false, prepareIndex(indexName).setId("1").setSource("body", "foo"));

        assertSearchHitsWithoutFailures(
            prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("foo")),
            "1"
        );
        assertEquals(getNumShards(indexName).numPrimaries, getNumberOfMeasurements(QUERY_ACTION_METRIC));
        assertNotEquals(0, getNumberOfMeasurements(FETCH_ID_ACTION_METRIC));
        resetMeter();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/103810")
    public void testSearchTransportMetricsScroll() throws InterruptedException {
        var indexName = "test3";
        createIndex(indexName);
        indexRandom(
            true,
            false,
            prepareIndex(indexName).setId("1").setSource("body", "foo"),
            prepareIndex(indexName).setId("2").setSource("body", "foo")
        ); // getNumShards(indexName).numPrimaries

        assertScrollResponsesAndHitCount(
            TimeValue.timeValueSeconds(60),
            prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSize(1).setQuery(simpleQueryStringQuery("foo")),
            2,
            (respNum, response) -> {
                if (respNum == 1) {
                    assertEquals(getNumShards(indexName).numPrimaries, getNumberOfMeasurements(DFS_ACTION_METRIC));
                    assertEquals(getNumShards(indexName).numPrimaries, getNumberOfMeasurements(QUERY_ID_ACTION_METRIC));
                    assertNotEquals(0, getNumberOfMeasurements(FETCH_ID_ACTION_METRIC));
                    resetMeter();
                } else if (respNum == 2) {
                    assertEquals(getNumShards(indexName).numPrimaries, getNumberOfMeasurements(QUERY_SCROLL_ACTION_METRIC));
                    assertNotEquals(0, getNumberOfMeasurements(FETCH_ID_SCROLL_ACTION_METRIC));
                } else {
                    resetMeter();
                }
            }
        );

        assertEquals(getNumShards(indexName).numPrimaries, getNumberOfMeasurements(FREE_CONTEXT_SCROLL_ACTION_METRIC));
        resetMeter();
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return internalCluster().getDataNodeInstance(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
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

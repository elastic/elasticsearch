/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.retriever.RescorerRetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.TOOK_DURATION_TOTAL_HISTOGRAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class SearchTookTimeTelemetryTests extends ESSingleNodeTestCase {
    private static final String indexName = "test_search_metrics2";

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    public void setUpIndex() {
        var num_primaries = randomIntBetween(1, 4);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource("body", "foo").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "foo").setRefreshPolicy(IMMEDIATE).get();
    }

    @After
    public void afterTest() {
        resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class);
    }

    public void testSimpleQuery() {
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(simpleQueryStringQuery("foo")).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        assertEquals(searchResponse.getTook().millis(), measurements.getFirst().getLong());
    }

    public void testCompoundRetriever() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.retriever(
            new RescorerRetrieverBuilder(
                new StandardRetrieverBuilder(new MatchAllQueryBuilder()),
                List.of(new QueryRescorerBuilder(new MatchAllQueryBuilder()))
            )
        );
        SearchResponse searchResponse = client().prepareSearch(indexName).setSource(searchSourceBuilder).get();
        try {
            assertNoFailures(searchResponse);
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            searchResponse.decRef();
        }

        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        // compound retriever does its own search as an async action, whose took time is recorded separately
        assertEquals(2, measurements.size());
        assertThat(measurements.getFirst().getLong(), Matchers.lessThan(searchResponse.getTook().millis()));
        assertEquals(searchResponse.getTook().millis(), measurements.getLast().getLong());
    }

    public void testMultiSearch() {
        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();
        int numSearchRequests = randomIntBetween(3, 10);
        for (int i = 0; i < numSearchRequests; i++) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().query(simpleQueryStringQuery("foo")));
            multiSearchRequestBuilder.add(searchRequest);
        }
        List<Long> tookTimes;
        MultiSearchResponse multiSearchResponse = null;
        try {
            multiSearchResponse = multiSearchRequestBuilder.get();
            tookTimes = Arrays.stream(multiSearchResponse.getResponses())
                .map(item -> item.getResponse().getTook().millis())
                .sorted()
                .toList();
        } finally {
            if (multiSearchResponse != null) {
                multiSearchResponse.decRef();
            }
        }
        List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(numSearchRequests, measurements.size());
        measurements.sort(Comparator.comparing(Measurement::getLong));

        int i = 0;
        for (Measurement measurement : measurements) {
            assertEquals(tookTimes.get(i++).longValue(), measurement.getLong());
        }
    }

    public void testScroll() {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(indexName).setSize(1).setQuery(simpleQueryStringQuery("foo")),
            2,
            (respNum, response) -> {
                if (respNum <= 2) {
                    List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(
                        TOOK_DURATION_TOTAL_HISTOGRAM_NAME
                    );
                    assertEquals(1, measurements.size());
                }
                resetMeter();
            }
        );
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }
}

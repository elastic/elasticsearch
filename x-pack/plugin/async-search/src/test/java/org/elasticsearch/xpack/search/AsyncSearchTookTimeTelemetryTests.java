/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.TOOK_DURATION_TOTAL_HISTOGRAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class AsyncSearchTookTimeTelemetryTests extends ESSingleNodeTestCase {

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

        prepareIndex(indexName).setId("1").setSource("body", "foo", "@timestamp", "2024-11-01").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "foo", "@timestamp", "2024-12-01").setRefreshPolicy(IMMEDIATE).get();
    }

    @After
    public void afterTest() {
        getTestTelemetryPlugin().resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class, AsyncSearch.class);
    }

    /**
     * Test an async search with a wait for completion timeout that is higher than the expected search execution time, that turns into
     * a sync request given its execution will always be completed directly as submit async search returns.
     */
    public void testAsyncForegroundQuery() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2024-10-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SubmitAsyncSearchRequest asyncSearchRequest = new SubmitAsyncSearchRequest(new SearchSourceBuilder().query(boolQueryBuilder));
        asyncSearchRequest.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(5));
        asyncSearchRequest.setKeepOnCompletion(true);
        AsyncSearchResponse asyncSearchResponse = client().execute(SubmitAsyncSearchAction.INSTANCE, asyncSearchRequest).actionGet();
        String id = asyncSearchResponse.getId();
        long tookInMillis;
        try {
            assertFalse(asyncSearchResponse.isPartial());
            assertFalse(asyncSearchResponse.isRunning());
            SearchResponse searchResponse = asyncSearchResponse.getSearchResponse();
            tookInMillis = searchResponse.getTookInMillis();
            assertSearchHits(searchResponse, "1", "2");
        } finally {
            asyncSearchResponse.decRef();
        }
        final List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        assertEquals(1, measurements.size());
        Measurement measurement = measurements.getFirst();
        assertEquals(tookInMillis, measurement.getLong());

        for (int i = 0; i < randomIntBetween(3, 10); i++) {
            AsyncSearchResponse asyncSearchResponse2 = client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id))
                .actionGet();
            try {
                assertSearchHits(asyncSearchResponse2.getSearchResponse(), "1", "2");
            } finally {
                asyncSearchResponse2.decRef();
            }
            assertEquals(1, measurements.size());
            assertEquals(tookInMillis, measurement.getLong());
            assertAttributes(measurement.attributes());
        }
    }

    /**
     * Test an async search with a very low wait for completion timeout, which then runs in the background and is not directly completed
     * when submit async search returns. Verify that the took time is recorded once, with the actual execution time of the search,
     * without any influence from get async search polling happening around the same async search request.
     */
    public void testAsyncBackgroundQuery() throws Exception {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2024-10-01"));
        boolQueryBuilder.must(simpleQueryStringQuery("foo"));
        SubmitAsyncSearchRequest asyncSearchRequest = new SubmitAsyncSearchRequest(new SearchSourceBuilder().query(boolQueryBuilder));
        asyncSearchRequest.setWaitForCompletionTimeout(TimeValue.ZERO);
        AsyncSearchResponse asyncSearchResponse = client().execute(SubmitAsyncSearchAction.INSTANCE, asyncSearchRequest).actionGet();
        String id;
        try {
            id = asyncSearchResponse.getId();
        } finally {
            asyncSearchResponse.decRef();
        }

        assertBusy(() -> {
            AsyncStatusResponse asyncStatusResponse = client().execute(GetAsyncStatusAction.INSTANCE, new GetAsyncStatusRequest(id))
                .actionGet();
            assertFalse(asyncStatusResponse.isRunning());
        });

        final List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(TOOK_DURATION_TOTAL_HISTOGRAM_NAME);
        for (int i = 0; i < randomIntBetween(3, 10); i++) {
            AsyncSearchResponse asyncSearchResponse2 = client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id))
                .actionGet();
            assertEquals(1, measurements.size());
            SearchResponse searchResponse = asyncSearchResponse2.getSearchResponse();
            try {
                assertSearchHits(searchResponse, "1", "2");
                Measurement measurement = measurements.getFirst();
                assertEquals(searchResponse.getTook().millis(), measurement.getLong());
                assertAttributes(measurement.attributes());
            } finally {
                asyncSearchResponse2.decRef();
            }
        }
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().getFirst();
    }

    private static void assertAttributes(Map<String, Object> attributes) {
        assertEquals(5, attributes.size());
        assertEquals("user", attributes.get("target"));
        assertEquals("hits_only", attributes.get("query_type"));
        assertEquals("_score", attributes.get("sort"));
        assertEquals("@timestamp", attributes.get("time_range_filter_field"));
        assertEquals("older_than_14_days", attributes.get("time_range_filter_from"));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Checks that the slow log will work properly for
 * {@link AbstractPercentilesAggregationBuilder} over the transport client.
 */
@ClusterScope(transportClientRatio = 1.0) // Force the transport client
public class PercentilesSlowLogIT extends ESIntegTestCase {
    private static final AtomicReference<SearchSourceBuilder> captured = new AtomicReference<>();

    @Before
    public void setup() {
        IndexResponse resp = client().prepareIndex("idx", "_doc", "id1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource("{ \"value\": 23 }", XContentType.JSON)
            .get();
        assertThat(resp.status(), equalTo(RestStatus.CREATED));
        captured.set(null);
    }

    public void testPercentilesWithSlowLog() {
        SearchRequestBuilder searchRequest = client().prepareSearch("idx")
            .addAggregation(AggregationBuilders.percentiles("percent").field("value"));

        assertThat(captured.get(), nullValue());

        SearchResponse response = searchRequest.get();

        // Make sure the response is sensible
        Percentiles percentiles = response.getAggregations().get("percent");
        for (Percentile entry : percentiles) {
            assertThat(entry.getValue(), equalTo(23.0));
        }

        // Now check if the slow log would have done a sensible thing
        assertThat(Strings.toString(captured.get()), equalTo(Strings.toString(searchRequest.request().source())));
    }

    public void testPercentileRanksWithSlowLog() {
        SearchRequestBuilder searchRequest = client().prepareSearch("idx")
            .addAggregation(AggregationBuilders.percentileRanks("percent", new double[] { 1.0, 2000.0 }).field("value"));

        assertThat(captured.get(), nullValue());

        SearchResponse response = searchRequest.get();

        // Make sure the response is sensible
        PercentileRanks percentiles = response.getAggregations().get("percent");
        assertThat(percentiles.percent(1.0), equalTo(0.0));
        assertThat(percentiles.percent(2000.0), equalTo(100.0));

        // Now check if the slow log would have done a sensible thing
        assertThat(Strings.toString(captured.get()), equalTo(Strings.toString(searchRequest.request().source())));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SearcherWrapperPlugin.class);
        return plugins;
    }

    public static class SearcherWrapperPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                // Let's capture the source that the slow log will get.
                @Override
                public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
                    captured.set(searchContext.request().source());
                }
            });
        }
    }
}

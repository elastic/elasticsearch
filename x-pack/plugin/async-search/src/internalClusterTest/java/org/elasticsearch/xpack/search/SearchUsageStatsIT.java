/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.admin.cluster.stats.SearchUsageStats;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SearchUsageStatsIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AsyncSearch.class);
    }

    public void testSearchUsageStats() throws IOException {
        {
            SearchUsageStats stats = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSearchUsageStats();
            assertEquals(0, stats.getTotalSearchCount());
            assertEquals(0, stats.getQueryUsage().size());
            assertEquals(0, stats.getSectionsUsage().size());
        }

        {
            Request request = new Request("POST", "/_async_search");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchQuery("field", "value"))
                .aggregation(new TermsAggregationBuilder("name").field("field"));
            request.setJsonEntity(Strings.toString(searchSourceBuilder));
            getRestClient().performRequest(request);
        }
        {
            Request request = new Request("POST", "/_async_search");
            // error at parsing: request not counted
            request.setJsonEntity("{\"unknown]\":10}");
            expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        }
        {
            // non existent index: request counted
            Request request = new Request("POST", "/unknown/_async_search");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"));
            request.setJsonEntity(Strings.toString(searchSourceBuilder));
            ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertEquals(404, responseException.getResponse().getStatusLine().getStatusCode());
        }
        SearchUsageStats stats = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSearchUsageStats();
        assertEquals(2, stats.getTotalSearchCount());
        assertEquals(2, stats.getQueryUsage().size());
        assertEquals(1, stats.getQueryUsage().get("match").longValue());
        assertEquals(1, stats.getQueryUsage().get("term").longValue());
        assertEquals(2, stats.getSectionsUsage().size());
        assertEquals(2, stats.getSectionsUsage().get("query").longValue());
        assertEquals(1, stats.getSectionsUsage().get("aggs").longValue());
    }
}

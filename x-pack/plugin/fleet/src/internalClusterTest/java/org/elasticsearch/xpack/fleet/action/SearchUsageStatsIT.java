/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.admin.cluster.stats.SearchUsageStats;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.fleet.Fleet;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SearchUsageStatsIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.of(Fleet.class, LocalStateCompositeXPackPlugin.class, IndexLifecycle.class).collect(Collectors.toList());
    }

    public void testSearchUsageStats() throws IOException {
        {
            SearchUsageStats stats = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSearchUsageStats();
            assertEquals(0, stats.getTotalSearchCount());
            assertEquals(0, stats.getQueryUsage().size());
            assertEquals(0, stats.getSectionsUsage().size());
        }

        indicesAdmin().prepareCreate("index").get();
        ensureGreen("index");

        // doesn't get counted because it doesn't specify a request body
        getRestClient().performRequest(new Request("GET", "/index/_fleet/_fleet_search"));
        {
            Request request = new Request("GET", "/index/_fleet/_fleet_search");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchQuery("field", "value"));
            request.setJsonEntity(Strings.toString(searchSourceBuilder));
            getRestClient().performRequest(request);
        }
        {
            Request request = new Request("GET", "/index/_fleet/_fleet_search");
            // error at parsing: request not counted
            request.setJsonEntity("{\"unknown]\":10}");
            expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        }
        {
            // non existent index: request counted
            Request request = new Request("GET", "/unknown/_fleet/_fleet_search");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"));
            request.setJsonEntity(Strings.toString(searchSourceBuilder));
            ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertEquals(404, responseException.getResponse().getStatusLine().getStatusCode());
        }
        {
            Request request = new Request("GET", "/index/_fleet/_fleet_search");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("field").from(10))
            );
            request.setJsonEntity(Strings.toString(searchSourceBuilder));
            getRestClient().performRequest(request);
        }
        {
            Request request = new Request("POST", "/index/_fleet/_fleet_msearch");
            SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder().aggregation(
                new TermsAggregationBuilder("name").field("field")
            );
            SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"));
            request.setJsonEntity(
                "{}\n" + Strings.toString(searchSourceBuilder1) + "\n" + "{}\n" + Strings.toString(searchSourceBuilder2) + "\n"
            );
            getRestClient().performRequest(request);
        }

        SearchUsageStats stats = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSearchUsageStats();
        assertEquals(5, stats.getTotalSearchCount());
        assertEquals(4, stats.getQueryUsage().size());
        assertEquals(1, stats.getQueryUsage().get("match").longValue());
        assertEquals(2, stats.getQueryUsage().get("term").longValue());
        assertEquals(1, stats.getQueryUsage().get("range").longValue());
        assertEquals(1, stats.getQueryUsage().get("bool").longValue());
        assertEquals(2, stats.getSectionsUsage().size());
        assertEquals(4, stats.getSectionsUsage().get("query").longValue());
        assertEquals(1, stats.getSectionsUsage().get("aggs").longValue());
    }
}

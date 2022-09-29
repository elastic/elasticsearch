/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.relevancesearch.RelevanceSearchPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;

public class RelevanceMatchQueryIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(RelevanceSearchPlugin.class);
    }

    public void testRelevanceMatchQuery() {
        client().admin().indices().prepareCreate("index").get();
        client().prepareIndex("index")
            .setId("1")
            .setSource(Map.of("textField", "text example", "intField", 12, "doubleField", 13.45, "anotherTextField", "should match"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet();
        client().prepareIndex("index")
            .setId("2")
            .setSource(
                Map.of("textField", "other document", "intField", 12, "doubleField", 13.45, "anotherTextField", "should not be found")
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet();

        RelevanceMatchQueryBuilder relevanceMatchQueryBuilder = new RelevanceMatchQueryBuilder();
        relevanceMatchQueryBuilder.setQuery("text match");
        SearchResponse response = client().prepareSearch("index").setQuery(relevanceMatchQueryBuilder).get();

        assertHitCount(response, 1);
        assertSearchHits(response, "1");
    }

    public void testRelevanceMatchQueryWithNoTextFields() {
        client().admin().indices().prepareCreate("index").get();
        client().prepareIndex("index")
            .setId("1")
            .setSource(Map.of("intField", 12, "doubleField", 13.45))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet();

        RelevanceMatchQueryBuilder relevanceMatchQueryBuilder = new RelevanceMatchQueryBuilder();
        relevanceMatchQueryBuilder.setQuery("text match");
        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch("index").setQuery(relevanceMatchQueryBuilder);

        final String expectedMsg = "[relevance_match] query cannot find text fields in the index";
        assertFailures(searchRequestBuilder, RestStatus.BAD_REQUEST, containsString(expectedMsg));
    }
}

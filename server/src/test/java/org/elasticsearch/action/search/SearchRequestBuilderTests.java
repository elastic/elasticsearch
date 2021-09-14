/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.equalTo;

public class SearchRequestBuilderTests extends ESTestCase {

    private SearchRequestBuilder makeBuilder() {
        ElasticsearchClient client = Mockito.mock(ElasticsearchClient.class);
        return new SearchRequestBuilder(client, SearchAction.INSTANCE);
    }

    public void testEmptySourceToString() {
        SearchRequestBuilder searchRequestBuilder = makeBuilder();
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().toString()));
    }

    public void testQueryBuilderQueryToString() {
        SearchRequestBuilder searchRequestBuilder = makeBuilder();
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()));
    }

    public void testSearchSourceBuilderToString() {
        SearchRequestBuilder searchRequestBuilder = makeBuilder();
        searchRequestBuilder.setSource(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")));
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder()
            .query(QueryBuilders.termQuery("field", "value")).toString()));
    }

    public void testThatToStringDoesntWipeRequestSource() {
        SearchRequestBuilder searchRequestBuilder = makeBuilder()
            .setSource(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")));
        String preToString = searchRequestBuilder.request().toString();
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder()
            .query(QueryBuilders.termQuery("field", "value")).toString()));
        String postToString = searchRequestBuilder.request().toString();
        assertThat(preToString, equalTo(postToString));
    }
}

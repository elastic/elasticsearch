/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;

public class AliasTests extends ESSingleNodeTestCase {

    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = client();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testIndexAliasing() throws IOException {
        String indexName = "test-index";
        String aliasName = "test-alias";

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
        );
        client.admin().indices().create(createIndexRequest).actionGet();

        IndicesAliasesRequest.AliasActions aliasAction = IndicesAliasesRequest.AliasActions.add()
            .index(indexName)
            .alias(aliasName);
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(aliasAction);
        client.admin().indices().aliases(indicesAliasesRequest).actionGet();

        IndexRequest indexRequest = new IndexRequest(aliasName).id("1")
            .source("field", "value");
        client.index(indexRequest).actionGet();

        client.admin().indices().prepareRefresh(indexName).get();

        SearchRequest searchRequest = new SearchRequest(aliasName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest).actionGet();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(Objects.requireNonNull(searchResponse.getHits().getAt(0).getSourceAsMap()).get("field"), equalTo("value"));
    }
}

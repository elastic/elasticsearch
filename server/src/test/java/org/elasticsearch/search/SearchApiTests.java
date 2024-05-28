/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit test(s) for SearchService
 */
public class SearchApiTests extends ESIntegTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("test");
        assertAcked(client().admin().indices().create(createIndexRequest).actionGet());

        IndexRequest indexRequest = new IndexRequest("test").id("1").source("{\"field\": \"value\"}", XContentType.JSON);
        IndexResponse indexResponse = (IndexResponse) client().index(indexRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE));
        assertEquals("1", indexResponse.getId());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("test");
        assertAcked(client().admin().indices().delete(deleteIndexRequest).actionGet());
    }

    @Test
    public void testSearchApiBasicFunctionality() throws Exception {
        SearchRequest searchRequest = new SearchRequest("test");
        searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        SearchResponse searchResponse = (SearchResponse) client().search(searchRequest);

        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertThat(searchResponse.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("value"));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 3)
public class KnnSearchIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test_knn_index";
    private static final String VECTOR_FIELD = "vector";
    private static final int DIMENSION = 2;

    private XContentBuilder createKnnMapping() throws Exception {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject();
    }

    public void testKnnSearchWithScroll() throws Exception {
        Client client = client();

        client.admin().indices().prepareCreate(INDEX_NAME).setMapping(createKnnMapping()).get();

        int count = randomIntBetween(10, 20);
        for (int i = 0; i < count; i++) {
            client.prepareIndex(INDEX_NAME).setSource(XContentType.JSON, VECTOR_FIELD, new float[] { i, i }).get();
        }

        refresh(INDEX_NAME);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        int k = count / 2;
        sourceBuilder.knnSearch(List.of(new KnnSearchBuilder(VECTOR_FIELD, new float[] { 0, 0 }, k, k, null, null)));

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder).scroll(TimeValue.timeValueMinutes(1));

        SearchResponse firstResponse = client.search(searchRequest).actionGet();
        assertThat(firstResponse.getScrollId(), notNullValue());
        assertThat(firstResponse.getHits().getHits().length, equalTo(k));

        while (true) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(firstResponse.getScrollId());
            scrollRequest.scroll(TimeValue.timeValueMinutes(1));
            SearchResponse scrollResponse = client.searchScroll(scrollRequest).actionGet();
            if (scrollResponse.getHits().getHits().length == 0) {
                break;
            }
            assertThat(scrollResponse.getHits().getHits().length, equalTo(1));
            assertThat(scrollResponse.getScrollId(), notNullValue());
            assertThat(scrollResponse.getHits().getTotalHits().value(), equalTo((long) k));
        }

        client.prepareClearScroll().addScrollId(firstResponse.getScrollId()).get();
    }
}

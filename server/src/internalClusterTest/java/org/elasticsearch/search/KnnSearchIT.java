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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.List;

import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class KnnSearchIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test_knn_index";
    private static final String VECTOR_FIELD = "vector";

    private XContentBuilder createKnnMapping() throws Exception {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", 2)
            .field("index", true)
            .field("similarity", "l2_norm")
            .startObject("index_options")
            .field("type", "hnsw")
            .endObject()
            .endObject()
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
    }

    public void testKnnSearchWithScroll() throws Exception {
        final int numShards = randomIntBetween(1, 3);
        Client client = client();
        client.admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards))
            .setMapping(createKnnMapping())
            .get();

        final int count = 100;
        for (int i = 0; i < count; i++) {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field(VECTOR_FIELD, new float[] { i * 0.1f, i * 0.1f })
                .field("category", i >= 90 ? "last_ten" : null)
                .endObject();
            client.prepareIndex(INDEX_NAME).setSource(source).get();
        }
        refresh(INDEX_NAME);

        final int k = randomIntBetween(11, 15);
        // test top level knn search
        {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.knnSearch(List.of(new KnnSearchBuilder(VECTOR_FIELD, new float[] { 0, 0 }, k, 100, null, null)));
            executeScrollSearch(client, sourceBuilder, k);
        }
        // test top level knn search + another query
        {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.knnSearch(List.of(new KnnSearchBuilder(VECTOR_FIELD, new float[] { 0, 0 }, k, 100, null, null)));
            sourceBuilder.query(QueryBuilders.existsQuery("category").boost(10));
            executeScrollSearch(client, sourceBuilder, k + 10);
        }

        // test knn query
        {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 0, 0 }, k, 100, null, null));
            executeScrollSearch(client, sourceBuilder, k * numShards);
        }
        // test knn query + another query
        {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(
                QueryBuilders.boolQuery()
                    .should(new KnnVectorQueryBuilder(VECTOR_FIELD, new float[] { 0, 0 }, k, 100, null, null))
                    .should(QueryBuilders.existsQuery("category").boost(10))
            );
            executeScrollSearch(client, sourceBuilder, k * numShards + 10);
        }

    }

    private static void executeScrollSearch(Client client, SearchSourceBuilder sourceBuilder, int expectedNumHits) {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder).scroll(TimeValue.timeValueMinutes(1));

        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        int hitsCollected = 0;
        float prevScore = Float.POSITIVE_INFINITY;
        try {
            do {
                assertThat(searchResponse.getScrollId(), notNullValue());
                assertEquals(expectedNumHits, searchResponse.getHits().getTotalHits().value);
                // assert correct order of returned hits
                for (var searchHit : searchResponse.getHits()) {
                    assert (searchHit.getScore() <= prevScore);
                    prevScore = searchHit.getScore();
                    hitsCollected += 1;
                }
                searchResponse.decRef();
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
            } while (searchResponse.getHits().getHits().length > 0);
        } finally {
            assertEquals(expectedNumHits, hitsCollected);
            clearScroll(searchResponse.getScrollId());
            searchResponse.decRef();
        }
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, maxNumDataNodes = 3)
public class RRFRankIT extends ESIntegTestCase {

    @Override
    protected int minimumNumberOfShards() {
        return 1;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 7;
    }

    @Override
    protected int minimumNumberOfReplicas() {
        return 0;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @Before
    public void setupIndicesAndDocs() throws IOException {
        // Setup an index with a very small number of documents to
        // test sizing limits and issues with empty data

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 1)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();

        assertAcked(prepareCreate("tiny_index").setMapping(builder));
        ensureGreen("tiny_index");

        client().prepareIndex("tiny_index").setSource("vector", new float[] { 0.0f }, "text", "term term").get();
        client().prepareIndex("tiny_index").setSource("vector", new float[] { 1.0f }, "text", "other").get();
        client().prepareIndex("tiny_index").setSource("vector", new float[] { 2.0f }, "text", "term").get();

        client().admin().indices().prepareRefresh("tiny_index").get();

        // Setup an index with non-random data so we can
        // do direct tests against expected results

        builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector_asc")
            .field("type", "dense_vector")
            .field("dims", 1)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("vector_desc")
            .field("type", "dense_vector")
            .field("dims", 1)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("int")
            .field("type", "integer")
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();

        assertAcked(prepareCreate("nrd_index").setMapping(builder));
        ensureGreen(TimeValue.timeValueSeconds(120), "nrd_index");

        for (int doc = 0; doc < 10000; ++doc) {
            client().prepareIndex("nrd_index").setSource("vector_asc", new float[] { doc }, "int", doc % 3, "text", "term " + doc).get();
        }

        client().admin().indices().prepareRefresh("nrd_index").get();
    }

    public void testRRFRankSmallerThanSize() {
        float[] queryVector = { 0.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 3, 3);
        SearchResponse response = client().prepareSearch("tiny_index")
            .setRankContextBuilder(new RRFRankContextBuilder().windowSize(100).rankConstant(1))
            .setKnnSearch(List.of(knnSearch))
            .setQuery(QueryBuilders.termQuery("text", "term"))
            .addFetchField("vector")
            .addFetchField("text")
            .get();

        // we cast to Number when looking at values in vector fields because different xContentTypes may return Float or Double

        assertEquals(3, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(0.0, ((Number) hit.field("vector").getValue()).doubleValue(), 0.0);
        assertEquals("term term", hit.field("text").getValue());

        hit = response.getHits().getAt(1);
        assertEquals(2, hit.getRank());
        assertEquals(2.0, ((Number) hit.field("vector").getValue()).doubleValue(), 0.0);
        assertEquals("term", hit.field("text").getValue());

        hit = response.getHits().getAt(2);
        assertEquals(3, hit.getRank());
        assertEquals(1.0, ((Number) hit.field("vector").getValue()).doubleValue(), 0.0);
        assertEquals("other", hit.field("text").getValue());
    }

    public void testSimpleRRFRank() {
        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 100, 1000);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankContextBuilder(new RRFRankContextBuilder().windowSize(100).rankConstant(1))
            .setTrackTotalHits(true)
            .setKnnSearch(List.of(knnSearch))
            .setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("text", "500").boost(10.0f))
                    .should(QueryBuilders.termQuery("text", "499").boost(9.0f))
                    .should(QueryBuilders.termQuery("text", "498").boost(8.0f))
                    .should(QueryBuilders.termQuery("text", "497").boost(7.0f))
                    .should(QueryBuilders.termQuery("text", "496").boost(6.0f))
                    .should(QueryBuilders.termQuery("text", "495").boost(5.0f))
                    .should(QueryBuilders.termQuery("text", "494").boost(4.0f))
                    .should(QueryBuilders.termQuery("text", "493").boost(3.0f))
                    .should(QueryBuilders.termQuery("text", "492").boost(2.0f))
                    .should(QueryBuilders.termQuery("text", "491"))
            )
            .addFetchField("vector_asc")
            .addFetchField("text")
            .setSize(10)
            .get();

        assertEquals(10, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(493.0, 494.0, 495.0, 496.0, 497.0, 498.0, 499.0, 500.0, 501.0, 502.0), vectors);
    }
}

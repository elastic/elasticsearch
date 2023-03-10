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
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(maxNumDataNodes = 3)
@ESIntegTestCase.SuiteScopeTestCase
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

    @Override
    public void setupSuiteScopeCluster() throws Exception {
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
            client().prepareIndex("nrd_index")
                .setSource(
                    "vector_asc",
                    new float[] { doc },
                    "vector_desc",
                    new float[] { 10000 - doc },
                    "int",
                    doc % 3,
                    "text",
                    "term " + doc
                )
                .get();
        }

        client().admin().indices().prepareRefresh("nrd_index").get();
    }

    public void testTotalDocsSmallerThanSize() {
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

    public void testBM25AndKnn() {
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
            .setSize(11)
            .get();

        assertEquals(11, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(492.0, 493.0, 494.0, 495.0, 496.0, 497.0, 498.0, 499.0, 500.0, 501.0, 502.0), vectors);
    }

    public void testMultipleOnlyKnn() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 9500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 50, 100);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 50, 100);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankContextBuilder(new RRFRankContextBuilder().windowSize(50).rankConstant(1))
            .setTrackTotalHits(true)
            .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
            .addFetchField("vector_asc")
            .addFetchField("text")
            .setSize(19)
            .get();

        assertEquals(19, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(491.0, 492.0, 493.0, 494.0, 495.0, 496.0, 497.0, 498.0, 499.0, 500.0,
            501.0, 502.0, 503.0, 504.0, 505.0, 506.0, 507.0, 508.0, 509.0), vectors);
    }

    public void testBM25AndMultipleKnn() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 9501.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 50, 100);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 50, 100);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankContextBuilder(new RRFRankContextBuilder().windowSize(50).rankConstant(1))
            .setTrackTotalHits(true)
            .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
            .setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("text", "500").boost(10.0f))
                    .should(QueryBuilders.termQuery("text", "499").boost(20.0f))
                    .should(QueryBuilders.termQuery("text", "498").boost(8.0f))
                    .should(QueryBuilders.termQuery("text", "497").boost(7.0f))
                    .should(QueryBuilders.termQuery("text", "496").boost(6.0f))
                    .should(QueryBuilders.termQuery("text", "485").boost(5.0f))
                    .should(QueryBuilders.termQuery("text", "494").boost(4.0f))
                    .should(QueryBuilders.termQuery("text", "506").boost(3.0f))
                    .should(QueryBuilders.termQuery("text", "505").boost(2.0f))
                    .should(QueryBuilders.termQuery("text", "511"))
            )
            .addFetchField("vector_asc")
            .addFetchField("vector_desc")
            .addFetchField("text")
            .setSize(19)
            .get();

        assertEquals(19, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(499.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals(9501.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
        assertEquals("term 499", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertTrue(vectors.contains(485.0));
        assertTrue(vectors.contains(492.0));
        assertTrue(vectors.contains(493.0));
        assertTrue(vectors.contains(494.0));
        assertTrue(vectors.contains(495.0));
        assertTrue(vectors.contains(496.0));
        assertTrue(vectors.contains(497.0));
        assertTrue(vectors.contains(498.0));
        assertTrue(vectors.contains(499.0));
        assertTrue(vectors.contains(500.0));
        assertTrue(vectors.contains(501.0));
        assertTrue(vectors.contains(502.0));
        assertTrue(vectors.contains(503.0));
        assertTrue(vectors.contains(504.0));
        assertTrue(vectors.contains(505.0));
        assertTrue(vectors.contains(506.0));
        assertTrue(vectors.contains(507.0));
        assertTrue(vectors.contains(511.0));
        // 491 and 508 have the score based on their positions so the result set
        // depends on what order the documents were received
        // on the coordinating node
        assertTrue(vectors.contains(491.0) || vectors.contains(508.0));
    }

    public void testBM25AndKnnWithBucketAggregation() {
        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 102, 1000);
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
            .setSize(11)
            .addAggregation(AggregationBuilders.terms("sums").field("int"))
            .get();

        assertEquals(11, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(492.0, 493.0, 494.0, 495.0, 496.0, 497.0, 498.0, 499.0, 500.0, 501.0, 502.0), vectors);

        LongTerms aggregation = response.getAggregations().get("sums");
        assertEquals(3, aggregation.getBuckets().size());

        Set<Long> keys = new HashSet<>();

        for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
            keys.add((long)bucket.getKey());
            assertEquals(34, bucket.getDocCount());
        }

        assertEquals(Set.of(0L, 1L, 2L), keys);
    }

    public void testMultipleOnlyKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 9500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 100);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 100);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankContextBuilder(new RRFRankContextBuilder().windowSize(50).rankConstant(1))
            .setTrackTotalHits(true)
            .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
            .addFetchField("vector_asc")
            .addFetchField("text")
            .addAggregation(AggregationBuilders.terms("sums").field("int"))
            .setSize(19)
            .get();

        assertEquals(19, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(Set.of(491.0, 492.0, 493.0, 494.0, 495.0, 496.0, 497.0, 498.0, 499.0, 500.0,
            501.0, 502.0, 503.0, 504.0, 505.0, 506.0, 507.0, 508.0, 509.0), vectors);

        LongTerms aggregation = response.getAggregations().get("sums");
        assertEquals(3, aggregation.getBuckets().size());

        Set<Long> keys = new HashSet<>();

        for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
            keys.add((long)bucket.getKey());
            assertEquals(17, bucket.getDocCount());
        }

        assertEquals(Set.of(0L, 1L, 2L), keys);
    }

    public void testBM25AndMultipleKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 9501.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 54, 100);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 54, 100);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankContextBuilder(new RRFRankContextBuilder().windowSize(50).rankConstant(1))
            .setTrackTotalHits(true)
            .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
            .setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("text", "500").boost(10.0f))
                    .should(QueryBuilders.termQuery("text", "499").boost(20.0f))
                    .should(QueryBuilders.termQuery("text", "498").boost(8.0f))
                    .should(QueryBuilders.termQuery("text", "497").boost(7.0f))
                    .should(QueryBuilders.termQuery("text", "496").boost(6.0f))
                    .should(QueryBuilders.termQuery("text", "485").boost(5.0f))
                    .should(QueryBuilders.termQuery("text", "494").boost(4.0f))
                    .should(QueryBuilders.termQuery("text", "506").boost(3.0f))
                    .should(QueryBuilders.termQuery("text", "505").boost(2.0f))
                    .should(QueryBuilders.termQuery("text", "511"))
            )
            .addFetchField("vector_asc")
            .addFetchField("vector_desc")
            .addFetchField("text")
            .addAggregation(AggregationBuilders.terms("sums").field("int"))
            .setSize(19)
            .get();

        assertEquals(19, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(499.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals(9501.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
        assertEquals("term 499", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertTrue(vectors.contains(485.0));
        assertTrue(vectors.contains(492.0));
        assertTrue(vectors.contains(493.0));
        assertTrue(vectors.contains(494.0));
        assertTrue(vectors.contains(495.0));
        assertTrue(vectors.contains(496.0));
        assertTrue(vectors.contains(497.0));
        assertTrue(vectors.contains(498.0));
        assertTrue(vectors.contains(499.0));
        assertTrue(vectors.contains(500.0));
        assertTrue(vectors.contains(501.0));
        assertTrue(vectors.contains(502.0));
        assertTrue(vectors.contains(503.0));
        assertTrue(vectors.contains(504.0));
        assertTrue(vectors.contains(505.0));
        assertTrue(vectors.contains(506.0));
        assertTrue(vectors.contains(507.0));
        assertTrue(vectors.contains(511.0));
        // 491 and 508 have the score based on their positions so the result set
        // depends on what order the documents were received
        // on the coordinating node
        assertTrue(vectors.contains(491.0) || vectors.contains(508.0));

        LongTerms aggregation = response.getAggregations().get("sums");
        assertEquals(3, aggregation.getBuckets().size());

        Set<Long> keys = new HashSet<>();

        for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
            keys.add((long)bucket.getKey());
            assertEquals(18, bucket.getDocCount());
        }

        assertEquals(Set.of(0L, 1L, 2L), keys);
    }
}

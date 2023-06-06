/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(maxNumDataNodes = 3)
@ESIntegTestCase.SuiteScopeTestCase
public class RRFRankIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RRFRankPlugin.class);
    }

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

        // Set up an index with a very small number of documents to
        // test sizing limits and issues with empty data.

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

        // Set up an index with non-random data, so we can
        // do direct tests against expected results.

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

        for (int doc = 0; doc < 1001; ++doc) {
            client().prepareIndex("nrd_index")
                .setSource(
                    "vector_asc",
                    new float[] { doc },
                    "vector_desc",
                    new float[] { 1000 - doc },
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
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 3, 3, null);
        SearchResponse response = client().prepareSearch("tiny_index")
            .setRankBuilder(new RRFRankBuilder(100, 1))
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
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankBuilder(new RRFRankBuilder(101, 1))
            .setTrackTotalHits(false)
            .setKnnSearch(List.of(knnSearch))
            .setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("text", "500").boost(11.0f))
                    .should(QueryBuilders.termQuery("text", "499").boost(10.0f))
                    .should(QueryBuilders.termQuery("text", "498").boost(9.0f))
                    .should(QueryBuilders.termQuery("text", "497").boost(8.0f))
                    .should(QueryBuilders.termQuery("text", "496").boost(7.0f))
                    .should(QueryBuilders.termQuery("text", "495").boost(6.0f))
                    .should(QueryBuilders.termQuery("text", "494").boost(5.0f))
                    .should(QueryBuilders.termQuery("text", "493").boost(4.0f))
                    .should(QueryBuilders.termQuery("text", "492").boost(3.0f))
                    .should(QueryBuilders.termQuery("text", "491").boost(2.0f))
            )
            .addFetchField("vector_asc")
            .addFetchField("text")
            .setSize(11)
            .get();

        assertNull(response.getHits().getTotalHits());
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
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankBuilder(new RRFRankBuilder(51, 1))
            .setTrackTotalHits(true)
            .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
            .addFetchField("vector_asc")
            .addFetchField("text")
            .setSize(19)
            .get();

        assertEquals(51, response.getHits().getTotalHits().value);
        assertEquals(19, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(
            Set.of(
                491.0,
                492.0,
                493.0,
                494.0,
                495.0,
                496.0,
                497.0,
                498.0,
                499.0,
                500.0,
                501.0,
                502.0,
                503.0,
                504.0,
                505.0,
                506.0,
                507.0,
                508.0,
                509.0
            ),
            vectors
        );
    }

    public void testBM25AndMultipleKnn() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankBuilder(new RRFRankBuilder(51, 1))
            .setTrackTotalHits(false)
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
                    .should(QueryBuilders.termQuery("text", "511").boost(9.0f))
            )
            .addFetchField("vector_asc")
            .addFetchField("vector_desc")
            .addFetchField("text")
            .setSize(19)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(19, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals(500.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        hit = response.getHits().getAt(1);
        assertEquals(2, hit.getRank());
        assertEquals(499.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals(501.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
        assertEquals("term 499", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(
            Set.of(
                485.0,
                492.0,
                493.0,
                494.0,
                495.0,
                496.0,
                497.0,
                498.0,
                499.0,
                500.0,
                501.0,
                502.0,
                503.0,
                504.0,
                505.0,
                506.0,
                507.0,
                508.0,
                511.0
            ),
            vectors
        );
    }

    public void testBM25AndKnnWithBucketAggregation() {
        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankBuilder(new RRFRankBuilder(101, 1))
            .setTrackTotalHits(true)
            .setKnnSearch(List.of(knnSearch))
            .setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("text", "500").boost(11.0f))
                    .should(QueryBuilders.termQuery("text", "499").boost(10.0f))
                    .should(QueryBuilders.termQuery("text", "498").boost(9.0f))
                    .should(QueryBuilders.termQuery("text", "497").boost(8.0f))
                    .should(QueryBuilders.termQuery("text", "496").boost(7.0f))
                    .should(QueryBuilders.termQuery("text", "495").boost(6.0f))
                    .should(QueryBuilders.termQuery("text", "494").boost(5.0f))
                    .should(QueryBuilders.termQuery("text", "493").boost(4.0f))
                    .should(QueryBuilders.termQuery("text", "492").boost(3.0f))
                    .should(QueryBuilders.termQuery("text", "491").boost(2.0f))
            )
            .addFetchField("vector_asc")
            .addFetchField("text")
            .setSize(11)
            .addAggregation(AggregationBuilders.terms("sums").field("int"))
            .get();

        assertEquals(101, response.getHits().getTotalHits().value);
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

        for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
            if (0L == (long) bucket.getKey()) {
                assertEquals(34, bucket.getDocCount());
            } else if (1L == (long) bucket.getKey()) {
                assertEquals(34, bucket.getDocCount());
            } else if (2L == (long) bucket.getKey()) {
                assertEquals(33, bucket.getDocCount());
            } else {
                throw new IllegalArgumentException("unexpected bucket key [" + bucket.getKey() + "]");
            }
        }
    }

    public void testMultipleOnlyKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankBuilder(new RRFRankBuilder(51, 1))
            .setTrackTotalHits(false)
            .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
            .addFetchField("vector_asc")
            .addFetchField("text")
            .setSize(19)
            .addAggregation(AggregationBuilders.terms("sums").field("int"))
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(19, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(
            Set.of(
                491.0,
                492.0,
                493.0,
                494.0,
                495.0,
                496.0,
                497.0,
                498.0,
                499.0,
                500.0,
                501.0,
                502.0,
                503.0,
                504.0,
                505.0,
                506.0,
                507.0,
                508.0,
                509.0
            ),
            vectors
        );

        LongTerms aggregation = response.getAggregations().get("sums");
        assertEquals(3, aggregation.getBuckets().size());

        for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
            if (0L == (long) bucket.getKey()) {
                assertEquals(17, bucket.getDocCount());
            } else if (1L == (long) bucket.getKey()) {
                assertEquals(17, bucket.getDocCount());
            } else if (2L == (long) bucket.getKey()) {
                assertEquals(17, bucket.getDocCount());
            } else {
                throw new IllegalArgumentException("unexpected bucket key [" + bucket.getKey() + "]");
            }
        }
    }

    public void testBM25AndMultipleKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankBuilder(new RRFRankBuilder(51, 1))
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
                    .should(QueryBuilders.termQuery("text", "511").boost(9.0f))
            )
            .addFetchField("vector_asc")
            .addFetchField("vector_desc")
            .addFetchField("text")
            .setSize(19)
            .addAggregation(AggregationBuilders.terms("sums").field("int"))
            .setStats("search")
            .get();

        assertEquals(51, response.getHits().getTotalHits().value);
        assertEquals(19, response.getHits().getHits().length);

        SearchHit hit = response.getHits().getAt(0);
        assertEquals(1, hit.getRank());
        assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals(500.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
        assertEquals("term 500", hit.field("text").getValue());

        hit = response.getHits().getAt(1);
        assertEquals(2, hit.getRank());
        assertEquals(499.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
        assertEquals(501.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
        assertEquals("term 499", hit.field("text").getValue());

        Set<Double> vectors = Arrays.stream(response.getHits().getHits())
            .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
            .collect(Collectors.toSet());
        assertEquals(
            Set.of(
                485.0,
                492.0,
                493.0,
                494.0,
                495.0,
                496.0,
                497.0,
                498.0,
                499.0,
                500.0,
                501.0,
                502.0,
                503.0,
                504.0,
                505.0,
                506.0,
                507.0,
                508.0,
                511.0
            ),
            vectors
        );

        LongTerms aggregation = response.getAggregations().get("sums");
        assertEquals(3, aggregation.getBuckets().size());

        for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
            if (0L == (long) bucket.getKey()) {
                assertEquals(17, bucket.getDocCount());
            } else if (1L == (long) bucket.getKey()) {
                assertEquals(17, bucket.getDocCount());
            } else if (2L == (long) bucket.getKey()) {
                assertEquals(17, bucket.getDocCount());
            } else {
                throw new IllegalArgumentException("unexpected bucket key [" + bucket.getKey() + "]");
            }
        }
    }
}

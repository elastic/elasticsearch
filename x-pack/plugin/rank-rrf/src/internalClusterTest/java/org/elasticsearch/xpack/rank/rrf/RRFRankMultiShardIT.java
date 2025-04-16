/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2, maxNumDataNodes = 4)
@ESIntegTestCase.SuiteScopeTestCase
public class RRFRankMultiShardIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RRFRankPlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 7;
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

        prepareIndex("tiny_index").setSource("vector", new float[] { 0.0f }, "text", "term term").get();
        prepareIndex("tiny_index").setSource("vector", new float[] { 1.0f }, "text", "other").get();
        prepareIndex("tiny_index").setSource("vector", new float[] { 2.0f }, "text", "term").get();

        indicesAdmin().prepareRefresh("tiny_index").get();

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
            .startObject("text0")
            .field("type", "text")
            .endObject()
            .startObject("text1")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();

        assertAcked(prepareCreate("nrd_index").setMapping(builder));
        ensureGreen(TimeValue.timeValueSeconds(120), "nrd_index");

        for (int doc = 0; doc < 1001; ++doc) {
            prepareIndex("nrd_index").setSource(
                "vector_asc",
                new float[] { doc },
                "vector_desc",
                new float[] { 1000 - doc },
                "int",
                doc % 3,
                "text0",
                "term " + doc,
                "text1",
                "term " + (1000 - doc)
            ).get();
        }

        indicesAdmin().prepareRefresh("nrd_index").get();
    }

    public void testTotalDocsSmallerThanSize() {
        float[] queryVector = { 0.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 3, 3, null, null);
        assertResponse(
            prepareSearch("tiny_index").setRankBuilder(new RRFRankBuilder(100, 1))
                .setKnnSearch(List.of(knnSearch))
                .setQuery(QueryBuilders.termQuery("text", "term"))
                .addFetchField("vector")
                .addFetchField("text"),
            response -> {
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
        );
    }

    public void testBM25AndKnn() {
        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(101, 1))
                .setTrackTotalHits(false)
                .setKnnSearch(List.of(knnSearch))
                .setQuery(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(11.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(10.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(9.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(8.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(7.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "495")).boost(6.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(5.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "493")).boost(4.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "492")).boost(3.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "491")).boost(2.0f))
                )
                .addFetchField("vector_asc")
                .addFetchField("text0")
                .setSize(11),
            response -> {
                assertNull(response.getHits().getTotalHits());
                assertEquals(11, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals("term 500", hit.field("text0").getValue());

                Set<Double> vectors = Arrays.stream(response.getHits().getHits())
                    .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
                    .collect(Collectors.toSet());
                assertEquals(Set.of(492.0, 493.0, 494.0, 495.0, 496.0, 497.0, 498.0, 499.0, 500.0, 501.0, 502.0), vectors);
            }
        );
    }

    public void testMultipleOnlyKnn() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(51, 1))
                .setTrackTotalHits(true)
                .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
                .addFetchField("vector_asc")
                .addFetchField("text0")
                .setSize(19),
            response -> {
                assertEquals(51, response.getHits().getTotalHits().value());
                assertEquals(19, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals("term 500", hit.field("text0").getValue());

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
        );
    }

    public void testBM25AndMultipleKnn() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(51, 1))
                .setTrackTotalHits(false)
                .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
                .setQuery(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(10.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(20.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(8.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(7.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(6.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "485")).boost(5.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(4.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "506")).boost(3.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "505")).boost(2.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "511")).boost(9.0f))
                )
                .addFetchField("vector_asc")
                .addFetchField("vector_desc")
                .addFetchField("text0")
                .setSize(19),
            response -> {
                assertNull(response.getHits().getTotalHits());
                assertEquals(19, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals(500.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
                assertEquals("term 500", hit.field("text0").getValue());

                hit = response.getHits().getAt(1);
                assertEquals(2, hit.getRank());
                assertEquals(499.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals(501.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
                assertEquals("term 499", hit.field("text0").getValue());

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
        );
    }

    public void testBM25AndKnnWithBucketAggregation() {
        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(101, 1))
                .setTrackTotalHits(true)
                .setKnnSearch(List.of(knnSearch))
                .setQuery(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(11.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(10.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(9.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(8.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(7.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "495")).boost(6.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(5.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "493")).boost(4.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "492")).boost(3.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "491")).boost(2.0f))
                )
                .addFetchField("vector_asc")
                .addFetchField("text0")
                .setSize(11)
                .addAggregation(AggregationBuilders.terms("sums").field("int")),
            response -> {
                assertEquals(101, response.getHits().getTotalHits().value());
                assertEquals(11, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals("term 500", hit.field("text0").getValue());

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
        );
    }

    public void testMultipleOnlyKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(51, 1))
                .setTrackTotalHits(false)
                .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
                .addFetchField("vector_asc")
                .addFetchField("text0")
                .setSize(19)
                .addAggregation(AggregationBuilders.terms("sums").field("int")),
            response -> {
                assertNull(response.getHits().getTotalHits());
                assertEquals(19, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals("term 500", hit.field("text0").getValue());

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
        );
    }

    public void testBM25AndMultipleKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(51, 1))
                .setTrackTotalHits(true)
                .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
                .setQuery(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(10.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(20.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(8.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(7.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(6.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "485")).boost(5.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(4.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "506")).boost(3.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "505")).boost(2.0f))
                        .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "511")).boost(9.0f))
                )
                .addFetchField("vector_asc")
                .addFetchField("vector_desc")
                .addFetchField("text0")
                .setSize(19)
                .addAggregation(AggregationBuilders.terms("sums").field("int"))
                .setStats("search"),
            response -> {
                assertEquals(51, response.getHits().getTotalHits().value());
                assertEquals(19, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals(500.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
                assertEquals("term 500", hit.field("text0").getValue());

                hit = response.getHits().getAt(1);
                assertEquals(2, hit.getRank());
                assertEquals(499.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals(501.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);
                assertEquals("term 499", hit.field("text0").getValue());

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
        );
    }

    public void testMultiBM25() {
        for (SearchType searchType : SearchType.CURRENTLY_SUPPORTED) {
            assertResponse(
                prepareSearch("nrd_index").setSearchType(searchType)
                    .setRankBuilder(new RRFRankBuilder(8, 1))
                    .setTrackTotalHits(false)
                    .setSubSearches(
                        List.of(
                            new SubSearchSourceBuilder(
                                QueryBuilders.boolQuery()
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(10.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(9.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(8.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(7.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(6.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "495")).boost(5.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(4.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "492")).boost(3.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "491")).boost(2.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "490")).boost(1.0f))
                            ),
                            new SubSearchSourceBuilder(
                                QueryBuilders.boolQuery()
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "508")).boost(9.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "304")).boost(8.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "501")).boost(7.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "504")).boost(6.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "502")).boost(5.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "499")).boost(4.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "800")).boost(3.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "201")).boost(2.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "492")).boost(1.0f))
                            )
                        )
                    )
                    .addFetchField("text0")
                    .addFetchField("text1")
                    .setSize(5),
                response -> {
                    assertNull(response.getHits().getTotalHits());
                    assertEquals(5, response.getHits().getHits().length);

                    SearchHit hit = response.getHits().getAt(0);
                    assertEquals(1, hit.getRank());
                    assertEquals("term 492", hit.field("text0").getValue());
                    assertEquals("term 508", hit.field("text1").getValue());

                    hit = response.getHits().getAt(1);
                    assertEquals(2, hit.getRank());
                    assertEquals("term 499", hit.field("text0").getValue());
                    assertEquals("term 501", hit.field("text1").getValue());

                    hit = response.getHits().getAt(2);
                    assertEquals(3, hit.getRank());
                    assertEquals("term 500", hit.field("text0").getValue());
                    assertEquals("term 500", hit.field("text1").getValue());

                    hit = response.getHits().getAt(3);
                    assertEquals(4, hit.getRank());
                    assertEquals("term 498", hit.field("text0").getValue());
                    assertEquals("term 502", hit.field("text1").getValue());

                    hit = response.getHits().getAt(4);
                    assertEquals(5, hit.getRank());
                    assertEquals("term 496", hit.field("text0").getValue());
                    assertEquals("term 504", hit.field("text1").getValue());
                }
            );
        }
    }

    public void testMultiBM25WithAggregation() {
        for (SearchType searchType : SearchType.CURRENTLY_SUPPORTED) {
            assertResponse(
                prepareSearch("nrd_index").setSearchType(searchType)
                    .setRankBuilder(new RRFRankBuilder(8, 1))
                    .setTrackTotalHits(false)
                    .setSubSearches(
                        List.of(
                            new SubSearchSourceBuilder(
                                QueryBuilders.boolQuery()
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(10.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(9.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(8.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(7.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(6.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "495")).boost(5.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(4.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "492")).boost(3.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "491")).boost(2.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "490")).boost(1.0f))
                            ),
                            new SubSearchSourceBuilder(
                                QueryBuilders.boolQuery()
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "508")).boost(9.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "304")).boost(8.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "501")).boost(7.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "504")).boost(6.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "502")).boost(5.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "499")).boost(4.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "801")).boost(3.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "201")).boost(2.0f))
                                    .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "492")).boost(1.0f))
                            )
                        )
                    )
                    .addFetchField("text0")
                    .addFetchField("text1")
                    .setSize(5)
                    .addAggregation(AggregationBuilders.terms("sums").field("int")),
                response -> {
                    assertNull(response.getHits().getTotalHits());
                    assertEquals(5, response.getHits().getHits().length);

                    SearchHit hit = response.getHits().getAt(0);
                    assertEquals(1, hit.getRank());
                    assertEquals("term 492", hit.field("text0").getValue());
                    assertEquals("term 508", hit.field("text1").getValue());

                    hit = response.getHits().getAt(1);
                    assertEquals(2, hit.getRank());
                    assertEquals("term 499", hit.field("text0").getValue());
                    assertEquals("term 501", hit.field("text1").getValue());

                    hit = response.getHits().getAt(2);
                    assertEquals(3, hit.getRank());
                    assertEquals("term 500", hit.field("text0").getValue());
                    assertEquals("term 500", hit.field("text1").getValue());

                    hit = response.getHits().getAt(3);
                    assertEquals(4, hit.getRank());
                    assertEquals("term 498", hit.field("text0").getValue());
                    assertEquals("term 502", hit.field("text1").getValue());

                    hit = response.getHits().getAt(4);
                    assertEquals(5, hit.getRank());
                    assertEquals("term 496", hit.field("text0").getValue());
                    assertEquals("term 504", hit.field("text1").getValue());

                    LongTerms aggregation = response.getAggregations().get("sums");
                    assertEquals(3, aggregation.getBuckets().size());

                    for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
                        if (0L == (long) bucket.getKey()) {
                            assertEquals(5, bucket.getDocCount());
                        } else if (1L == (long) bucket.getKey()) {
                            assertEquals(6, bucket.getDocCount());
                        } else if (2L == (long) bucket.getKey()) {
                            assertEquals(4, bucket.getDocCount());
                        } else {
                            throw new IllegalArgumentException("unexpected bucket key [" + bucket.getKey() + "]");
                        }
                    }
                }
            );
        }
    }

    public void testMultiBM25AndSingleKnn() {
        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(101, 1))
                .setTrackTotalHits(false)
                .setKnnSearch(List.of(knnSearch))
                .setSubSearches(
                    List.of(
                        new SubSearchSourceBuilder(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(10.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(9.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(8.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(7.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(6.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "495")).boost(5.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(4.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "492")).boost(3.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "491")).boost(2.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "490")).boost(1.0f))
                        ),
                        new SubSearchSourceBuilder(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "508")).boost(9.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "304")).boost(8.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "501")).boost(7.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "504")).boost(6.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "492")).boost(5.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "502")).boost(4.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "499")).boost(3.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "800")).boost(2.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "201")).boost(1.0f))
                        )
                    )
                )
                .addFetchField("text0")
                .addFetchField("text1")
                .addFetchField("vector_asc")
                .setSize(5),
            response -> {
                assertNull(response.getHits().getTotalHits());
                assertEquals(5, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals("term 500", hit.field("text0").getValue());
                assertEquals("term 500", hit.field("text1").getValue());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);

                Set<Double> vectors = Arrays.stream(response.getHits().getHits())
                    .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
                    .collect(Collectors.toSet());
                assertEquals(Set.of(492.0, 496.0, 498.0, 499.0, 500.0), vectors);
            }
        );
    }

    public void testMultiBM25AndSingleKnnWithAggregation() {
        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(101, 1))
                .setTrackTotalHits(false)
                .setKnnSearch(List.of(knnSearch))
                .setSubSearches(
                    List.of(
                        new SubSearchSourceBuilder(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(10.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(9.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(8.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(7.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(6.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "495")).boost(5.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(4.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "492")).boost(3.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "491")).boost(2.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "490")).boost(1.0f))
                        ),
                        new SubSearchSourceBuilder(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "508")).boost(9.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "304")).boost(8.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "501")).boost(7.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "504")).boost(6.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "492")).boost(5.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "502")).boost(4.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "499")).boost(3.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "800")).boost(2.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "201")).boost(1.0f))
                        )
                    )
                )
                .addFetchField("text0")
                .addFetchField("text1")
                .addFetchField("vector_asc")
                .setSize(5)
                .addAggregation(AggregationBuilders.terms("sums").field("int")),
            response -> {
                assertNull(response.getHits().getTotalHits());
                assertEquals(5, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals("term 500", hit.field("text0").getValue());
                assertEquals("term 500", hit.field("text1").getValue());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);

                Set<Double> vectors = Arrays.stream(response.getHits().getHits())
                    .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
                    .collect(Collectors.toSet());
                assertEquals(Set.of(492.0, 496.0, 498.0, 499.0, 500.0), vectors);

                LongTerms aggregation = response.getAggregations().get("sums");
                assertEquals(3, aggregation.getBuckets().size());

                for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
                    if (0L == (long) bucket.getKey()) {
                        assertEquals(35, bucket.getDocCount());
                    } else if (1L == (long) bucket.getKey()) {
                        assertEquals(35, bucket.getDocCount());
                    } else if (2L == (long) bucket.getKey()) {
                        assertEquals(34, bucket.getDocCount());
                    } else {
                        throw new IllegalArgumentException("unexpected bucket key [" + bucket.getKey() + "]");
                    }
                }
            }
        );
    }

    public void testMultiBM25AndMultipleKnn() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 101, 1001, null, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 101, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(101, 1))
                .setTrackTotalHits(false)
                .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
                .setSubSearches(
                    List.of(
                        new SubSearchSourceBuilder(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(10.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(9.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(8.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(7.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(6.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "495")).boost(5.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(4.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "492")).boost(3.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "491")).boost(2.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "490")).boost(1.0f))
                        ),
                        new SubSearchSourceBuilder(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "508")).boost(9.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "304")).boost(8.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "501")).boost(7.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "504")).boost(6.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "492")).boost(5.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "502")).boost(4.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "499")).boost(3.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "800")).boost(2.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "201")).boost(1.0f))
                        )
                    )
                )
                .addFetchField("text0")
                .addFetchField("text1")
                .addFetchField("vector_asc")
                .addFetchField("vector_desc")
                .setSize(5),
            response -> {
                assertNull(response.getHits().getTotalHits());
                assertEquals(5, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals("term 500", hit.field("text0").getValue());
                assertEquals("term 500", hit.field("text1").getValue());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals(500.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);

                Set<Double> vectors = Arrays.stream(response.getHits().getHits())
                    .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
                    .collect(Collectors.toSet());
                assertEquals(Set.of(492.0, 498.0, 499.0, 500.0, 501.0), vectors);
            }
        );
    }

    public void testMultiBM25AndMultipleKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 101, 1001, null, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 101, 1001, null, null);
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(101, 1))
                .setTrackTotalHits(false)
                .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
                .setSubSearches(
                    List.of(
                        new SubSearchSourceBuilder(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "500")).boost(10.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "499")).boost(9.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "498")).boost(8.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "497")).boost(7.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "496")).boost(6.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "495")).boost(5.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "494")).boost(4.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "492")).boost(3.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "491")).boost(2.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text0", "490")).boost(1.0f))
                        ),
                        new SubSearchSourceBuilder(
                            QueryBuilders.boolQuery()
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "508")).boost(9.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "304")).boost(8.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "501")).boost(7.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "504")).boost(6.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "492")).boost(5.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "502")).boost(4.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "499")).boost(3.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "800")).boost(2.0f))
                                .should(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("text1", "201")).boost(1.0f))
                        )
                    )
                )
                .addFetchField("text0")
                .addFetchField("text1")
                .addFetchField("vector_asc")
                .addFetchField("vector_desc")
                .setSize(5)
                .addAggregation(AggregationBuilders.terms("sums").field("int")),
            response -> {
                assertNull(response.getHits().getTotalHits());
                assertEquals(5, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertEquals("term 500", hit.field("text0").getValue());
                assertEquals("term 500", hit.field("text1").getValue());
                assertEquals(500.0, ((Number) hit.field("vector_asc").getValue()).doubleValue(), 0.0);
                assertEquals(500.0, ((Number) hit.field("vector_desc").getValue()).doubleValue(), 0.0);

                Set<Double> vectors = Arrays.stream(response.getHits().getHits())
                    .map(h -> ((Number) h.field("vector_asc").getValue()).doubleValue())
                    .collect(Collectors.toSet());
                assertEquals(Set.of(492.0, 498.0, 499.0, 500.0, 501.0), vectors);

                LongTerms aggregation = response.getAggregations().get("sums");
                assertEquals(3, aggregation.getBuckets().size());

                for (LongTerms.Bucket bucket : aggregation.getBuckets()) {
                    if (0L == (long) bucket.getKey()) {
                        assertEquals(35, bucket.getDocCount());
                    } else if (1L == (long) bucket.getKey()) {
                        assertEquals(35, bucket.getDocCount());
                    } else if (2L == (long) bucket.getKey()) {
                        assertEquals(34, bucket.getDocCount());
                    } else {
                        throw new IllegalArgumentException("unexpected bucket key [" + bucket.getKey() + "]");
                    }
                }
            }
        );
    }

    public void testBasicRRFExplain() {
        // our query here is a top-level knn query for vector [9] and a term query for "text0: 10"
        // the first result should be the one present in both queries (i.e. doc with text0: 10 and vector: [10]) and the other ones
        // should only match the knn query
        float[] queryVector = { 9f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null, null).queryName("my_knn_search");
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(100, 1))
                .setKnnSearch(List.of(knnSearch))
                .setQuery(QueryBuilders.termQuery("text0", "10"))
                .setExplain(true)
                .setSize(3),
            response -> {
                // we cast to Number when looking at values in vector fields because different xContentTypes may return Float or Double
                assertEquals(3, response.getHits().getHits().length);

                // first result is the one which matches the term (10) so we should expect an explanation for both queries
                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(2, hit.getExplanation().getDetails().length);
                assertTrue(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(1, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertTrue(hit.getExplanation().getDetails()[0].getDescription().contains("query at index [0]"));
                assertTrue(hit.getExplanation().getDetails()[0].getDetails().length > 0);
                assertTrue(hit.getExplanation().getDetails()[1].isMatch());
                assertTrue(hit.getExplanation().getDetails()[1].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[1].getDetails().length > 0);

                // second result matched only on the knn query so no match should be expected for the term query
                hit = response.getHits().getAt(1);
                assertEquals(2, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(2, hit.getExplanation().getDetails().length);
                assertFalse(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(0, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertEquals(
                    "rrf score: [0], result not found in query at index [0]",
                    hit.getExplanation().getDetails()[0].getDescription()
                );
                assertEquals(0, hit.getExplanation().getDetails()[0].getDetails().length);
                assertTrue(hit.getExplanation().getDetails()[1].isMatch());
                assertTrue(hit.getExplanation().getDetails()[1].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[1].getDetails().length > 0);

                // third result matched only on the knn query so no match should be expected for the term query
                hit = response.getHits().getAt(2);
                assertEquals(3, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(2, hit.getExplanation().getDetails().length);
                assertFalse(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(0, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertEquals(
                    "rrf score: [0], result not found in query at index [0]",
                    hit.getExplanation().getDetails()[0].getDescription()
                );
                assertEquals(0, hit.getExplanation().getDetails()[0].getDetails().length);
                assertTrue(hit.getExplanation().getDetails()[1].isMatch());
                assertTrue(hit.getExplanation().getDetails()[1].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[1].getDetails().length > 0);
            }
        );
    }

    public void testRRFExplainUnknownField() {
        // in this test we try knn with a query on an unknown field that would be rewritten to MatchNoneQuery
        // so we expect results and explanations only for the first part
        float[] queryVector = { 9f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null, null).queryName("my_knn_search");
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(100, 1))
                .setKnnSearch(List.of(knnSearch))
                .setQuery(QueryBuilders.termQuery("unknown_field", "10"))
                .setExplain(true)
                .setSize(3),
            response -> {
                // we cast to Number when looking at values in vector fields because different xContentTypes may return Float or Double
                assertEquals(3, response.getHits().getHits().length);

                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(2, hit.getExplanation().getDetails().length);
                assertFalse(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(0, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertEquals(
                    "rrf score: [0], result not found in query at index [0]",
                    hit.getExplanation().getDetails()[0].getDescription()
                );
                assertEquals(0, hit.getExplanation().getDetails()[0].getDetails().length);
                assertTrue(hit.getExplanation().getDetails()[1].isMatch());
                assertTrue(hit.getExplanation().getDetails()[1].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[1].getDetails().length > 0);

                hit = response.getHits().getAt(1);
                assertEquals(2, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(2, hit.getExplanation().getDetails().length);
                assertFalse(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(0, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertEquals(
                    "rrf score: [0], result not found in query at index [0]",
                    hit.getExplanation().getDetails()[0].getDescription()
                );
                assertEquals(0, hit.getExplanation().getDetails()[0].getDetails().length);
                assertTrue(hit.getExplanation().getDetails()[1].isMatch());
                assertTrue(hit.getExplanation().getDetails()[1].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[1].getDetails().length > 0);

                hit = response.getHits().getAt(2);
                assertEquals(3, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(2, hit.getExplanation().getDetails().length);
                assertFalse(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(0, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertEquals(
                    "rrf score: [0], result not found in query at index [0]",
                    hit.getExplanation().getDetails()[0].getDescription()
                );
                assertEquals(0, hit.getExplanation().getDetails()[0].getDetails().length, 0);
                assertTrue(hit.getExplanation().getDetails()[1].isMatch());
                assertTrue(hit.getExplanation().getDetails()[1].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[1].getDetails().length > 0);
            }
        );
    }

    public void testRRFExplainOneUnknownFieldSubSearches() {
        // this test is similar to the above with the difference that we have a list of subsearches that one would fail,
        // while the other one would produce a match.
        // So, we'd have a total of 3 queries, a (rewritten) MatchNoneQuery, a TermQuery, and a kNN query
        float[] queryVector = { 9f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null, null).queryName("my_knn_search");
        assertResponse(
            prepareSearch("nrd_index").setRankBuilder(new RRFRankBuilder(100, 1))
                .setKnnSearch(List.of(knnSearch))
                .setSubSearches(
                    List.of(
                        new SubSearchSourceBuilder(QueryBuilders.termQuery("unknown_field", "10")),
                        new SubSearchSourceBuilder(QueryBuilders.termQuery("text0", "10"))
                    )
                )
                .setExplain(true)
                .setSize(3),
            response -> {
                // we cast to Number when looking at values in vector fields because different xContentTypes may return Float or Double
                assertEquals(3, response.getHits().getHits().length);

                // first result is the one which matches the term (10) and is 3rd closest to our query vector (9)
                SearchHit hit = response.getHits().getAt(0);
                assertEquals(1, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(3, hit.getExplanation().getDetails().length);
                // MatchNone query
                assertFalse(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(0, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertEquals(
                    "rrf score: [0], result not found in query at index [0]",
                    hit.getExplanation().getDetails()[0].getDescription()
                );
                assertEquals(0, hit.getExplanation().getDetails()[0].getDetails().length);
                // Term query
                assertTrue(hit.getExplanation().getDetails()[1].isMatch());
                assertTrue(hit.getExplanation().getDetails()[1].getDescription().contains("query at index [1]"));
                assertTrue(hit.getExplanation().getDetails()[1].getDetails().length > 0);
                // knn query
                assertTrue(hit.getExplanation().getDetails()[2].isMatch());
                assertTrue(hit.getExplanation().getDetails()[2].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[2].getDetails().length > 0);

                // rest of hits match only on the knn query so no match should be expected for the term query either
                hit = response.getHits().getAt(1);
                assertEquals(2, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(3, hit.getExplanation().getDetails().length);
                // MatchNone query
                assertFalse(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(0, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertEquals(
                    "rrf score: [0], result not found in query at index [0]",
                    hit.getExplanation().getDetails()[0].getDescription()
                );
                // term query - should not match
                assertFalse(hit.getExplanation().getDetails()[1].isMatch());
                assertEquals(
                    "rrf score: [0], result not found in query at index [1]",
                    hit.getExplanation().getDetails()[1].getDescription()
                );
                assertEquals(0, hit.getExplanation().getDetails()[1].getDetails().length);
                // knn query
                assertTrue(hit.getExplanation().getDetails()[2].isMatch());
                assertTrue(hit.getExplanation().getDetails()[2].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[2].getDetails().length > 0);

                // rest of hits match only on the knn query so no match should be expected for the term query either
                hit = response.getHits().getAt(2);
                assertEquals(3, hit.getRank());
                assertTrue(hit.getExplanation().isMatch());
                assertTrue(hit.getExplanation().getDescription().contains("initial ranks"));
                assertEquals(3, hit.getExplanation().getDetails().length);
                // MatchNone query
                assertFalse(hit.getExplanation().getDetails()[0].isMatch());
                assertEquals(0, hit.getExplanation().getDetails()[0].getValue().intValue());
                assertEquals(
                    "rrf score: [0], result not found in query at index [0]",
                    hit.getExplanation().getDetails()[0].getDescription()
                );
                // term query - should not match
                assertFalse(hit.getExplanation().getDetails()[1].isMatch());
                assertEquals(
                    "rrf score: [0], result not found in query at index [1]",
                    hit.getExplanation().getDetails()[1].getDescription()
                );
                assertEquals(0, hit.getExplanation().getDetails()[1].getDetails().length);
                // knn query
                assertTrue(hit.getExplanation().getDetails()[2].isMatch());
                assertTrue(hit.getExplanation().getDetails()[2].getDescription().contains("[my_knn_search]"));
                assertTrue(hit.getExplanation().getDetails()[2].getDetails().length > 0);
            }
        );
    }
}

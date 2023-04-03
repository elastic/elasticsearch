/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(maxNumDataNodes = 3)
@ESIntegTestCase.SuiteScopeTestCase
public class RRFRankScriptIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PainlessPlugin.class, RankRRFPlugin.class);
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

        // Set up an index with non-random data, so we can
        // do direct tests against expected results.

        XContentBuilder builder = XContentFactory.jsonBuilder()
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

    /*public void testBM25AndKnnWithScriptedMetricAggregation() {
        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector_asc", queryVector, 101, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankContextBuilder(new RRFRankBuilder().windowSize(101).rankConstant(1))
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
            .addAggregation(
                AggregationBuilders.scriptedMetric("sums")
                    .initScript(new Script("state['sums'] = ['asc': [], 'text': []]"))
                    .mapScript(new Script("""
                        state['sums']['asc'].add($('vector_asc', null).getVector()[0]);
                        state['sums']['text'].add(Integer.parseInt($('text', null).substring(5)));
                        """))
                    .combineScript(new Script("""
                        return [
                            'asc_total': state['sums']['asc'].stream().mapToDouble(v -> v).sum(),
                            'text_total': state['sums']['text'].stream().mapToInt(v -> v).sum()
                        ]
                        """))
                    .reduceScript(new Script("""
                        return [
                            'asc_total': states.stream().mapToDouble(v -> v['asc_total']).sum(),
                            'text_total': states.stream().mapToInt(v -> v['text_total']).sum()
                        ]
                        """))
            )
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

        InternalScriptedMetric ism = response.getAggregations().get("sums");
        @SuppressWarnings("unchecked")
        Map<String, Object> sums = (Map<String, Object>) ism.aggregation();
        for (Map.Entry<String, Object> sum : sums.entrySet()) {
            if ("asc_total".equals(sum.getKey())) {
                assertEquals(50500.0, sum.getValue());
            } else if ("text_total".equals(sum.getKey())) {
                assertEquals(50500, sum.getValue());
            } else {
                throw new IllegalArgumentException("unexpected scripted metric aggregation key [" + sum.getKey() + "]");
            }
        }
    }

    public void testMultipleOnlyKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankContextBuilder(new RRFRankBuilder().windowSize(51).rankConstant(1))
            .setTrackTotalHits(false)
            .setKnnSearch(List.of(knnSearchAsc, knnSearchDesc))
            .addFetchField("vector_asc")
            .addFetchField("text")
            .setSize(19)
            .addAggregation(
                AggregationBuilders.scriptedMetric("sums")
                    .initScript(new Script("state['sums'] = ['asc': [], 'desc': []]"))
                    .mapScript(new Script("""
                        state['sums']['asc'].add($('vector_asc', null).getVector()[0]);
                        state['sums']['desc'].add($('vector_desc', null).getVector()[0]);
                        """))
                    .combineScript(new Script("""
                        return [
                            'asc_total': state['sums']['asc'].stream().mapToDouble(v -> v).sum(),
                            'desc_total': state['sums']['desc'].stream().mapToDouble(v -> v).sum()
                        ]
                        """))
                    .reduceScript(new Script("""
                        return [
                            'asc_total': states.stream().mapToDouble(v -> v['asc_total']).sum(),
                            'desc_total': states.stream().mapToDouble(v -> v['desc_total']).sum()
                        ]
                        """))
            )
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

        InternalScriptedMetric ism = response.getAggregations().get("sums");
        @SuppressWarnings("unchecked")
        Map<String, Object> sums = (Map<String, Object>) ism.aggregation();
        for (Map.Entry<String, Object> sum : sums.entrySet()) {
            if ("asc_total".equals(sum.getKey())) {
                assertEquals(25500.0, sum.getValue());
            } else if ("desc_total".equals(sum.getKey())) {
                assertEquals(25500.0, sum.getValue());
            } else {
                throw new IllegalArgumentException("unexpected scripted metric aggregation key [" + sum.getKey() + "]");
            }
        }
    }

    public void testBM25AndMultipleKnnWithAggregation() {
        float[] queryVectorAsc = { 500.0f };
        float[] queryVectorDesc = { 500.0f };
        KnnSearchBuilder knnSearchAsc = new KnnSearchBuilder("vector_asc", queryVectorAsc, 51, 1001, null);
        KnnSearchBuilder knnSearchDesc = new KnnSearchBuilder("vector_desc", queryVectorDesc, 51, 1001, null);
        SearchResponse response = client().prepareSearch("nrd_index")
            .setRankContextBuilder(new RRFRankBuilder().windowSize(51).rankConstant(1))
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
            .addAggregation(
                AggregationBuilders.scriptedMetric("sums")
                    .initScript(new Script("state['sums'] = ['asc': [], 'desc': [], 'text': []]"))
                    .mapScript(new Script("""
                        state['sums']['asc'].add($('vector_asc', null).getVector()[0]);
                        state['sums']['desc'].add($('vector_desc', null).getVector()[0]);
                        state['sums']['text'].add(Integer.parseInt($('text', null).substring(5)));
                        """))
                    .combineScript(new Script("""
                        return [
                            'asc_total': state['sums']['asc'].stream().mapToDouble(v -> v).sum(),
                            'desc_total': state['sums']['desc'].stream().mapToDouble(v -> v).sum(),
                            'text_total': state['sums']['text'].stream().mapToInt(v -> v).sum()
                        ]
                        """))
                    .reduceScript(new Script("""
                        return [
                            'asc_total': states.stream().mapToDouble(v -> v['asc_total']).sum(),
                            'desc_total': states.stream().mapToDouble(v -> v['desc_total']).sum(),
                            'text_total': states.stream().mapToInt(v -> v['text_total']).sum()
                        ]
                        """))
            )
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

        InternalScriptedMetric ism = response.getAggregations().get("sums");
        @SuppressWarnings("unchecked")
        Map<String, Object> sums = (Map<String, Object>) ism.aggregation();
        for (Map.Entry<String, Object> sum : sums.entrySet()) {
            if ("asc_total".equals(sum.getKey())) {
                assertEquals(25500.0, sum.getValue());
            } else if ("desc_total".equals(sum.getKey())) {
                assertEquals(25500.0, sum.getValue());
            } else if ("text_total".equals(sum.getKey())) {
                assertEquals(25500, sum.getValue());
            } else {
                throw new IllegalArgumentException("unexpected scripted metric aggregation key [" + sum.getKey() + "]");
            }
        }
    }*/
}

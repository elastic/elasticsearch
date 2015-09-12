/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class MinIT extends AbstractNumericTestCase {

    @Override
    @Test
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(min("min")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        Min min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(Double.POSITIVE_INFINITY));
    }

    @Override
    @Test
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(Double.POSITIVE_INFINITY));
    }

    @Override
    @Test
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testSingleValuedField_WithFormatter() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(min("min").format("0000.0").field("value")).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
        assertThat(min.getValueAsString(), equalTo("0001.0"));
    }

    @Override
    @Test
    public void testSingleValuedField_getProperty() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(min("min").field("value"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10l));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Min min = global.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        double expectedMinValue = 1.0;
        assertThat(min.getValue(), equalTo(expectedMinValue));
        assertThat((Min) global.getProperty("min"), equalTo(min));
        assertThat((double) global.getProperty("min.value"), equalTo(expectedMinValue));
        assertThat((double) min.getProperty("value"), equalTo(expectedMinValue));
    }

    @Override
    @Test
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Override
    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value").script(new Script("_value - 1")))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Override
    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value").script(new Script("_value - dec", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Override
    @Test
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));
    }

    @Override
    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values").script(new Script("_value - 1"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testMultiValuedField_WithValueScript_Reverse() throws Exception {
        // test what happens when values arrive in reverse order since the min
        // aggregator is optimized to work on sorted values
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(min("min").field("values").script(new Script("_value * -1"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(-12d));
    }

    @Override
    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(min("min").field("values").script(new Script("_value - dec", ScriptType.INLINE, null, params))).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Override
    @Test
    public void testScript_SingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(min("min").script(new Script("doc['value'].value"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Override
    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(min("min").script(new Script("doc['value'].value - dec", ScriptType.INLINE, null, params))).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Override
    @Test
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(min("min").script(new Script("doc['value'].value - dec", ScriptType.INLINE, null, params))).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Override
    @Test
    public void testScript_MultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(min("min").script(new Script("doc['values'].values"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));
    }

    @Override
    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(min("min").script(new Script("doc['values'].values"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));
    }

    @Override
    @Test
    public void testScript_MultiValued_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        SearchResponse searchResponse = client()
                .prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        min("min")
                                .script(new Script(
                                        "List values = doc['values'].values; double[] res = new double[values.size()]; for (int i = 0; i < res.length; i++) { res[i] = values.get(i) - dec; }; return res;",
                                        ScriptType.INLINE, null, params))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

}
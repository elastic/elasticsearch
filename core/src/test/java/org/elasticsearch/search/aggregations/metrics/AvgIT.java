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
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class AvgIT extends AbstractNumericTestCase {

    @Override
    @Test
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(avg("avg")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        Avg avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(Double.isNaN(avg.getValue()), is(true));
    }

    @Override
    @Test
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo(Double.NaN));
    }

    @Override
    @Test
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Override
    @Test
    public void testSingleValuedField_getProperty() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(avg("avg").field("value"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10l));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Avg avg = global.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        double expectedAvgValue = (double) (1+2+3+4+5+6+7+8+9+10) / 10;
        assertThat(avg.getValue(), equalTo(expectedAvgValue));
        assertThat((Avg) global.getProperty("avg"), equalTo(avg));
        assertThat((double) global.getProperty("avg.value"), equalTo(expectedAvgValue));
        assertThat((double) avg.getProperty("value"), equalTo(expectedAvgValue));
    }

    @Override
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Override
    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value").script(new Script("_value + 1")))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    @Override
    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value").script(new Script("_value + inc", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    public void testSingleValuedField_WithFormatter() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(avg("avg").format("#").field("value")).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(avg.getValueAsString(), equalTo("6"));
    }

    @Override
    @Test
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12) / 20));
    }

    @Override
    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values").script(new Script("_value + 1")))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13) / 20));
    }

    @Override
    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values").script(new Script("_value + inc", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13) / 20));
    }

    @Override
    @Test
    public void testScript_SingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script(new Script("doc['value'].value")))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Override
    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script(new Script("doc['value'].value + inc", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    @Override
    @Test
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script(new Script("doc['value'].value + inc", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    @Override
    @Test
    public void testScript_MultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script(new Script("[ doc['value'].value, doc['value'].value + 1 ]")))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11) / 20));
    }

    @Override
    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").script(new Script("[ doc['value'].value, doc['value'].value + 1 ]")))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11) / 20));
    }

    @Override
    @Test
    public void testScript_MultiValued_WithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        avg("avg").script(new Script("[ doc['value'].value, doc['value'].value + inc ]", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11) / 20));
    }
}
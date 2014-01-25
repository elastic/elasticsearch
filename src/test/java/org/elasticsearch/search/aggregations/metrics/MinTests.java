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
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class MinTests extends AbstractNumericTests {

    @Test
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(min("min")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBucketByKey(1l);
        assertThat(bucket, notNullValue());

        Min min = bucket.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(Double.POSITIVE_INFINITY));
    }

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

    @Test
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value").script("_value - 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("value").script("_value - dec").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Test
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));
    }

    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values").script("_value - 1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testMultiValuedField_WithValueScript_Reverse() throws Exception {
        // test what happens when values arrive in reverse order since the min aggregator is optimized to work on sorted values
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values").script("_value * -1"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(-12d));
    }

    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").field("values").script("_value - dec").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testScript_SingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['value'].value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['value'].value - dec").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Test
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['value'].value - dec").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(0.0));
    }

    @Test
    public void testScript_MultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['values'].values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));
    }

    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("doc['values'].values"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(2.0));
    }

    @Test
    public void testScript_MultiValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(min("min").script("List values = doc['values'].values; double[] res = new double[values.length]; for (int i = 0; i < res.length; i++) { res[i] = values.get(i) - dec; }; return res;").param("dec", 1))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Min min = searchResponse.getAggregations().get("min");
        assertThat(min, notNullValue());
        assertThat(min.getName(), equalTo("min"));
        assertThat(min.getValue(), equalTo(1.0));
    }

}
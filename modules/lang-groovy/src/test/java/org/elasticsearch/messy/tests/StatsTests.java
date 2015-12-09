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
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.AbstractNumericTestCase;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 *
 */
public class StatsTests extends AbstractNumericTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GroovyPlugin.class);
    }

    @Override
    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(stats("stats")))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        Stats stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getCount(), equalTo(0l));
        assertThat(stats.getSum(), equalTo(0.0));
        assertThat(stats.getMin(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(stats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(Double.isNaN(stats.getAvg()), is(true));
    }

    @Override
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo(Double.NaN));
        assertThat(stats.getMin(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(stats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(stats.getSum(), equalTo(0.0));
        assertThat(stats.getCount(), equalTo(0l));
    }

    @Override
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
    }

    public void testSingleValuedField_WithFormatter() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(stats("stats").format("0000.0").field("value")).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(stats.getAvgAsString(), equalTo("0005.5"));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMinAsString(), equalTo("0001.0"));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getMaxAsString(), equalTo("0010.0"));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(stats.getSumAsString(), equalTo("0055.0"));
        assertThat(stats.getCount(), equalTo(10l));
        assertThat(stats.getCountAsString(), equalTo("0010.0"));
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(stats("stats").field("value"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10l));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Stats stats = global.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        Stats statsFromProperty = (Stats) global.getProperty("stats");
        assertThat(statsFromProperty, notNullValue());
        assertThat(statsFromProperty, sameInstance(stats));
        double expectedAvgValue = (double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10;
        assertThat(stats.getAvg(), equalTo(expectedAvgValue));
        assertThat((double) global.getProperty("stats.avg"), equalTo(expectedAvgValue));
        double expectedMinValue = 1.0;
        assertThat(stats.getMin(), equalTo(expectedMinValue));
        assertThat((double) global.getProperty("stats.min"), equalTo(expectedMinValue));
        double expectedMaxValue = 10.0;
        assertThat(stats.getMax(), equalTo(expectedMaxValue));
        assertThat((double) global.getProperty("stats.max"), equalTo(expectedMaxValue));
        double expectedSumValue = 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10;
        assertThat(stats.getSum(), equalTo(expectedSumValue));
        assertThat((double) global.getProperty("stats.sum"), equalTo(expectedSumValue));
        long expectedCountValue = 10;
        assertThat(stats.getCount(), equalTo(expectedCountValue));
        assertThat((double) global.getProperty("stats.count"), equalTo((double) expectedCountValue));
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value").script(new Script("_value + 1")))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value").script(new Script("_value + inc", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Override
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("values"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12) / 20));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("values").script(new Script("_value - 1")))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11) / 20));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("values").script(new Script("_value - dec", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11) / 20));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script(new Script("doc['value'].value")))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script(new Script("doc['value'].value + inc", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script(new Script("doc['values'].values")))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12) / 20));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        stats("stats").script(
                                new Script("[ doc['value'].value, doc['value'].value - dec ]", ScriptType.INLINE, null, params)))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10+0+1+2+3+4+5+6+7+8+9) / 20));
        assertThat(stats.getMin(), equalTo(0.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10+0+1+2+3+4+5+6+7+8+9));
        assertThat(stats.getCount(), equalTo(20l));
    }


    private void assertShardExecutionState(SearchResponse response, int expectedFailures) throws Exception {
        ShardSearchFailure[] failures = response.getShardFailures();
        if (failures.length != expectedFailures) {
            for (ShardSearchFailure failure : failures) {
                logger.error("Shard Failure: {}", failure.reason(), failure.toString());
            }
            fail("Unexpected shard failures!");
        }
        assertThat("Not all shards are initialized", response.getSuccessfulShards(), equalTo(response.getTotalShards()));
    }
}
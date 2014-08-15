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
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class StatsTests extends AbstractNumericTests {

    @Test
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).minDocCount(0).subAggregation(stats("stats")))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBucketByKey(1l);
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

    @Test
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

    @Test
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Test
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Test
    public void testSingleValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value").script("_value + 1"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Test
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("value").script("_value + inc").param("inc", 1))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Test
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("values"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12) / 20));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Test
    public void testMultiValuedField_WithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("values").script("_value - 1"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11) / 20));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Test
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").field("values").script("_value - dec").param("dec", 1))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11) / 20));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10+2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Test
    public void testScript_SingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script("doc['value'].value"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Test
    public void testScript_SingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script("doc['value'].value + inc").param("inc", 1))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Test
    @TestLogging("search:TRACE")
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script("doc['value'].value + inc").param("inc", 1))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(11.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
        assertThat(stats.getCount(), equalTo(10l));
    }

    @Test
    public void testScript_MultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script("doc['values'].values"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12) / 20));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Test
    public void testScript_ExplicitMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script("doc['values'].values"))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12) / 20));
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2+3+4+5+6+7+8+9+10+11+3+4+5+6+7+8+9+10+11+12));
        assertThat(stats.getCount(), equalTo(20l));
    }

    @Test
    public void testScript_MultiValued_WithParams() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(stats("stats").script("[ doc['value'].value, doc['value'].value - dec ]").param("dec", 1))
                .execute().actionGet();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(10l));

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
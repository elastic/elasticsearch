/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationTestScriptsPlugin;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class StatsIT extends AbstractNumericTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(AggregationTestScriptsPlugin.class);
    }

    @Override
    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0).subAggregation(stats("stats").field("value")))
            .get();

        assertShardExecutionState(searchResponse, 0);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        Stats stats = bucket.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getCount(), equalTo(0L));
        assertThat(stats.getSum(), equalTo(0.0));
        assertThat(stats.getMin(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(stats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(Double.isNaN(stats.getAvg()), is(true));
    }

    @Override
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(stats("stats").field("value"))
            .get();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(stats.getAvg(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo(10.0));
        assertThat(stats.getSum(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(stats.getCount(), equalTo(10L));
    }

    public void testSingleValuedField_WithFormatter() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(stats("stats").format("0000.0").field("value"))
            .get();

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
        assertThat(stats.getCount(), equalTo(10L));
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(stats("stats").field("value")))
            .get();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10L));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Stats stats = global.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        Stats statsFromProperty = (Stats) ((InternalAggregation) global).getProperty("stats");
        assertThat(statsFromProperty, notNullValue());
        assertThat(statsFromProperty, sameInstance(stats));
        double expectedAvgValue = (double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10;
        assertThat(stats.getAvg(), equalTo(expectedAvgValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.avg"), equalTo(expectedAvgValue));
        double expectedMinValue = 1.0;
        assertThat(stats.getMin(), equalTo(expectedMinValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.min"), equalTo(expectedMinValue));
        double expectedMaxValue = 10.0;
        assertThat(stats.getMax(), equalTo(expectedMaxValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.max"), equalTo(expectedMaxValue));
        double expectedSumValue = 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10;
        assertThat(stats.getSum(), equalTo(expectedSumValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.sum"), equalTo(expectedSumValue));
        long expectedCountValue = 10;
        assertThat(stats.getCount(), equalTo(expectedCountValue));
        assertThat((double) ((InternalAggregation) global).getProperty("stats.count"), equalTo((double) expectedCountValue));
    }

    @Override
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(stats("stats").field("values"))
            .get();

        assertShardExecutionState(searchResponse, 0);

        assertHitCount(searchResponse, 10);

        Stats stats = searchResponse.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("stats"));
        assertThat(
            stats.getAvg(),
            equalTo((double) (2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12) / 20)
        );
        assertThat(stats.getMin(), equalTo(2.0));
        assertThat(stats.getMax(), equalTo(12.0));
        assertThat(stats.getSum(), equalTo((double) 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12));
        assertThat(stats.getCount(), equalTo(20L));
    }

    @Override
    public void testOrderByEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("terms").field("value")
                    .order(BucketOrder.compound(BucketOrder.aggregation("filter>stats.avg", true)))
                    .subAggregation(filter("filter", termQuery("value", 100)).subAggregation(stats("stats").field("value")))
            )
            .get();

        assertHitCount(searchResponse, 10);

        Terms terms = searchResponse.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets, notNullValue());
        assertThat(buckets.size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsNumber(), equalTo((long) i + 1));
            assertThat(bucket.getDocCount(), equalTo(1L));
            Filter filter = bucket.getAggregations().get("filter");
            assertThat(filter, notNullValue());
            assertThat(filter.getDocCount(), equalTo(0L));
            Stats stats = filter.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMin(), equalTo(Double.POSITIVE_INFINITY));
            assertThat(stats.getMax(), equalTo(Double.NEGATIVE_INFINITY));
            assertThat(stats.getAvg(), equalTo(Double.NaN));
            assertThat(stats.getSum(), equalTo(0.0));
            assertThat(stats.getCount(), equalTo(0L));

        }
    }

    private void assertShardExecutionState(SearchResponse response, int expectedFailures) throws Exception {
        ShardSearchFailure[] failures = response.getShardFailures();
        if (failures.length != expectedFailures) {
            for (ShardSearchFailure failure : failures) {
                logger.error(new ParameterizedMessage("Shard Failure: {}", failure), failure.getCause());
            }
            fail("Unexpected shard failures!");
        }
        assertThat("Not all shards are initialized", response.getSuccessfulShards(), equalTo(response.getTotalShards()));
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").addMapping("type", "d", "type=long")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get()
        );
        indexRandom(
            true,
            client().prepareIndex("cache_test_idx", "type", "1").setSource("s", 1),
            client().prepareIndex("cache_test_idx", "type", "2").setSource("s", 2)
        );

        // Make sure we are starting with a clear cache
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a nondeterministic script does not get cached
        SearchResponse r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                stats("foo").field("d")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "Math.random()", Collections.emptyMap()))
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                stats("foo").field("d")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value + 1", Collections.emptyMap()))
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(stats("foo").field("d")).get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(2L)
        );
    }
}

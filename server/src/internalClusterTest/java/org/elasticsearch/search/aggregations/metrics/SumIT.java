/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.hamcrest.core.IsNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.METRIC_SCRIPT_ENGINE;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.RANDOM_SCRIPT;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.VALUE_SCRIPT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SumIT extends AbstractNumericTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MetricAggScriptPlugin.class);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        super.setupSuiteScopeCluster();

        // Create two indices and add the field 'route_length_miles' as an alias in
        // one, and a concrete field in the other.
        prepareCreate("old_index").addMapping(
            "_doc",
            "transit_mode",
            "type=keyword",
            "distance",
            "type=double",
            "route_length_miles",
            "type=alias,path=distance"
        ).get();
        prepareCreate("new_index").addMapping("_doc", "transit_mode", "type=keyword", "route_length_miles", "type=double").get();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("old_index", "_doc").setSource("transit_mode", "train", "distance", 42.0));
        builders.add(client().prepareIndex("old_index", "_doc").setSource("transit_mode", "bus", "distance", 50.5));
        builders.add(client().prepareIndex("new_index", "_doc").setSource("transit_mode", "train", "route_length_miles", 100.2));

        indexRandom(true, builders);
        ensureSearchable();
    }

    @Override
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0).subAggregation(sum("sum").field("value")))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(0.0));
    }

    /** This test has been moved to {@link SumAggregatorTests#testUnmapped()} */
    @Override
    public void testUnmapped() throws Exception {}

    @Override
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").field("value"))
            .get();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
    }

    public void testSingleValuedFieldWithFormatter() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").format("0000.0").field("value"))
            .get();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(sum.getValueAsString(), equalTo("0055.0"));
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(sum("sum").field("value")))
            .get();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10L));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Sum sum = global.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        double expectedSumValue = (double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10;
        assertThat(sum.getValue(), equalTo(expectedSumValue));
        assertThat((Sum) ((InternalAggregation) global).getProperty("sum"), equalTo(sum));
        assertThat((double) ((InternalAggregation) global).getProperty("sum.value"), equalTo(expectedSumValue));
        assertThat((double) ((InternalAggregation) sum).getProperty("value"), equalTo(expectedSumValue));
    }

    @Override
    public void testMultiValuedField() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").field("values"))
            .get();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2 + 3 + 3 + 4 + 4 + 5 + 5 + 6 + 6 + 7 + 7 + 8 + 8 + 9 + 9 + 10 + 10 + 11 + 11 + 12));
    }

    @Override
    public void testOrderByEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("terms").field("value")
                    .order(BucketOrder.compound(BucketOrder.aggregation("filter>sum", true)))
                    .subAggregation(filter("filter", termQuery("value", 100)).subAggregation(sum("sum").field("value")))
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
            Sum sum = filter.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.value(), equalTo(0.0));

        }
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
                sum("foo").field("d").script(new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, RANDOM_SCRIPT, Collections.emptyMap()))
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
                sum("foo").field("d").script(new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, VALUE_SCRIPT, Collections.emptyMap()))
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
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(sum("foo").field("d")).get();
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

    public void testFieldAlias() {
        SearchResponse response = client().prepareSearch("old_index", "new_index")
            .addAggregation(sum("sum").field("route_length_miles"))
            .get();

        assertSearchResponse(response);

        Sum sum = response.getAggregations().get("sum");
        assertThat(sum, IsNull.notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(192.7));
    }

    public void testFieldAliasInSubAggregation() {
        SearchResponse response = client().prepareSearch("old_index", "new_index")
            .addAggregation(terms("terms").field("transit_mode").subAggregation(sum("sum").field("route_length_miles")))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));

        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        Terms.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("train"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(142.2));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("bus"));
        assertThat(bucket.getDocCount(), equalTo(1L));

        sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getValue(), equalTo(50.5));
    }
}

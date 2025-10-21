/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationTestScriptsPlugin;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentileRanks;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TDigestPercentileRanksIT extends AbstractNumericTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(AggregationTestScriptsPlugin.class);
    }

    private static double[] randomPercents(long minValue, long maxValue) {
        final int length = randomIntBetween(1, 20);
        final double[] percents = new double[length];
        for (int i = 0; i < percents.length; ++i) {
            percents[i] = switch (randomInt(20)) {
                case 0 -> minValue;
                case 1 -> maxValue;
                default -> (randomDouble() * (maxValue - minValue)) + minValue;
            };
        }
        Arrays.sort(percents);
        LogManager.getLogger(TDigestPercentileRanksIT.class).info("Using values={}", Arrays.toString(percents));
        return percents;
    }

    private static PercentileRanksAggregationBuilder randomCompression(PercentileRanksAggregationBuilder builder) {
        if (randomBoolean()) {
            builder.compression(randomIntBetween(20, 120) + randomDouble());
        }
        return builder;
    }

    private void assertConsistent(double[] pcts, PercentileRanks values, long minValue) {
        final List<Percentile> percentileList = CollectionUtils.iterableAsArrayList(values);
        assertEquals(pcts.length, percentileList.size());
        for (int i = 0; i < pcts.length; ++i) {
            final Percentile percentile = percentileList.get(i);
            assertThat(percentile.value(), equalTo(pcts[i]));
            assertThat(percentile.percent(), greaterThanOrEqualTo(0.0));
            assertThat(percentile.percent(), lessThanOrEqualTo(100.0));

            if (percentile.percent() == 0) {
                assertThat(percentile.value(), lessThanOrEqualTo((double) minValue));
            }
        }

        for (int i = 1; i < percentileList.size(); ++i) {
            assertThat(percentileList.get(i).value(), greaterThanOrEqualTo(percentileList.get(i - 1).value()));
        }
    }

    @Override
    public void testEmptyAggregation() throws Exception {
        assertResponse(
            prepareSearch("empty_bucket_idx").setQuery(matchAllQuery())
                .addAggregation(
                    histogram("histo").field("value")
                        .interval(1L)
                        .minDocCount(0)
                        .subAggregation(randomCompression(percentileRanks("percentile_ranks", new double[] { 10, 15 }).field("value")))
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                Histogram histo = response.getAggregations().get("histo");
                assertThat(histo, notNullValue());
                Histogram.Bucket bucket = histo.getBuckets().get(1);
                assertThat(bucket, notNullValue());

                PercentileRanks reversePercentiles = bucket.getAggregations().get("percentile_ranks");
                assertThat(reversePercentiles, notNullValue());
                assertThat(reversePercentiles.getName(), equalTo("percentile_ranks"));
                assertThat(reversePercentiles.percent(10), equalTo(Double.NaN));
                assertThat(reversePercentiles.percent(15), equalTo(Double.NaN));
            }
        );
    }

    public void testNullValuesField() throws Exception {
        final double[] pcts = null;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(percentileRanks("percentile_ranks", pcts).method(PercentilesMethod.TDIGEST).field("value"))
        );
        assertThat(e.getMessage(), equalTo("[values] must not be null: [percentile_ranks]"));
    }

    public void testEmptyValuesField() throws Exception {
        final double[] pcts = new double[0];
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(percentileRanks("percentile_ranks", pcts).method(PercentilesMethod.TDIGEST).field("value"))
        );
        assertThat(e.getMessage(), equalTo("[values] must not be an empty array: [percentile_ranks]"));
    }

    @Override
    public void testUnmapped() throws Exception {
        assertResponse(
            prepareSearch("idx_unmapped").setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks", new double[] { 0, 10, 15, 100 })).field("value")),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(0L));

                PercentileRanks reversePercentiles = response.getAggregations().get("percentile_ranks");
                assertThat(reversePercentiles, notNullValue());
                assertThat(reversePercentiles.getName(), equalTo("percentile_ranks"));
                assertThat(reversePercentiles.percent(0), equalTo(Double.NaN));
                assertThat(reversePercentiles.percent(10), equalTo(Double.NaN));
                assertThat(reversePercentiles.percent(15), equalTo(Double.NaN));
                assertThat(reversePercentiles.percent(100), equalTo(Double.NaN));
            }
        );
    }

    @Override
    public void testSingleValuedField() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("value")),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValue);
            }
        );
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    global("global").subAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("value"))
                ),
            response -> {
                assertHitCount(response, 10);

                SingleBucketAggregation global = response.getAggregations().get("global");
                assertThat(global, notNullValue());
                assertThat(global.getName(), equalTo("global"));
                assertThat(global.getDocCount(), equalTo(10L));
                assertThat(global.getAggregations(), notNullValue());
                assertThat(global.getAggregations().asList().size(), equalTo(1));

                PercentileRanks values = global.getAggregations().get("percentile_ranks");
                assertThat(values, notNullValue());
                assertThat(values.getName(), equalTo("percentile_ranks"));
                assertThat(((InternalAggregation) global).getProperty("percentile_ranks"), sameInstance(values));
            }
        );
    }

    public void testSingleValuedFieldOutsideRange() throws Exception {
        final double[] pcts = new double[] { minValue - 1, maxValue + 1 };
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("value")),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValue);
            }
        );
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        assertResponse(
            prepareSearch("idx", "idx_unmapped").setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("value")),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValue);
            }
        );
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    randomCompression(percentileRanks("percentile_ranks", pcts)).field("value")
                        .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
                ),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValue - 1);
            }
        );
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    randomCompression(percentileRanks("percentile_ranks", pcts)).field("value")
                        .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - dec", params))
                ),
            response -> {

                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValue - 1);
            }
        );
    }

    @Override
    public void testMultiValuedField() throws Exception {
        final double[] pcts = randomPercents(minValues, maxValues);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).field("values")),
            response -> {

                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValues);
            }
        );
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {
        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    randomCompression(percentileRanks("percentile_ranks", pcts)).field("values")
                        .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
                ),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValues - 1);
            }
        );
    }

    public void testMultiValuedFieldWithValueScriptReverse() throws Exception {
        final double[] pcts = randomPercents(-maxValues, -minValues);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    randomCompression(percentileRanks("percentile_ranks", pcts)).field("values")
                        .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value * -1", emptyMap()))
                ),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, -maxValues);
            }
        );
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);
        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    randomCompression(percentileRanks("percentile_ranks", pcts)).field("values")
                        .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - dec", params))
                ),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValues - 1);
            }
        );
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        final double[] pcts = randomPercents(minValue, maxValue);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    randomCompression(percentileRanks("percentile_ranks", pcts)).script(
                        new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value", emptyMap())
                    )
                ),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValue);
            }
        );
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("dec", 1);

        Script script = new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value - dec", params);

        final double[] pcts = randomPercents(minValue - 1, maxValue - 1);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).script(script)),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValue - 1);
            }
        );
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        final double[] pcts = randomPercents(minValues, maxValues);
        Script script = new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['values']", emptyMap());
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).script(script)),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValues);
            }
        );
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        Script script = AggregationTestScriptsPlugin.DECREMENT_ALL_VALUES;

        final double[] pcts = randomPercents(minValues - 1, maxValues - 1);
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(randomCompression(percentileRanks("percentile_ranks", pcts)).script(script)),
            response -> {
                assertHitCount(response, 10);

                final PercentileRanks values = response.getAggregations().get("percentile_ranks");
                assertConsistent(pcts, values, minValues - 1);
            }
        );
    }

    public void testOrderBySubAggregation() {
        boolean asc = randomBoolean();
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    histogram("histo").field("value")
                        .interval(2L)
                        .subAggregation(randomCompression(percentileRanks("percentile_ranks", new double[] { 99 }).field("value")))
                        .order(BucketOrder.aggregation("percentile_ranks", "99", asc))
                ),
            response -> {
                assertHitCount(response, 10);

                Histogram histo = response.getAggregations().get("histo");
                double previous = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                for (Histogram.Bucket bucket : histo.getBuckets()) {
                    PercentileRanks values = bucket.getAggregations().get("percentile_ranks");
                    double p99 = values.percent(99);
                    if (asc) {
                        assertThat(p99, greaterThanOrEqualTo(previous));
                    } else {
                        assertThat(p99, lessThanOrEqualTo(previous));
                    }
                    previous = p99;
                }
            }
        );
    }

    @Override
    public void testOrderByEmptyAggregation() throws Exception {
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    terms("terms").field("value")
                        .order(BucketOrder.compound(BucketOrder.aggregation("filter>ranks.99", true)))
                        .subAggregation(
                            filter("filter", termQuery("value", 100)).subAggregation(
                                percentileRanks("ranks", new double[] { 99 }).method(PercentilesMethod.TDIGEST).field("value")
                            )
                        )
                ),
            response -> {
                assertHitCount(response, 10);

                Terms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                List<? extends Terms.Bucket> buckets = terms.getBuckets();
                assertThat(buckets, notNullValue());
                assertThat(buckets.size(), equalTo(10));

                for (int i = 0; i < 10; i++) {
                    Terms.Bucket bucket = buckets.get(i);
                    assertThat(bucket, notNullValue());
                    assertThat(bucket.getKeyAsNumber(), equalTo((long) i + 1));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                    SingleBucketAggregation filter = bucket.getAggregations().get("filter");
                    assertThat(filter, notNullValue());
                    assertThat(filter.getDocCount(), equalTo(0L));
                    PercentileRanks ranks = filter.getAggregations().get("ranks");
                    assertThat(ranks, notNullValue());
                    assertThat(ranks.percent(99), equalTo(Double.NaN));

                }
            }
        );
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=long").setSettings(indexSettings(1, 1).put("requests.cache.enable", true))
        );
        indexRandom(
            true,
            prepareIndex("cache_test_idx").setId("1").setSource("s", 1),
            prepareIndex("cache_test_idx").setId("2").setSource("s", 2)
        );

        // Make sure we are starting with a clear cache
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a nondeterministic script does not get cached
        assertNoFailures(
            prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(
                    percentileRanks("foo", new double[] { 50.0 }).field("d")
                        .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "Math.random()", emptyMap()))
                )
        );

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        assertNoFailures(
            prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(
                    percentileRanks("foo", new double[] { 50.0 }).field("d")
                        .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
                )
        );

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        assertNoFailures(
            prepareSearch("cache_test_idx").setSize(0).addAggregation(percentileRanks("foo", new double[] { 50.0 }).field("d"))
        );

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(2L)
        );
    }
}

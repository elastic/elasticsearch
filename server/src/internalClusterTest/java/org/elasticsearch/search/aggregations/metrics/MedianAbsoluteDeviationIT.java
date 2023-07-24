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
import org.elasticsearch.search.aggregations.AggregationTestScriptsPlugin;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.range;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregatorTests.ExactMedianAbsoluteDeviation.calculateMAD;
import static org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregatorTests.IsCloseToRelative.closeToRelative;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class MedianAbsoluteDeviationIT extends AbstractNumericTestCase {

    private static final int MIN_SAMPLE_VALUE = -1000000;
    private static final int MAX_SAMPLE_VALUE = 1000000;
    private static final int NUMBER_OF_DOCS = 1000;
    private static final Supplier<Long> sampleSupplier = () -> randomLongBetween(MIN_SAMPLE_VALUE, MAX_SAMPLE_VALUE);

    private static long[] singleValueSample;
    private static long[] multiValueSample;
    private static double singleValueExactMAD;
    private static double multiValueExactMAD;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx", 1, 0);
        createIndex("idx_unmapped", 1, 0);

        minValue = MIN_SAMPLE_VALUE;
        minValues = MIN_SAMPLE_VALUE;
        maxValue = MAX_SAMPLE_VALUE;
        maxValues = MAX_SAMPLE_VALUE;

        singleValueSample = new long[NUMBER_OF_DOCS];
        multiValueSample = new long[NUMBER_OF_DOCS * 2];

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_DOCS; i++) {
            final long singleValueDatapoint = sampleSupplier.get();
            final long firstMultiValueDatapoint = sampleSupplier.get();
            final long secondMultiValueDatapoint = sampleSupplier.get();

            singleValueSample[i] = singleValueDatapoint;
            multiValueSample[i * 2] = firstMultiValueDatapoint;
            multiValueSample[(i * 2) + 1] = secondMultiValueDatapoint;

            IndexRequestBuilder builder = client().prepareIndex("idx")
                .setId(String.valueOf(i))
                .setSource(
                    jsonBuilder().startObject()
                        .field("value", singleValueDatapoint)
                        .startArray("values")
                        .value(firstMultiValueDatapoint)
                        .value(secondMultiValueDatapoint)
                        .endArray()
                        .endObject()
                );

            builders.add(builder);
        }

        singleValueExactMAD = calculateMAD(singleValueSample);
        multiValueExactMAD = calculateMAD(multiValueSample);

        indexRandom(true, builders);

        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").get();

        builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId(String.valueOf(i))
                    .setSource(jsonBuilder().startObject().field("value", i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(AggregationTestScriptsPlugin.class);
    }

    private static MedianAbsoluteDeviationAggregationBuilder randomBuilder() {
        final MedianAbsoluteDeviationAggregationBuilder builder = new MedianAbsoluteDeviationAggregationBuilder("mad");
        if (randomBoolean()) {
            builder.compression(randomDoubleBetween(25, 1000, false));
        }
        return builder;
    }

    @Override
    public void testEmptyAggregation() throws Exception {
        final SearchResponse response = client().prepareSearch("empty_bucket_idx")
            .addAggregation(histogram("histogram").field("value").interval(1).minDocCount(0).subAggregation(randomBuilder().field("value")))
            .get();

        assertHitCount(response, 2);

        final Histogram histogram = response.getAggregations().get("histogram");
        assertThat(histogram, notNullValue());
        final Histogram.Bucket bucket = histogram.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        final MedianAbsoluteDeviation mad = bucket.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));
        assertThat(mad.getMedianAbsoluteDeviation(), is(Double.NaN));
    }

    @Override
    public void testUnmapped() throws Exception {
        // Test moved to MedianAbsoluteDeviationAggregatorTests.testUnmapped()
    }

    @Override
    public void testSingleValuedField() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomBuilder().field("value"))
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(singleValueExactMAD));
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(global("global").subAggregation(randomBuilder().field("value")))
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final Global global = response.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), is("global"));
        assertThat(global.getDocCount(), is((long) NUMBER_OF_DOCS));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().entrySet(), hasSize(1));

        final MedianAbsoluteDeviation mad = global.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));
        assertThat(((InternalAggregation) global).getProperty("mad"), sameInstance(mad));
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        final SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .setQuery(matchAllQuery())
            .addAggregation(randomBuilder().field("value"))
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(singleValueExactMAD));
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomBuilder().field("value")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value + 1", Collections.emptyMap()))
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));

        final double fromIncrementedSampleMAD = calculateMAD(Arrays.stream(singleValueSample).map(point -> point + 1).toArray());
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(fromIncrementedSampleMAD));
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        final Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);

        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomBuilder().field("value")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value + inc", params))
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));

        final double fromIncrementedSampleMAD = calculateMAD(Arrays.stream(singleValueSample).map(point -> point + 1).toArray());
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(fromIncrementedSampleMAD));
    }

    @Override
    public void testMultiValuedField() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(randomBuilder().field("values"))
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(multiValueExactMAD));
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomBuilder().field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value + 1", Collections.emptyMap()))
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());

        final double fromIncrementedSampleMAD = calculateMAD(Arrays.stream(multiValueSample).map(point -> point + 1).toArray());
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(fromIncrementedSampleMAD));
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        final Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);

        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomBuilder().field("values")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value + inc", params))
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());

        final double fromIncrementedSampleMAD = calculateMAD(Arrays.stream(multiValueSample).map(point -> point + 1).toArray());
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(fromIncrementedSampleMAD));
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomBuilder().script(
                    new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value", Collections.emptyMap())
                )
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(singleValueExactMAD));
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        final Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);

        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomBuilder().script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['value'].value + inc", params))
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));

        final double fromIncrementedSampleMAD = calculateMAD(Arrays.stream(singleValueSample).map(point -> point + 1).toArray());
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(fromIncrementedSampleMAD));
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomBuilder().script(
                    new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "doc['values']", Collections.emptyMap())
                )
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(multiValueExactMAD));
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        final Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);

        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                randomBuilder().script(
                    new Script(
                        ScriptType.INLINE,
                        AggregationTestScriptsPlugin.NAME,
                        "[ doc['value'].value, doc['value'].value + inc ]",
                        params
                    )
                )
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final MedianAbsoluteDeviation mad = response.getAggregations().get("mad");
        assertThat(mad, notNullValue());
        assertThat(mad.getName(), is("mad"));

        final double fromIncrementedSampleMAD = calculateMAD(
            Arrays.stream(singleValueSample).flatMap(point -> LongStream.of(point, point + 1)).toArray()
        );
        assertThat(mad.getMedianAbsoluteDeviation(), closeToRelative(fromIncrementedSampleMAD));
    }

    public void testAsSubAggregation() throws Exception {
        final int rangeBoundary = (MAX_SAMPLE_VALUE + MIN_SAMPLE_VALUE) / 2;
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                range("range").field("value")
                    .addRange(MIN_SAMPLE_VALUE, rangeBoundary)
                    .addRange(rangeBoundary, MAX_SAMPLE_VALUE)
                    .subAggregation(randomBuilder().field("value"))
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final long[] lowerBucketSample = Arrays.stream(singleValueSample)
            .filter(point -> point >= MIN_SAMPLE_VALUE && point < rangeBoundary)
            .toArray();
        final long[] upperBucketSample = Arrays.stream(singleValueSample)
            .filter(point -> point >= rangeBoundary && point < MAX_SAMPLE_VALUE)
            .toArray();

        final Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        List<? extends Range.Bucket> buckets = range.getBuckets();
        assertThat(buckets, notNullValue());
        assertThat(buckets, hasSize(2));

        final Range.Bucket lowerBucket = buckets.get(0);
        assertThat(lowerBucket, notNullValue());

        final MedianAbsoluteDeviation lowerBucketMAD = lowerBucket.getAggregations().get("mad");
        assertThat(lowerBucketMAD, notNullValue());
        assertThat(lowerBucketMAD.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(lowerBucketSample)));

        final Range.Bucket upperBucket = buckets.get(1);
        assertThat(upperBucket, notNullValue());

        final MedianAbsoluteDeviation upperBucketMAD = upperBucket.getAggregations().get("mad");
        assertThat(upperBucketMAD, notNullValue());
        assertThat(upperBucketMAD.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(upperBucketSample)));

    }

    @Override
    public void testOrderByEmptyAggregation() throws Exception {
        final int numberOfBuckets = 10;
        final SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("terms").field("value")
                    .size(numberOfBuckets)
                    .order(BucketOrder.compound(BucketOrder.aggregation("filter>mad", true)))
                    .subAggregation(
                        filter("filter", termQuery("value", MAX_SAMPLE_VALUE + 1)).subAggregation(randomBuilder().field("value"))
                    )
            )
            .get();

        assertHitCount(response, NUMBER_OF_DOCS);

        final Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets, notNullValue());
        assertThat(buckets, hasSize(numberOfBuckets));

        for (int i = 0; i < numberOfBuckets; i++) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());

            Filter filter = bucket.getAggregations().get("filter");
            assertThat(filter, notNullValue());
            assertThat(filter.getDocCount(), equalTo(0L));

            MedianAbsoluteDeviation mad = filter.getAggregations().get("mad");
            assertThat(mad, notNullValue());
            assertThat(mad.getMedianAbsoluteDeviation(), equalTo(Double.NaN));
        }
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=long")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get()
        );

        indexRandom(
            true,
            client().prepareIndex("cache_test_idx").setId("1").setSource("s", 1),
            client().prepareIndex("cache_test_idx").setId("2").setSource("s", 2)
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
        SearchResponse r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                randomBuilder().field("d")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "Math.random()", emptyMap()))
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                randomBuilder().field("d")
                    .script(new Script(ScriptType.INLINE, AggregationTestScriptsPlugin.NAME, "_value - 1", emptyMap()))
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(randomBuilder().field("d")).get();
        assertSearchResponse(r);

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

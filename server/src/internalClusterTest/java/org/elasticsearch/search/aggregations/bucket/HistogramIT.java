/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import com.carrotsearch.hppc.LongHashSet;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.DoubleBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class HistogramIT extends ESIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final String MULTI_VALUED_FIELD_NAME = "l_values";

    static int numDocs;
    static int interval;
    static int numValueBuckets, numValuesBuckets;
    static long[] valueCounts, valuesCounts;
    static Map<Long, Map<String, Object>> expectedMultiSortBuckets;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("_value + 1", vars -> {
                double value = (double) vars.get("_value");
                return value + 1L;
            });

            scripts.put("doc['l_value'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return doc.get(SINGLE_VALUED_FIELD_NAME);
            });

            scripts.put("doc['l_values']", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return doc.get(MULTI_VALUED_FIELD_NAME);
            });

            return scripts;
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("Math.random()", vars -> HistogramIT.randomDouble());

            return scripts;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        numDocs = randomIntBetween(6, 20);
        interval = randomIntBetween(2, 5);

        numValueBuckets = numDocs / interval + 1;
        valueCounts = new long[numValueBuckets];
        for (int i = 0; i < numDocs; i++) {
            final int bucket = (i + 1) / interval;
            valueCounts[bucket]++;
        }

        numValuesBuckets = (numDocs + 1) / interval + 1;
        valuesCounts = new long[numValuesBuckets];
        for (int i = 0; i < numDocs; i++) {
            final int bucket1 = (i + 1) / interval;
            final int bucket2 = (i + 2) / interval;
            valuesCounts[bucket1]++;
            if (bucket1 != bucket2) {
                valuesCounts[bucket2]++;
            }
        }
        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            builders.add(
                client().prepareIndex("idx")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SINGLE_VALUED_FIELD_NAME, i + 1)
                            .startArray(MULTI_VALUED_FIELD_NAME)
                            .value(i + 1)
                            .value(i + 2)
                            .endArray()
                            .field("tag", "tag" + i)
                            .field("constant", 1)
                            .endObject()
                    )
            );
        }

        getMultiSortDocs(builders);

        assertAcked(prepareCreate("empty_bucket_idx").setMapping(SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId("" + i)
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    private void addExpectedBucket(long key, long docCount, double avg, double sum) {
        Map<String, Object> bucketProps = new HashMap<>();
        bucketProps.put("key", key);
        bucketProps.put("_count", docCount);
        bucketProps.put("avg_l", avg);
        bucketProps.put("sum_d", sum);
        expectedMultiSortBuckets.put(key, bucketProps);
    }

    private void getMultiSortDocs(List<IndexRequestBuilder> builders) throws IOException {
        expectedMultiSortBuckets = new HashMap<>();
        addExpectedBucket(1, 3, 1, 6);
        addExpectedBucket(2, 3, 2, 6);
        addExpectedBucket(3, 2, 3, 3);
        addExpectedBucket(4, 2, 3, 4);
        addExpectedBucket(5, 2, 5, 3);
        addExpectedBucket(6, 1, 5, 1);
        addExpectedBucket(7, 1, 5, 1);

        assertAcked(client().admin().indices().prepareCreate("sort_idx").setMapping(SINGLE_VALUED_FIELD_NAME, "type=double").get());
        for (int i = 1; i <= 3; i++) {
            builders.add(
                client().prepareIndex("sort_idx")
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 1).field("l", 1).field("d", i).endObject())
            );
            builders.add(
                client().prepareIndex("sort_idx")
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 2).field("l", 2).field("d", i).endObject())
            );
        }
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 3).field("l", 3).field("d", 1).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 3.8).field("l", 3).field("d", 2).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 4).field("l", 3).field("d", 1).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 4.4).field("l", 3).field("d", 3).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 5).field("l", 5).field("d", 1).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 5.1).field("l", 5).field("d", 2).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 6).field("l", 5).field("d", 1).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, 7).field("l", 5).field("d", 1).endObject())
        );
    }

    public void testSingleValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
        }
    }

    public void singleValuedField_withOffset() throws Exception {
        int interval1 = 10;
        int offset = 5;
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval1).offset(offset))
            .get();

        // from setup we have between 6 and 20 documents, each with value 1 in test field
        int expectedNumberOfBuckets = (offset >= (numDocs % interval + 1)) ? numValueBuckets : numValueBuckets + 1;
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(expectedNumberOfBuckets));

        // first bucket should start at -5, contain 4 documents
        Histogram.Bucket bucket = histo.getBuckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(-5L));
        assertThat(bucket.getDocCount(), equalTo(4L));

        // last bucket should have (numDocs % interval + 1) docs
        bucket = histo.getBuckets().get(0);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo(numDocs % interval1 + 5L));
        assertThat(bucket.getDocCount(), equalTo((numDocs % interval) + 1L));
    }

    /**
     * Shift buckets by random offset between [2..interval]. From setup we have 1 doc per values from 1..numdocs.
     * Special care needs to be taken for expecations on counts in first and last bucket.
     */
    public void testSingleValuedFieldWithRandomOffset() throws Exception {
        int offset = randomIntBetween(2, interval);
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).offset(offset))
            .get();
        assertSearchResponse(response);
        // shifting by offset>2 creates new extra bucket [0,offset-1]
        // if offset is >= number of values in original last bucket, that effect is canceled
        int expectedNumberOfBuckets = (offset >= (numDocs % interval + 1)) ? numValueBuckets : numValueBuckets + 1;

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(expectedNumberOfBuckets));

        int docsCounted = 0;
        for (int i = 0; i < expectedNumberOfBuckets; ++i) {
            Histogram.Bucket bucket = histo.getBuckets().get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) ((i - 1) * interval + offset)));
            if (i == 0) {
                // first bucket
                long expectedFirstBucketCount = offset - 1;
                assertThat(bucket.getDocCount(), equalTo(expectedFirstBucketCount));
                docsCounted += expectedFirstBucketCount;
            } else if (i < expectedNumberOfBuckets - 1) {
                assertThat(bucket.getDocCount(), equalTo((long) interval));
                docsCounted += interval;
            } else {
                assertThat(bucket.getDocCount(), equalTo((long) numDocs - docsCounted));
            }
        }
    }

    public void testSingleValuedFieldOrderedByKeyAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).order(BucketOrder.key(true)))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
        }
    }

    public void testsingleValuedFieldOrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).order(BucketOrder.key(false)))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(numValueBuckets - i - 1);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
        }
    }

    public void testSingleValuedFieldOrderedByCountAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).order(BucketOrder.count(true)))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        LongHashSet buckets = new LongHashSet();
        List<Histogram.Bucket> histoBuckets = new ArrayList<>(histo.getBuckets());
        long previousCount = Long.MIN_VALUE;
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = histoBuckets.get(i);
            assertThat(bucket, notNullValue());
            long key = ((Number) bucket.getKey()).longValue();
            assertEquals(0, key % interval);
            assertTrue(buckets.add(key));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[(int) (key / interval)]));
            assertThat(bucket.getDocCount(), greaterThanOrEqualTo(previousCount));
            previousCount = bucket.getDocCount();
        }
    }

    public void testSingleValuedFieldOrderedByCountDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).order(BucketOrder.count(false)))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        LongHashSet buckets = new LongHashSet();
        List<Histogram.Bucket> histoBuckets = new ArrayList<>(histo.getBuckets());
        long previousCount = Long.MAX_VALUE;
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = histoBuckets.get(i);
            assertThat(bucket, notNullValue());
            long key = ((Number) bucket.getKey()).longValue();
            assertEquals(0, key % interval);
            assertTrue(buckets.add(key));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[(int) (key / interval)]));
            assertThat(bucket.getDocCount(), lessThanOrEqualTo(previousCount));
            previousCount = bucket.getDocCount();
        }
    }

    public void testSingleValuedFieldWithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(interval)
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));
        assertThat(((InternalAggregation) histo).getProperty("_bucket_count"), equalTo(numValueBuckets));
        Object[] propertiesKeys = (Object[]) ((InternalAggregation) histo).getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) histo).getProperty("_count");
        Object[] propertiesCounts = (Object[]) ((InternalAggregation) histo).getProperty("sum.value");

        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
            assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            long s = 0;
            for (int j = 0; j < numDocs; ++j) {
                if ((j + 1) / interval == i) {
                    s += j + 1;
                }
            }
            assertThat(sum.getValue(), equalTo((double) s));
            assertEquals(propertiesKeys[i], (double) i * interval);
            assertThat(propertiesDocCounts[i], equalTo(valueCounts[i]));
            assertThat(propertiesCounts[i], equalTo((double) s));
        }
    }

    public void testSingleValuedFieldOrderedBySubAggregationAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(interval)
                    .order(BucketOrder.aggregation("sum", true))
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        LongHashSet visited = new LongHashSet();
        double previousSum = Double.NEGATIVE_INFINITY;
        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            long key = ((Number) bucket.getKey()).longValue();
            assertTrue(visited.add(key));
            int b = (int) (key / interval);
            assertThat(bucket.getDocCount(), equalTo(valueCounts[b]));
            assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            long s = 0;
            for (int j = 0; j < numDocs; ++j) {
                if ((j + 1) / interval == b) {
                    s += j + 1;
                }
            }
            assertThat(sum.getValue(), equalTo((double) s));
            assertThat(sum.getValue(), greaterThanOrEqualTo(previousSum));
            previousSum = s;
        }
    }

    public void testSingleValuedFieldOrderedBySubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(interval)
                    .order(BucketOrder.aggregation("sum", false))
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        LongHashSet visited = new LongHashSet();
        double previousSum = Double.POSITIVE_INFINITY;
        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            long key = ((Number) bucket.getKey()).longValue();
            assertTrue(visited.add(key));
            int b = (int) (key / interval);
            assertThat(bucket.getDocCount(), equalTo(valueCounts[b]));
            assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            long s = 0;
            for (int j = 0; j < numDocs; ++j) {
                if ((j + 1) / interval == b) {
                    s += j + 1;
                }
            }
            assertThat(sum.getValue(), equalTo((double) s));
            assertThat(sum.getValue(), lessThanOrEqualTo(previousSum));
            previousSum = s;
        }
    }

    public void testSingleValuedFieldOrderedByMultiValuedSubAggregationDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(interval)
                    .order(BucketOrder.aggregation("stats.sum", false))
                    .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        LongHashSet visited = new LongHashSet();
        double previousSum = Double.POSITIVE_INFINITY;

        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            long key = ((Number) bucket.getKey()).longValue();
            assertTrue(visited.add(key));
            int b = (int) (key / interval);
            assertThat(bucket.getDocCount(), equalTo(valueCounts[b]));
            assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            long s = 0;
            for (int j = 0; j < numDocs; ++j) {
                if ((j + 1) / interval == b) {
                    s += j + 1;
                }
            }
            assertThat(stats.getSum(), equalTo((double) s));
            assertThat(stats.getSum(), lessThanOrEqualTo(previousSum));
            previousSum = s;
        }
    }

    public void testSingleValuedFieldOrderedBySubAggregationDescDeepOrderPath() throws Exception {
        boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(interval)
                    .order(BucketOrder.aggregation("filter>max", asc))
                    .subAggregation(filter("filter", matchAllQuery()).subAggregation(max("max").field(SINGLE_VALUED_FIELD_NAME)))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        LongHashSet visited = new LongHashSet();
        double prevMax = asc ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            long key = ((Number) bucket.getKey()).longValue();
            assertTrue(visited.add(key));
            int b = (int) (key / interval);
            assertThat(bucket.getDocCount(), equalTo(valueCounts[b]));
            assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
            Filter filter = bucket.getAggregations().get("filter");
            assertThat(filter, notNullValue());
            assertThat(bucket.getDocCount(), equalTo(filter.getDocCount()));
            Max max = filter.getAggregations().get("max");
            assertThat(max, Matchers.notNullValue());
            assertThat(max.getValue(), asc ? greaterThanOrEqualTo(prevMax) : lessThanOrEqualTo(prevMax));
            prevMax = max.getValue();
        }
    }

    public void testSingleValuedFieldOrderedByTieBreaker() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(interval)
                    .order(BucketOrder.aggregation("max_constant", randomBoolean()))
                    .subAggregation(max("max_constant").field("constant"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValueBuckets));

        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
        }
    }

    public void testSingleValuedFieldOrderedByIllegalAgg() throws Exception {
        boolean asc = true;
        try {
            client().prepareSearch("idx")
                .addAggregation(
                    histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                        .interval(interval)
                        .order(BucketOrder.aggregation("inner_histo>avg", asc))
                        .subAggregation(
                            histogram("inner_histo").interval(interval)
                                .field(MULTI_VALUED_FIELD_NAME)
                                .subAggregation(avg("avg").field("value"))
                        )
                )
                .get();
            fail("Expected an exception");
        } catch (SearchPhaseExecutionException e) {
            ElasticsearchException[] rootCauses = e.guessRootCauses();
            if (rootCauses.length == 1) {
                ElasticsearchException rootCause = rootCauses[0];
                if (rootCause instanceof AggregationExecutionException) {
                    AggregationExecutionException aggException = (AggregationExecutionException) rootCause;
                    assertThat(aggException.getMessage(), Matchers.startsWith("Invalid aggregation order path"));
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value + 1", emptyMap()))
                    .interval(interval)
            )
            .get();

        assertSearchResponse(response);

        final int numBuckets = (numDocs + 1) / interval - 2 / interval + 1;
        final long[] counts = new long[(numDocs + 1) / interval + 1];
        for (int i = 0; i < numDocs; ++i) {
            ++counts[(i + 2) / interval];
        }

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numBuckets));

        for (int i = 0; i < numBuckets; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            int key = ((2 / interval) + i) * interval;
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) key));
            assertThat(bucket.getDocCount(), equalTo(counts[key / interval]));
        }
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(MULTI_VALUED_FIELD_NAME).interval(interval))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValuesBuckets));

        for (int i = 0; i < numValuesBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valuesCounts[i]));
        }
    }

    public void testMultiValuedFieldOrderedByKeyDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field(MULTI_VALUED_FIELD_NAME).interval(interval).order(BucketOrder.key(false)))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(numValuesBuckets));

        List<Histogram.Bucket> buckets = new ArrayList<>(histo.getBuckets());
        for (int i = 0; i < numValuesBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(numValuesBuckets - i - 1);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valuesCounts[i]));
        }
    }

    public void testMultiValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(MULTI_VALUED_FIELD_NAME)
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value + 1", emptyMap()))
                    .interval(interval)
            )
            .get();

        assertSearchResponse(response);

        final int numBuckets = (numDocs + 2) / interval - 2 / interval + 1;
        final long[] counts = new long[(numDocs + 2) / interval + 1];
        for (int i = 0; i < numDocs; ++i) {
            final int bucket1 = (i + 2) / interval;
            final int bucket2 = (i + 3) / interval;
            ++counts[bucket1];
            if (bucket1 != bucket2) {
                ++counts[bucket2];
            }
        }

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numBuckets));

        for (int i = 0; i < numBuckets; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            int key = ((2 / interval) + i) * interval;
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) key));
            assertThat(bucket.getDocCount(), equalTo(counts[key / interval]));
        }
    }

    public void testScriptSingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['l_value'].value", emptyMap()))
                    .interval(interval)
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
        }
    }

    public void testScriptMultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['l_values']", emptyMap()))
                    .interval(interval)
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValuesBuckets));

        for (int i = 0; i < numValuesBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valuesCounts[i]));
        }
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        assertThat(histo.getBuckets().size(), equalTo(0));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
        }
    }

    public void testPartiallyUnmappedWithExtendedBounds() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(interval)
                    .extendedBounds(-1 * 2 * interval, valueCounts.length * interval)
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets + 3));

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) -1 * 2 * interval));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) -1 * interval));
        assertThat(bucket.getDocCount(), equalTo(0L));

        for (int i = 2; i < numValueBuckets + 2; ++i) {
            bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) (i - 2) * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i - 2]));
        }
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(1L)
                    .minDocCount(0)
                    .subAggregation(histogram("sub_histo").field(SINGLE_VALUED_FIELD_NAME).interval(1L))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        List<? extends Bucket> buckets = histo.getBuckets();
        Histogram.Bucket bucket = buckets.get(1);
        assertThat(bucket, Matchers.notNullValue());

        histo = bucket.getAggregations().get("sub_histo");
        assertThat(histo, Matchers.notNullValue());
        assertThat(histo.getName(), equalTo("sub_histo"));
        assertThat(histo.getBuckets().isEmpty(), is(true));
    }

    public void testSingleValuedFieldWithExtendedBounds() throws Exception {
        int lastDataBucketKey = (numValueBuckets - 1) * interval;

        // randomizing the number of buckets on the min bound
        // (can sometimes fall within the data range, but more frequently will fall before the data range)
        int addedBucketsLeft = randomIntBetween(0, numValueBuckets);
        long boundsMinKey = addedBucketsLeft * interval;
        if (frequently()) {
            boundsMinKey = -boundsMinKey;
        } else {
            addedBucketsLeft = 0;
        }
        long boundsMin = boundsMinKey + randomIntBetween(0, interval - 1);

        // randomizing the number of buckets on the max bound
        // (can sometimes fall within the data range, but more frequently will fall after the data range)
        int addedBucketsRight = randomIntBetween(0, numValueBuckets);
        long boundsMaxKeyDelta = addedBucketsRight * interval;
        if (rarely()) {
            addedBucketsRight = 0;
            boundsMaxKeyDelta = -boundsMaxKeyDelta;
        }
        long boundsMaxKey = lastDataBucketKey + boundsMaxKeyDelta;
        long boundsMax = boundsMaxKey + randomIntBetween(0, interval - 1);

        // it could be that the random bounds.min we chose ended up greater than bounds.max - this should cause an
        // error
        boolean invalidBoundsError = boundsMin > boundsMax;

        // constructing the newly expected bucket list
        int bucketsCount = numValueBuckets + addedBucketsLeft + addedBucketsRight;
        long[] extendedValueCounts = new long[bucketsCount];
        System.arraycopy(valueCounts, 0, extendedValueCounts, addedBucketsLeft, valueCounts.length);

        SearchResponse response = null;
        try {
            response = client().prepareSearch("idx")
                .addAggregation(
                    histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                        .interval(interval)
                        .minDocCount(0)
                        .extendedBounds(boundsMin, boundsMax)
                )
                .get();

            if (invalidBoundsError) {
                fail("Expected an exception to be thrown when bounds.min is greater than bounds.max");
                return;
            }

        } catch (IllegalArgumentException e) {
            if (invalidBoundsError) {
                // expected
                return;
            } else {
                throw e;
            }
        }
        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(bucketsCount));

        long key = Math.min(boundsMinKey, 0);
        for (int i = 0; i < bucketsCount; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo(key));
            assertThat(bucket.getDocCount(), equalTo(extendedValueCounts[i]));
            key += interval;
        }
    }

    public void testEmptyWithExtendedBounds() throws Exception {
        int lastDataBucketKey = (numValueBuckets - 1) * interval;

        // randomizing the number of buckets on the min bound
        // (can sometimes fall within the data range, but more frequently will fall before the data range)
        int addedBucketsLeft = randomIntBetween(0, numValueBuckets);
        long boundsMinKey = addedBucketsLeft * interval;
        if (frequently()) {
            boundsMinKey = -boundsMinKey;
        } else {
            addedBucketsLeft = 0;
        }
        long boundsMin = boundsMinKey + randomIntBetween(0, interval - 1);

        // randomizing the number of buckets on the max bound
        // (can sometimes fall within the data range, but more frequently will fall after the data range)
        int addedBucketsRight = randomIntBetween(0, numValueBuckets);
        long boundsMaxKeyDelta = addedBucketsRight * interval;
        if (rarely()) {
            addedBucketsRight = 0;
            boundsMaxKeyDelta = -boundsMaxKeyDelta;
        }
        long boundsMaxKey = lastDataBucketKey + boundsMaxKeyDelta;
        long boundsMax = boundsMaxKey + randomIntBetween(0, interval - 1);

        // it could be that the random bounds.min we chose ended up greater than bounds.max - this should cause an
        // error
        boolean invalidBoundsError = boundsMin > boundsMax;

        // constructing the newly expected bucket list
        int bucketsCount = (int) ((boundsMaxKey - boundsMinKey) / interval) + 1;
        long[] extendedValueCounts = new long[valueCounts.length + addedBucketsLeft + addedBucketsRight];
        System.arraycopy(valueCounts, 0, extendedValueCounts, addedBucketsLeft, valueCounts.length);

        SearchResponse response = null;
        try {
            response = client().prepareSearch("idx")
                .setQuery(QueryBuilders.termQuery("foo", "bar"))
                .addAggregation(
                    histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                        .interval(interval)
                        .minDocCount(0)
                        .extendedBounds(boundsMin, boundsMax)
                )
                .get();

            if (invalidBoundsError) {
                fail("Expected an exception to be thrown when bounds.min is greater than bounds.max");
                return;
            }

        } catch (IllegalArgumentException e) {
            if (invalidBoundsError) {
                // expected
                return;
            } else {
                throw e;
            }
        }
        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(bucketsCount));

        long key = boundsMinKey;
        for (int i = 0; i < bucketsCount; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo(key));
            assertThat(bucket.getDocCount(), equalTo(0L));
            key += interval;
        }
    }

    /**
     * see issue #9634, negative interval in histogram should raise exception
     */
    public void testExeptionOnNegativerInterval() {
        try {
            client().prepareSearch("empty_bucket_idx")
                .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(-1).minDocCount(0))
                .get();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.toString(), containsString("[interval] must be >0 for histogram aggregation [histo]"));
        }
    }

    public void testDecimalIntervalAndOffset() throws Exception {
        assertAcked(prepareCreate("decimal_values").setMapping("d", "type=float").get());
        indexRandom(
            true,
            client().prepareIndex("decimal_values").setId("1").setSource("d", -0.6),
            client().prepareIndex("decimal_values").setId("2").setSource("d", 0.1)
        );

        SearchResponse r = client().prepareSearch("decimal_values")
            .addAggregation(histogram("histo").field("d").interval(0.7).offset(0.05))
            .get();
        assertSearchResponse(r);

        Histogram histogram = r.getAggregations().get("histo");
        List<? extends Bucket> buckets = histogram.getBuckets();
        assertEquals(2, buckets.size());
        assertEquals(-0.65, (double) buckets.get(0).getKey(), 0.01d);
        assertEquals(1, buckets.get(0).getDocCount());
        assertEquals(0.05, (double) buckets.get(1).getKey(), 0.01d);
        assertEquals(1, buckets.get(1).getDocCount());
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=float")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get()
        );
        indexRandom(
            true,
            client().prepareIndex("cache_test_idx").setId("1").setSource("d", -0.6),
            client().prepareIndex("cache_test_idx").setId("2").setSource("d", 0.1)
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
                histogram("histo").field("d")
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "Math.random()", emptyMap()))
                    .interval(0.7)
                    .offset(0.05)
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
                histogram("histo").field("d")
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value + 1", emptyMap()))
                    .interval(0.7)
                    .offset(0.05)
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
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(histogram("histo").field("d").interval(0.7).offset(0.05))
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
            equalTo(2L)
        );
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndKeyDesc() throws Exception {
        long[] expectedKeys = new long[] { 1, 2, 4, 3, 7, 6, 5 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true), BucketOrder.key(false));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndKeyAsc() throws Exception {
        long[] expectedKeys = new long[] { 1, 2, 3, 4, 5, 6, 7 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true), BucketOrder.key(true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationDescAndKeyAsc() throws Exception {
        long[] expectedKeys = new long[] { 5, 6, 7, 3, 4, 2, 1 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", false), BucketOrder.key(true));
    }

    public void testSingleValuedFieldOrderedByCountAscAndSingleValueSubAggregationAsc() throws Exception {
        long[] expectedKeys = new long[] { 6, 7, 3, 4, 5, 1, 2 };
        assertMultiSortResponse(expectedKeys, BucketOrder.count(true), BucketOrder.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscSingleValueSubAggregationAsc() throws Exception {
        long[] expectedKeys = new long[] { 6, 7, 3, 5, 4, 1, 2 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("sum_d", true), BucketOrder.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedByThreeCriteria() throws Exception {
        long[] expectedKeys = new long[] { 2, 1, 4, 5, 3, 6, 7 };
        assertMultiSortResponse(
            expectedKeys,
            BucketOrder.count(false),
            BucketOrder.aggregation("sum_d", false),
            BucketOrder.aggregation("avg_l", false)
        );
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAsCompound() throws Exception {
        long[] expectedKeys = new long[] { 1, 2, 3, 4, 5, 6, 7 };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true));
    }

    public void testInvalidBounds() {
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("empty_bucket_idx")
                .addAggregation(
                    histogram("histo").field(SINGLE_VALUED_FIELD_NAME).hardBounds(new DoubleBounds(0.0, 10.0)).extendedBounds(3, 20)
                )
                .get()
        );
        assertThat(e.toString(), containsString("Extended bounds have to be inside hard bounds, hard bounds"));

        e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("empty_bucket_idx")
                .addAggregation(
                    histogram("histo").field(SINGLE_VALUED_FIELD_NAME).hardBounds(new DoubleBounds(3.0, null)).extendedBounds(0, 20)
                )
                .get()
        );
        assertThat(e.toString(), containsString("Extended bounds have to be inside hard bounds, hard bounds"));
    }

    public void testHardBounds() throws Exception {
        assertAcked(prepareCreate("test").setMapping("d", "type=double").get());
        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource("d", -0.6),
            client().prepareIndex("test").setId("2").setSource("d", 0.5),
            client().prepareIndex("test").setId("3").setSource("d", 0.1)
        );

        SearchResponse r = client().prepareSearch("test")
            .addAggregation(histogram("histo").field("d").interval(0.1).hardBounds(new DoubleBounds(0.0, null)))
            .get();
        assertSearchResponse(r);

        Histogram histogram = r.getAggregations().get("histo");
        List<? extends Bucket> buckets = histogram.getBuckets();
        assertEquals(5, buckets.size());
        assertEquals(0.1, (double) buckets.get(0).getKey(), 0.01d);
        assertEquals(0.5, (double) buckets.get(4).getKey(), 0.01d);

        r = client().prepareSearch("test")
            .addAggregation(histogram("histo").field("d").interval(0.1).hardBounds(new DoubleBounds(null, 0.0)))
            .get();
        assertSearchResponse(r);

        histogram = r.getAggregations().get("histo");
        buckets = histogram.getBuckets();
        assertEquals(1, buckets.size());
        assertEquals(-0.6, (double) buckets.get(0).getKey(), 0.01d);

        r = client().prepareSearch("test")
            .addAggregation(histogram("histo").field("d").interval(0.1).hardBounds(new DoubleBounds(0.0, 0.3)))
            .get();
        assertSearchResponse(r);

        histogram = r.getAggregations().get("histo");
        buckets = histogram.getBuckets();
        assertEquals(1, buckets.size());
        assertEquals(0.1, (double) buckets.get(0).getKey(), 0.01d);

    }

    private void assertMultiSortResponse(long[] expectedKeys, BucketOrder... order) {
        SearchResponse response = client().prepareSearch("sort_idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                    .interval(1)
                    .order(BucketOrder.compound(order))
                    .subAggregation(avg("avg_l").field("l"))
                    .subAggregation(sum("sum_d").field("d"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histogram = response.getAggregations().get("histo");
        assertThat(histogram, notNullValue());
        assertThat(histogram.getName(), equalTo("histo"));
        assertThat(histogram.getBuckets().size(), equalTo(expectedKeys.length));

        int i = 0;
        for (Histogram.Bucket bucket : histogram.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(expectedKeys[i]));
            assertThat(bucket.getDocCount(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("_count")));
            Avg avg = bucket.getAggregations().get("avg_l");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("avg_l")));
            Sum sum = bucket.getAggregations().get("sum_d");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("sum_d")));
            i++;
        }
    }

    private long key(Histogram.Bucket bucket) {
        return ((Number) bucket.getKey()).longValue();
    }
}

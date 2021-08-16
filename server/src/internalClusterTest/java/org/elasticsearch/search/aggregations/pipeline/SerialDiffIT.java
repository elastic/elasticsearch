/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.diff;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class SerialDiffIT extends ESIntegTestCase {
    private static final String INTERVAL_FIELD = "l_value";
    private static final String VALUE_FIELD = "v_value";

    static int interval;
    static int numBuckets;
    static int lag;
    static BucketHelpers.GapPolicy gapPolicy;
    static ValuesSourceAggregationBuilder<? extends ValuesSourceAggregationBuilder<?>> metric;
    static List<PipelineAggregationHelperTests.MockBucket> mockHisto;

    static Map<String, ArrayList<Double>> testValues;

    enum MetricTarget {
        VALUE ("value"), COUNT("count");

        private final String name;

        MetricTarget(String s) {
            name = s;
        }

        @Override
        public String toString(){
            return name;
        }
    }

    private ValuesSourceAggregationBuilder<
        ? extends ValuesSourceAggregationBuilder<?>> randomMetric(String name, String field) {
        int rand = randomIntBetween(0,3);

        switch (rand) {
            case 0:
                return min(name).field(field);
            case 2:
                return max(name).field(field);
            case 3:
                return avg(name).field(field);
            default:
                return avg(name).field(field);
        }
    }

    private void assertValidIterators(Iterator<?> expectedBucketIter, Iterator<?> expectedCountsIter, Iterator<?> expectedValuesIter) {
        if (expectedBucketIter.hasNext() == false) {
            fail("`expectedBucketIter` iterator ended before `actual` iterator, size mismatch");
        }
        if (expectedCountsIter.hasNext() == false) {
            fail("`expectedCountsIter` iterator ended before `actual` iterator, size mismatch");
        }
        if (expectedValuesIter.hasNext() == false) {
            fail("`expectedValuesIter` iterator ended before `actual` iterator, size mismatch");
        }
    }

    private void assertBucketContents(Histogram.Bucket actual, Double expectedCount, Double expectedValue) {
        // This is a gap bucket
        SimpleValue countDiff = actual.getAggregations().get("diff_counts");
        if (expectedCount == null) {
            assertThat("[_count] diff is not null", countDiff, nullValue());
        } else {
            assertThat("[_count] diff is null", countDiff, notNullValue());
            assertThat("[_count] diff does not match expected [" + countDiff.value() + " vs " + expectedCount + "]",
                    countDiff.value(), closeTo(expectedCount, 0.1));
        }

        // This is a gap bucket
        SimpleValue valuesDiff = actual.getAggregations().get("diff_values");
        if (expectedValue == null) {
            assertThat("[value] diff is not null", valuesDiff, Matchers.nullValue());
        } else {
            assertThat("[value] diff is null", valuesDiff, notNullValue());
            assertThat("[value] diff does not match expected [" + valuesDiff.value() + " vs " + expectedValue + "]",
                    valuesDiff.value(), closeTo(expectedValue, 0.1));
        }
    }


    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        List<IndexRequestBuilder> builders = new ArrayList<>();


        interval = 5;
        numBuckets = randomIntBetween(10, 80);
        lag = randomIntBetween(1, numBuckets / 2);

        gapPolicy = randomFrom(BucketHelpers.GapPolicy.values());
        metric = randomMetric("the_metric", VALUE_FIELD);
        mockHisto = PipelineAggregationHelperTests.generateHistogram(interval, numBuckets, randomDouble(), randomDouble());

        testValues = new HashMap<>(8);

        for (MetricTarget target : MetricTarget.values()) {
            setupExpected(target);
        }

        for (PipelineAggregationHelperTests.MockBucket mockBucket : mockHisto) {
            for (double value : mockBucket.docValues) {
                builders.add(client().prepareIndex("idx").setSource(jsonBuilder().startObject()
                        .field(INTERVAL_FIELD, mockBucket.key)
                        .field(VALUE_FIELD, value).endObject()));
            }
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    /**
     * @param target    The document field "target", e.g. _count or a field value
     */
    private void setupExpected(MetricTarget target) {
        ArrayList<Double> values = new ArrayList<>(numBuckets);
        EvictingQueue<Double> lagWindow = new EvictingQueue<>(lag);

        int counter = 0;
        for (PipelineAggregationHelperTests.MockBucket mockBucket : mockHisto) {
            Double metricValue;
            double[] docValues = mockBucket.docValues;

            // Gaps only apply to metric values, not doc _counts
            if (mockBucket.count == 0 && target.equals(MetricTarget.VALUE)) {
                // If there was a gap in doc counts and we are ignoring, just skip this bucket
                if (gapPolicy.equals(BucketHelpers.GapPolicy.SKIP)) {
                    metricValue = null;
                } else if (gapPolicy.equals(BucketHelpers.GapPolicy.INSERT_ZEROS)) {
                    // otherwise insert a zero instead of the true value
                    metricValue = 0.0;
                } else {
                    metricValue = PipelineAggregationHelperTests.calculateMetric(docValues, metric);
                    if (gapPolicy.equals(BucketHelpers.GapPolicy.KEEP_VALUES)) {
                        if (Double.isInfinite(metricValue) || Double.isNaN(metricValue)) {
                            // serial diff ignores these values and replaces them with null
                            metricValue = Double.NaN;
                        }
                    }
                }

            } else {
                // If this isn't a gap, or is a _count, just insert the value
                metricValue = target.equals(MetricTarget.VALUE)
                    ? PipelineAggregationHelperTests.calculateMetric(docValues, metric)
                    : mockBucket.count;
            }

            counter += 1;

            // Still under the initial lag period, add nothing and move on
            Double lagValue;
            if (counter <= lag) {
                lagValue = Double.NaN;
            } else {
                lagValue = lagWindow.peek();  // Peek here, because we rely on add'ing to always move the window
            }

            // Normalize null's to NaN
            if (metricValue == null) {
                metricValue = Double.NaN;
            }

            // Both have values, calculate diff and replace the "empty" bucket
            if (Double.isNaN(metricValue) == false && Double.isNaN(lagValue) == false) {
                double diff = metricValue - lagValue;
                values.add(diff);
            } else {
                values.add(null);   // The tests need null, even though the agg doesn't
            }

            lagWindow.add(metricValue);
        }

        testValues.put(target.toString(), values);
    }

    public void testBasicDiff() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(0L, (long) (interval * (numBuckets - 1)))
                                .subAggregation(metric)
                                .subAggregation(diff("diff_counts", "_count")
                                        .lag(lag)
                                        .gapPolicy(gapPolicy))
                                .subAggregation(diff("diff_values", "the_metric")
                                        .lag(lag)
                                        .gapPolicy(gapPolicy))
                ).get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedCounts = testValues.get(MetricTarget.COUNT.toString());
        List<Double> expectedValues = testValues.get(MetricTarget.VALUE.toString());

        Iterator<? extends Histogram.Bucket> actualIter = buckets.iterator();
        Iterator<PipelineAggregationHelperTests.MockBucket> expectedBucketIter = mockHisto.iterator();
        Iterator<Double> expectedCountsIter = expectedCounts.iterator();
        Iterator<Double> expectedValuesIter = expectedValues.iterator();

        while (actualIter.hasNext()) {
            assertValidIterators(expectedBucketIter, expectedCountsIter, expectedValuesIter);

            Histogram.Bucket actual = actualIter.next();
            PipelineAggregationHelperTests.MockBucket expected = expectedBucketIter.next();
            Double expectedCount = expectedCountsIter.next();
            Double expectedValue = expectedValuesIter.next();

            assertThat("keys do not match", ((Number) actual.getKey()).longValue(), equalTo(expected.key));
            assertThat("doc counts do not match", actual.getDocCount(), equalTo((long)expected.count));

            assertBucketContents(actual, expectedCount, expectedValue);
        }
    }

    public void testInvalidLagSize() {
        try {
            client()
                    .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(0L, (long) (interval * (numBuckets - 1)))
                                .subAggregation(metric)
                                .subAggregation(diff("diff_counts", "_count")
                                        .lag(-1)
                                        .gapPolicy(gapPolicy))
                ).get();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("[lag] must be a positive integer: [diff_counts]"));
        }
    }
}

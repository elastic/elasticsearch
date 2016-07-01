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

package org.elasticsearch.search.aggregations.pipeline.moving.avg;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregationHelperTests;
import org.elasticsearch.search.aggregations.pipeline.SimpleValue;
import org.elasticsearch.search.aggregations.pipeline.derivative.Derivative;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.EwmaModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltLinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.LinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.search.aggregations.AggregationBuilders.range;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.derivative;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.movingAvg;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class MovAvgIT extends ESIntegTestCase {
    private static final String INTERVAL_FIELD = "l_value";
    private static final String VALUE_FIELD = "v_value";

    static int interval;
    static int numBuckets;
    static int windowSize;
    static double alpha;
    static double beta;
    static double gamma;
    static int period;
    static HoltWintersModel.SeasonalityType seasonalityType;
    static BucketHelpers.GapPolicy gapPolicy;
    static ValuesSourceAggregationBuilder<? extends ValuesSource, ? extends ValuesSourceAggregationBuilder<?, ?>> metric;
    static List<PipelineAggregationHelperTests.MockBucket> mockHisto;

    static Map<String, ArrayList<Double>> testValues;


    enum MovAvgType {
        SIMPLE ("simple"), LINEAR("linear"), EWMA("ewma"), HOLT("holt"), HOLT_WINTERS("holt_winters"), HOLT_BIG_MINIMIZE("holt");

        private final String name;

        MovAvgType(String s) {
            name = s;
        }

        @Override
        public String toString(){
            return name;
        }
    }

    enum MetricTarget {
        VALUE ("value"), COUNT("count"), METRIC("metric");

        private final String name;

        MetricTarget(String s) {
            name = s;
        }

        @Override
        public String toString(){
            return name;
        }
    }


    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        List<IndexRequestBuilder> builders = new ArrayList<>();


        interval = 5;
        numBuckets = randomIntBetween(6, 80);
        period = randomIntBetween(1, 5);
        windowSize = randomIntBetween(period * 2, 10);  // start must be 2*period to play nice with HW
        alpha = randomDouble();
        beta = randomDouble();
        gamma = randomDouble();
        seasonalityType = randomBoolean() ? HoltWintersModel.SeasonalityType.ADDITIVE : HoltWintersModel.SeasonalityType.MULTIPLICATIVE;


        gapPolicy = randomBoolean() ? BucketHelpers.GapPolicy.SKIP : BucketHelpers.GapPolicy.INSERT_ZEROS;
        metric = randomMetric("the_metric", VALUE_FIELD);
        mockHisto = PipelineAggregationHelperTests.generateHistogram(interval, numBuckets, randomDouble(), randomDouble());

        testValues = new HashMap<>(8);

        for (MovAvgType type : MovAvgType.values()) {
            for (MetricTarget target : MetricTarget.values()) {
                if (type.equals(MovAvgType.HOLT_BIG_MINIMIZE)) {
                    setupExpected(type, target, numBuckets);
                } else {
                    setupExpected(type, target, windowSize);
                }

            }
        }

        for (PipelineAggregationHelperTests.MockBucket mockBucket : mockHisto) {
            for (double value : mockBucket.docValues) {
                builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder().startObject()
                        .field(INTERVAL_FIELD, mockBucket.key)
                        .field(VALUE_FIELD, value).endObject()));
            }
        }

        for (int i = -10; i < 10; i++) {
            builders.add(client().prepareIndex("neg_idx", "type").setSource(
                    jsonBuilder().startObject().field(INTERVAL_FIELD, i).field(VALUE_FIELD, 10).endObject()));
        }

        for (int i = 0; i < 12; i++) {
            builders.add(client().prepareIndex("double_predict", "type").setSource(
                    jsonBuilder().startObject().field(INTERVAL_FIELD, i).field(VALUE_FIELD, 10).endObject()));
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    /**
     * Calculates the moving averages for a specific (model, target) tuple based on the previously generated mock histogram.
     * Computed values are stored in the testValues map.
     *
     * @param type      The moving average model to use
     * @param target    The document field "target", e.g. _count or a field value
     */
    private void setupExpected(MovAvgType type, MetricTarget target, int windowSize) {
        ArrayList<Double> values = new ArrayList<>(numBuckets);
        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);

        for (PipelineAggregationHelperTests.MockBucket mockBucket : mockHisto) {
            double metricValue;
            double[] docValues = mockBucket.docValues;

            // Gaps only apply to metric values, not doc _counts
            if (mockBucket.count == 0 && target.equals(MetricTarget.VALUE)) {
                // If there was a gap in doc counts and we are ignoring, just skip this bucket
                if (gapPolicy.equals(BucketHelpers.GapPolicy.SKIP)) {
                    values.add(null);
                    continue;
                } else if (gapPolicy.equals(BucketHelpers.GapPolicy.INSERT_ZEROS)) {
                    // otherwise insert a zero instead of the true value
                    metricValue = 0.0;
                } else {
                    metricValue = PipelineAggregationHelperTests.calculateMetric(docValues, metric);
                }

            } else {
                // If this isn't a gap, or is a _count, just insert the value
                metricValue = target.equals(MetricTarget.VALUE) ? PipelineAggregationHelperTests.calculateMetric(docValues, metric) : mockBucket.count;
            }

            if (window.size() > 0) {
                switch (type) {
                    case SIMPLE:
                        values.add(simple(window));
                        break;
                    case LINEAR:
                        values.add(linear(window));
                        break;
                    case EWMA:
                        values.add(ewma(window));
                        break;
                    case HOLT:
                        values.add(holt(window));
                        break;
                    case HOLT_BIG_MINIMIZE:
                        values.add(holt(window));
                        break;
                    case HOLT_WINTERS:
                        // HW needs at least 2 periods of data to start
                        if (window.size() >= period * 2) {
                            values.add(holtWinters(window));
                        } else {
                            values.add(null);
                        }

                        break;
                }
            } else {
                values.add(null);
            }

            window.offer(metricValue);

        }
        testValues.put(type.name() + "_" + target.name(), values);
    }

    /**
     * Simple, unweighted moving average
     *
     * @param window Window of values to compute movavg for
     */
    private double simple(Collection<Double> window) {
        double movAvg = 0;
        for (double value : window) {
            movAvg += value;
        }
        movAvg /= window.size();
        return movAvg;
    }

    /**
     * Linearly weighted moving avg
     *
     * @param window Window of values to compute movavg for
     */
    private double linear(Collection<Double> window) {
        double avg = 0;
        long totalWeight = 1;
        long current = 1;

        for (double value : window) {
            avg += value * current;
            totalWeight += current;
            current += 1;
        }
        return avg / totalWeight;
    }

    /**
     * Exponentially weighted (EWMA, Single exponential) moving avg
     *
     * @param window Window of values to compute movavg for
     */
    private double ewma(Collection<Double> window) {
        double avg = 0;
        boolean first = true;

        for (double value : window) {
            if (first) {
                avg = value;
                first = false;
            } else {
                avg = (value * alpha) + (avg * (1 - alpha));
            }
        }
        return avg;
    }

    /**
     * Holt-Linear (Double exponential) moving avg
     * @param window Window of values to compute movavg for
     */
    private double holt(Collection<Double> window) {
        double s = 0;
        double last_s = 0;

        // Trend value
        double b = 0;
        double last_b = 0;

        int counter = 0;

        double last;
        for (double value : window) {
            last = value;
            if (counter == 1) {
                s = value;
                b = value - last;
            } else {
                s = alpha * value + (1.0d - alpha) * (last_s + last_b);
                b = beta * (s - last_s) + (1 - beta) * last_b;
            }

            counter += 1;
            last_s = s;
            last_b = b;
        }

        return s + (0 * b) ;
    }

    /**
     * Holt winters (triple exponential) moving avg
     * @param window Window of values to compute movavg for
     */
    private double holtWinters(Collection<Double> window) {
        // Smoothed value
        double s = 0;
        double last_s = 0;

        // Trend value
        double b = 0;
        double last_b = 0;

        // Seasonal value
        double[] seasonal = new double[window.size()];

        double padding = seasonalityType.equals(HoltWintersModel.SeasonalityType.MULTIPLICATIVE) ? 0.0000000001 : 0;

        int counter = 0;
        double[] vs = new double[window.size()];
        for (double v : window) {
            vs[counter] = v + padding;
            counter += 1;
        }


        // Initial level value is average of first season
        // Calculate the slopes between first and second season for each period
        for (int i = 0; i < period; i++) {
            s += vs[i];
            b += (vs[i + period] - vs[i]) / period;
        }
        s /= period;
        b /= period;
        last_s = s;

        // Calculate first seasonal
        if (Double.compare(s, 0.0) == 0 || Double.compare(s, -0.0) == 0) {
            Arrays.fill(seasonal, 0.0);
        } else {
            for (int i = 0; i < period; i++) {
                seasonal[i] = vs[i] / s;
            }
        }

        for (int i = period; i < vs.length; i++) {
            if (seasonalityType.equals(HoltWintersModel.SeasonalityType.MULTIPLICATIVE)) {
                s = alpha * (vs[i] / seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            } else {
                s = alpha * (vs[i] - seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            }

            b = beta * (s - last_s) + (1 - beta) * last_b;

            if (seasonalityType.equals(HoltWintersModel.SeasonalityType.MULTIPLICATIVE)) {
                seasonal[i] = gamma * (vs[i] / (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            } else {
                seasonal[i] = gamma * (vs[i] - (last_s - last_b )) + (1 - gamma) * seasonal[i - period];
            }

            last_s = s;
            last_b = b;
        }

        int idx = window.size() - period + (0 % period);

        // TODO perhaps pad out seasonal to a power of 2 and use a mask instead of modulo?
        if (seasonalityType.equals(HoltWintersModel.SeasonalityType.MULTIPLICATIVE)) {
            return (s + (1 * b)) * seasonal[idx];
        } else {
            return s + (1 * b) + seasonal[idx];
        }
    }


    /**
     * test simple moving average on single value field
     */
    public void testSimpleSingleValuedField() {
        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                .subAggregation(metric)
                                .subAggregation(movingAvg("movavg_counts","_count")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy))
                                .subAggregation(movingAvg("movavg_values","the_metric")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedCounts = testValues.get(MovAvgType.SIMPLE.name() + "_" + MetricTarget.COUNT.name());
        List<Double> expectedValues = testValues.get(MovAvgType.SIMPLE.name() + "_" + MetricTarget.VALUE.name());

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

    public void testLinearSingleValuedField() {
        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                .subAggregation(metric)
                                .subAggregation(movingAvg("movavg_counts", "_count")
                                        .window(windowSize)
                                        .modelBuilder(new LinearModel.LinearModelBuilder())
                                        .gapPolicy(gapPolicy))
                                .subAggregation(movingAvg("movavg_values", "the_metric")
                                        .window(windowSize)
                                        .modelBuilder(new LinearModel.LinearModelBuilder())
                                        .gapPolicy(gapPolicy))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedCounts = testValues.get(MovAvgType.LINEAR.name() + "_" + MetricTarget.COUNT.name());
        List<Double> expectedValues = testValues.get(MovAvgType.LINEAR.name() + "_" + MetricTarget.VALUE.name());

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

    public void testEwmaSingleValuedField() {
        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                .subAggregation(metric)
                                .subAggregation(movingAvg("movavg_counts", "_count")
                                        .window(windowSize)
                                        .modelBuilder(new EwmaModel.EWMAModelBuilder().alpha(alpha))
                                        .gapPolicy(gapPolicy))
                                .subAggregation(movingAvg("movavg_values", "the_metric")
                                        .window(windowSize)
                                        .modelBuilder(new EwmaModel.EWMAModelBuilder().alpha(alpha))
                                        .gapPolicy(gapPolicy))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedCounts = testValues.get(MovAvgType.EWMA.name() + "_" + MetricTarget.COUNT.name());
        List<Double> expectedValues = testValues.get(MovAvgType.EWMA.name() + "_" + MetricTarget.VALUE.name());

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

    public void testHoltSingleValuedField() {
        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                .subAggregation(metric)
                                .subAggregation(movingAvg("movavg_counts", "_count")
                                        .window(windowSize)
                                        .modelBuilder(new HoltLinearModel.HoltLinearModelBuilder().alpha(alpha).beta(beta))
                                        .gapPolicy(gapPolicy))
                                .subAggregation(movingAvg("movavg_values", "the_metric")
                                        .window(windowSize)
                                        .modelBuilder(new HoltLinearModel.HoltLinearModelBuilder().alpha(alpha).beta(beta))
                                        .gapPolicy(gapPolicy))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedCounts = testValues.get(MovAvgType.HOLT.name() + "_" + MetricTarget.COUNT.name());
        List<Double> expectedValues = testValues.get(MovAvgType.HOLT.name() + "_" + MetricTarget.VALUE.name());

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

    public void testHoltWintersValuedField() {
        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                .subAggregation(metric)
                                .subAggregation(movingAvg("movavg_counts", "_count")
                                        .window(windowSize)
                                        .modelBuilder(new HoltWintersModel.HoltWintersModelBuilder()
                                                .alpha(alpha).beta(beta).gamma(gamma).period(period).seasonalityType(seasonalityType))
                                        .gapPolicy(gapPolicy)
                                        .minimize(false))
                                .subAggregation(movingAvg("movavg_values", "the_metric")
                                        .window(windowSize)
                                        .modelBuilder(new HoltWintersModel.HoltWintersModelBuilder()
                                                .alpha(alpha).beta(beta).gamma(gamma).period(period).seasonalityType(seasonalityType))
                                        .gapPolicy(gapPolicy)
                                        .minimize(false))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedCounts = testValues.get(MovAvgType.HOLT_WINTERS.name() + "_" + MetricTarget.COUNT.name());
        List<Double> expectedValues = testValues.get(MovAvgType.HOLT_WINTERS.name() + "_" + MetricTarget.VALUE.name());

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

    public void testPredictNegativeKeysAtStart() {

        SearchResponse response = client()
                .prepareSearch("neg_idx")
                .setTypes("type")
                .addAggregation(
                        histogram("histo")
                                .field(INTERVAL_FIELD)
                                .interval(1)
                                .subAggregation(avg("avg").field(VALUE_FIELD))
                                .subAggregation(
                                        movingAvg("movavg_values", "avg").window(windowSize).modelBuilder(new SimpleModel.SimpleModelBuilder())
                                                .gapPolicy(gapPolicy).predict(5))).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(25));

        SimpleValue current = buckets.get(0).getAggregations().get("movavg_values");
        assertThat(current, nullValue());

        for (int i = 1; i < 20; i++) {
            Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((long) bucket.getKey(), equalTo((long) i - 10));
            assertThat(bucket.getDocCount(), equalTo(1L));
            Avg avgAgg = bucket.getAggregations().get("avg");
            assertThat(avgAgg, notNullValue());
            assertThat(avgAgg.value(), equalTo(10d));
            SimpleValue movAvgAgg = bucket.getAggregations().get("movavg_values");
            assertThat(movAvgAgg, notNullValue());
            assertThat(movAvgAgg.value(), equalTo(10d));
        }

        for (int i = 20; i < 25; i++) {
            Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((long) bucket.getKey(), equalTo((long) i - 10));
            assertThat(bucket.getDocCount(), equalTo(0L));
            Avg avgAgg = bucket.getAggregations().get("avg");
            assertThat(avgAgg, nullValue());
            SimpleValue movAvgAgg = bucket.getAggregations().get("movavg_values");
            assertThat(movAvgAgg, notNullValue());
            assertThat(movAvgAgg.value(), equalTo(10d));
        }
    }

    public void testSizeZeroWindow() {
        try {
            client()
                    .prepareSearch("idx").setTypes("type")
                    .addAggregation(
                            histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                    .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                    .subAggregation(randomMetric("the_metric", VALUE_FIELD))
                                    .subAggregation(movingAvg("movavg_counts", "the_metric")
                                            .window(0)
                                            .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                            .gapPolicy(gapPolicy))
                    ).execute().actionGet();
            fail("MovingAvg should not accept a window that is zero");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("[window] must be a positive integer: [movavg_counts]"));
        }
    }

    public void testBadParent() {
        try {
            client()
                    .prepareSearch("idx").setTypes("type")
                    .addAggregation(
                            range("histo").field(INTERVAL_FIELD).addRange(0, 10)
                                    .subAggregation(randomMetric("the_metric", VALUE_FIELD))
                                    .subAggregation(movingAvg("movavg_counts", "the_metric")
                                            .window(windowSize)
                                            .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                            .gapPolicy(gapPolicy))
                    ).execute().actionGet();
            fail("MovingAvg should not accept non-histogram as parent");

        } catch (SearchPhaseExecutionException exception) {
            // All good
        }
    }

    public void testNegativeWindow() {
        try {
            client()
                    .prepareSearch("idx").setTypes("type")
                    .addAggregation(
                            histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                    .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                    .subAggregation(randomMetric("the_metric", VALUE_FIELD))
                                    .subAggregation(movingAvg("movavg_counts", "_count")
                                            .window(-10)
                                            .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                            .gapPolicy(gapPolicy))
                    ).execute().actionGet();
            fail("MovingAvg should not accept a window that is negative");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("[window] must be a positive integer: [movavg_counts]"));
        }
    }

    public void testNoBucketsInHistogram() {

        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field("test").interval(interval)
                                .subAggregation(randomMetric("the_metric", VALUE_FIELD))
                                .subAggregation(movingAvg("movavg_counts", "the_metric")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(0));
    }

    public void testNoBucketsInHistogramWithPredict() {
        int numPredictions = randomIntBetween(1,10);
        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field("test").interval(interval)
                                .subAggregation(randomMetric("the_metric", VALUE_FIELD))
                                .subAggregation(movingAvg("movavg_counts", "the_metric")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .predict(numPredictions))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(0));
    }

    public void testZeroPrediction() {
        try {
            client()
                    .prepareSearch("idx").setTypes("type")
                    .addAggregation(
                            histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                    .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                    .subAggregation(randomMetric("the_metric", VALUE_FIELD))
                                    .subAggregation(movingAvg("movavg_counts", "the_metric")
                                            .window(windowSize)
                                            .modelBuilder(randomModelBuilder())
                                            .gapPolicy(gapPolicy)
                                            .predict(0))
                    ).execute().actionGet();
            fail("MovingAvg should not accept a prediction size that is zero");

        } catch (IllegalArgumentException exception) {
            // All Good
        }
    }

    public void testNegativePrediction() {
        try {
            client()
                    .prepareSearch("idx").setTypes("type")
                    .addAggregation(
                            histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                    .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                    .subAggregation(randomMetric("the_metric", VALUE_FIELD))
                                    .subAggregation(movingAvg("movavg_counts", "the_metric")
                                            .window(windowSize)
                                            .modelBuilder(randomModelBuilder())
                                            .gapPolicy(gapPolicy)
                                            .predict(-10))
                    ).execute().actionGet();
            fail("MovingAvg should not accept a prediction size that is negative");

        } catch (IllegalArgumentException exception) {
            // All Good
        }
    }

    public void testHoltWintersNotEnoughData() {
        try {
            SearchResponse response = client()
                    .prepareSearch("idx").setTypes("type")
                    .addAggregation(
                            histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                    .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                    .subAggregation(metric)
                                    .subAggregation(movingAvg("movavg_counts", "_count")
                                            .window(10)
                                            .modelBuilder(new HoltWintersModel.HoltWintersModelBuilder()
                                                    .alpha(alpha).beta(beta).gamma(gamma).period(20).seasonalityType(seasonalityType))
                                            .gapPolicy(gapPolicy))
                                    .subAggregation(movingAvg("movavg_values", "the_metric")
                                            .window(windowSize)
                                            .modelBuilder(new HoltWintersModel.HoltWintersModelBuilder()
                                                    .alpha(alpha).beta(beta).gamma(gamma).period(20).seasonalityType(seasonalityType))
                                            .gapPolicy(gapPolicy))
                    ).execute().actionGet();
        } catch (SearchPhaseExecutionException e) {
            // All good
        }

    }

    public void testTwoMovAvgsWithPredictions() {
        SearchResponse response = client()
                .prepareSearch("double_predict")
                .setTypes("type")
                .addAggregation(
                        histogram("histo")
                                .field(INTERVAL_FIELD)
                                .interval(1)
                                .subAggregation(avg("avg").field(VALUE_FIELD))
                                .subAggregation(derivative("deriv", "avg").gapPolicy(gapPolicy))
                                .subAggregation(
                                        movingAvg("avg_movavg", "avg").window(windowSize).modelBuilder(new SimpleModel.SimpleModelBuilder())
                                                .gapPolicy(gapPolicy).predict(12))
                                .subAggregation(
                                        movingAvg("deriv_movavg", "deriv").window(windowSize).modelBuilder(new SimpleModel.SimpleModelBuilder())
                                                .gapPolicy(gapPolicy).predict(12))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(24));

        Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((long) bucket.getKey(), equalTo((long) 0));
        assertThat(bucket.getDocCount(), equalTo(1L));

        Avg avgAgg = bucket.getAggregations().get("avg");
        assertThat(avgAgg, notNullValue());
        assertThat(avgAgg.value(), equalTo(10d));

        SimpleValue movAvgAgg = bucket.getAggregations().get("avg_movavg");
        assertThat(movAvgAgg, nullValue());

        Derivative deriv = bucket.getAggregations().get("deriv");
        assertThat(deriv, nullValue());

        SimpleValue derivMovAvg = bucket.getAggregations().get("deriv_movavg");
        assertThat(derivMovAvg, nullValue());

        // Second bucket
        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((long) bucket.getKey(), equalTo(1L));
        assertThat(bucket.getDocCount(), equalTo(1L));

        avgAgg = bucket.getAggregations().get("avg");
        assertThat(avgAgg, notNullValue());
        assertThat(avgAgg.value(), equalTo(10d));

        deriv = bucket.getAggregations().get("deriv");
        assertThat(deriv, notNullValue());
        assertThat(deriv.value(), equalTo(0d));

        movAvgAgg = bucket.getAggregations().get("avg_movavg");
        assertThat(movAvgAgg, notNullValue());
        assertThat(movAvgAgg.value(), equalTo(10d));

        derivMovAvg = bucket.getAggregations().get("deriv_movavg");
        assertThat(derivMovAvg, Matchers.nullValue());                 // still null because of movavg delay

        for (int i = 2; i < 12; i++) {
            bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((long) bucket.getKey(), equalTo((long) i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            avgAgg = bucket.getAggregations().get("avg");
            assertThat(avgAgg, notNullValue());
            assertThat(avgAgg.value(), equalTo(10d));

            deriv = bucket.getAggregations().get("deriv");
            assertThat(deriv, notNullValue());
            assertThat(deriv.value(), equalTo(0d));

            movAvgAgg = bucket.getAggregations().get("avg_movavg");
            assertThat(movAvgAgg, notNullValue());
            assertThat(movAvgAgg.value(), equalTo(10d));

            derivMovAvg = bucket.getAggregations().get("deriv_movavg");
            assertThat(derivMovAvg, notNullValue());
            assertThat(derivMovAvg.value(), equalTo(0d));
        }

        // Predictions
        for (int i = 12; i < 24; i++) {
            bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((long) bucket.getKey(), equalTo((long) i));
            assertThat(bucket.getDocCount(), equalTo(0L));

            avgAgg = bucket.getAggregations().get("avg");
            assertThat(avgAgg, nullValue());

            deriv = bucket.getAggregations().get("deriv");
            assertThat(deriv, nullValue());

            movAvgAgg = bucket.getAggregations().get("avg_movavg");
            assertThat(movAvgAgg, notNullValue());
            assertThat(movAvgAgg.value(), equalTo(10d));

            derivMovAvg = bucket.getAggregations().get("deriv_movavg");
            assertThat(derivMovAvg, notNullValue());
            assertThat(derivMovAvg.value(), equalTo(0d));
        }
    }

    public void testBadModelParams() {
        try {
            SearchResponse response = client()
                    .prepareSearch("idx").setTypes("type")
                    .addAggregation(
                            histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                    .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                    .subAggregation(metric)
                                    .subAggregation(movingAvg("movavg_counts", "_count")
                                            .window(10)
                                            .modelBuilder(randomModelBuilder(100))
                                            .gapPolicy(gapPolicy))
                    ).execute().actionGet();
        } catch (SearchPhaseExecutionException e) {
            // All good
        }

    }

    public void testHoltWintersMinimization() {
        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                .subAggregation(metric)
                                .subAggregation(movingAvg("movavg_counts", "_count")
                                        .window(windowSize)
                                        .modelBuilder(new HoltWintersModel.HoltWintersModelBuilder()
                                                .period(period).seasonalityType(seasonalityType))
                                        .gapPolicy(gapPolicy)
                                        .minimize(true))
                                .subAggregation(movingAvg("movavg_values", "the_metric")
                                        .window(windowSize)
                                        .modelBuilder(new HoltWintersModel.HoltWintersModelBuilder()
                                                .period(period).seasonalityType(seasonalityType))
                                        .gapPolicy(gapPolicy)
                                        .minimize(true))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));


        List<Double> expectedCounts = testValues.get(MovAvgType.HOLT_WINTERS.name() + "_" + MetricTarget.COUNT.name());
        List<Double> expectedValues = testValues.get(MovAvgType.HOLT_WINTERS.name() + "_" + MetricTarget.VALUE.name());

        Iterator<? extends Histogram.Bucket> actualIter = buckets.iterator();
        Iterator<PipelineAggregationHelperTests.MockBucket> expectedBucketIter = mockHisto.iterator();
        Iterator<Double> expectedCountsIter = expectedCounts.iterator();
        Iterator<Double> expectedValueIter = expectedValues.iterator();

        // The minimizer is stochastic, so just make sure all the values coming back aren't null
        while (actualIter.hasNext()) {

            Histogram.Bucket actual = actualIter.next();
            PipelineAggregationHelperTests.MockBucket expected = expectedBucketIter.next();
            Double expectedCount = expectedCountsIter.next();
            Double expectedValue = expectedValueIter.next();

            assertThat("keys do not match", ((Number) actual.getKey()).longValue(), equalTo(expected.key));
            assertThat("doc counts do not match", actual.getDocCount(), equalTo((long)expected.count));

            SimpleValue countMovAvg = actual.getAggregations().get("movavg_counts");
            SimpleValue valuesMovAvg = actual.getAggregations().get("movavg_values");

            if (expectedCount == null) {
                //this bucket wasn't supposed to have a value (empty, skipped, etc), so
                //movavg should be null too
                assertThat(countMovAvg, nullValue());
            } else {

                // Note that we don't compare against the mock values, since those are assuming
                // a non-minimized set of coefficients.  Just check for not-nullness
                assertThat(countMovAvg, notNullValue());
            }

            if (expectedValue == null) {
                //this bucket wasn't supposed to have a value (empty, skipped, etc), so
                //movavg should be null too
                assertThat(valuesMovAvg, nullValue());
            } else {

                // Note that we don't compare against the mock values, since those are assuming
                // a non-minimized set of coefficients.  Just check for not-nullness
                assertThat(valuesMovAvg, notNullValue());
            }
        }

    }


    /**
     * If the minimizer is turned on, but there isn't enough data to minimize with, it will simply use
     * the default settings.  Which means our mock histo will match the generated result (which it won't
     * if the minimizer is actually working, since the coefficients will be different and thus generate different
     * data)
     *
     * We can simulate this by setting the window size == size of histo
     */
    public void testMinimizeNotEnoughData() {
        SearchResponse response = client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                .subAggregation(metric)
                                .subAggregation(movingAvg("movavg_counts", "_count")
                                        .window(numBuckets)
                                        .modelBuilder(new HoltLinearModel.HoltLinearModelBuilder().alpha(alpha).beta(beta))
                                        .gapPolicy(gapPolicy)
                                        .minimize(true))
                                .subAggregation(movingAvg("movavg_values", "the_metric")
                                        .window(numBuckets)
                                        .modelBuilder(new HoltLinearModel.HoltLinearModelBuilder().alpha(alpha).beta(beta))
                                        .gapPolicy(gapPolicy)
                                        .minimize(true))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat("Size of buckets array is not correct.", buckets.size(), equalTo(mockHisto.size()));

        List<Double> expectedCounts = testValues.get(MovAvgType.HOLT_BIG_MINIMIZE.name() + "_" + MetricTarget.COUNT.name());
        List<Double> expectedValues = testValues.get(MovAvgType.HOLT_BIG_MINIMIZE.name() + "_" + MetricTarget.VALUE.name());

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

    /**
     * Only some models can be minimized, should throw exception for: simple, linear
     */
    public void testCheckIfNonTunableCanBeMinimized() {
        try {
            client()
                .prepareSearch("idx").setTypes("type")
                .addAggregation(
                        histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                .subAggregation(metric)
                                .subAggregation(movingAvg("movavg_counts", "_count")
                                        .window(numBuckets)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .minimize(true))
                ).execute().actionGet();
            fail("Simple Model cannot be minimized, but an exception was not thrown");
        } catch (SearchPhaseExecutionException e) {
            // All good
        }

        try {
            client()
                    .prepareSearch("idx").setTypes("type")
                    .addAggregation(
                            histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                    .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                    .subAggregation(metric)
                                    .subAggregation(movingAvg("movavg_counts", "_count")
                                            .window(numBuckets)
                                            .modelBuilder(new LinearModel.LinearModelBuilder())
                                            .gapPolicy(gapPolicy)
                                            .minimize(true))
                    ).execute().actionGet();
            fail("Linear Model cannot be minimized, but an exception was not thrown");
        } catch (SearchPhaseExecutionException e) {
            // all good
        }
    }

    /**
     * These models are all minimizable, so they should not throw exceptions
     */
    public void testCheckIfTunableCanBeMinimized() {
        MovAvgModelBuilder[] builders = new MovAvgModelBuilder[]{
                new EwmaModel.EWMAModelBuilder(),
                new HoltLinearModel.HoltLinearModelBuilder(),
                new HoltWintersModel.HoltWintersModelBuilder()
        };

        for (MovAvgModelBuilder builder : builders) {
            try {
                client()
                        .prepareSearch("idx").setTypes("type")
                        .addAggregation(
                                histogram("histo").field(INTERVAL_FIELD).interval(interval)
                                        .extendedBounds(new ExtendedBounds(0L, (long) (interval * (numBuckets - 1))))
                                        .subAggregation(metric)
                                        .subAggregation(movingAvg("movavg_counts", "_count")
                                                .window(numBuckets)
                                                .modelBuilder(builder)
                                                .gapPolicy(gapPolicy)
                                                .minimize(true))
                        ).execute().actionGet();
            } catch (SearchPhaseExecutionException e) {
                fail("Model [" + builder.toString() + "] can be minimized, but an exception was thrown");
            }
        }
    }

    private void assertValidIterators(Iterator expectedBucketIter, Iterator expectedCountsIter, Iterator expectedValuesIter) {
        if (!expectedBucketIter.hasNext()) {
            fail("`expectedBucketIter` iterator ended before `actual` iterator, size mismatch");
        }
        if (!expectedCountsIter.hasNext()) {
            fail("`expectedCountsIter` iterator ended before `actual` iterator, size mismatch");
        }
        if (!expectedValuesIter.hasNext()) {
            fail("`expectedValuesIter` iterator ended before `actual` iterator, size mismatch");
        }
    }

    private void assertBucketContents(Histogram.Bucket actual, Double expectedCount, Double expectedValue) {
        // This is a gap bucket
        SimpleValue countMovAvg = actual.getAggregations().get("movavg_counts");
        if (expectedCount == null) {
            assertThat("[_count] movavg is not null", countMovAvg, nullValue());
        } else if (Double.isNaN(expectedCount)) {
            assertThat("[_count] movavg should be NaN, but is ["+countMovAvg.value()+"] instead", countMovAvg.value(), equalTo(Double.NaN));
        } else {
            assertThat("[_count] movavg is null", countMovAvg, notNullValue());
            assertTrue("[_count] movavg does not match expected [" + countMovAvg.value() + " vs " + expectedCount + "]",
                    nearlyEqual(countMovAvg.value(), expectedCount, 0.1));
        }

        // This is a gap bucket
        SimpleValue valuesMovAvg = actual.getAggregations().get("movavg_values");
        if (expectedValue == null) {
            assertThat("[value] movavg is not null", valuesMovAvg, Matchers.nullValue());
        } else if (Double.isNaN(expectedValue)) {
            assertThat("[value] movavg should be NaN, but is ["+valuesMovAvg.value()+"] instead", valuesMovAvg.value(), equalTo(Double.NaN));
        } else {
            assertThat("[value] movavg is null", valuesMovAvg, notNullValue());
            assertTrue("[value] movavg does not match expected [" + valuesMovAvg.value() + " vs " + expectedValue + "]",
                    nearlyEqual(valuesMovAvg.value(), expectedValue, 0.1));
        }
    }

    /**
     * Better floating point comparisons courtesy of https://github.com/brazzy/floating-point-gui.de
     *
     * Snippet adapted to use doubles instead of floats
     */
    private static boolean nearlyEqual(double a, double b, double epsilon) {
        final double absA = Math.abs(a);
        final double absB = Math.abs(b);
        final double diff = Math.abs(a - b);

        if (a == b) { // shortcut, handles infinities
            return true;
        } else if (a == 0 || b == 0 || diff < Double.MIN_NORMAL) {
            // a or b is zero or both are extremely close to it
            // relative error is less meaningful here
            return diff < (epsilon * Double.MIN_NORMAL);
        } else { // use relative error
            return diff / Math.min((absA + absB), Double.MAX_VALUE) < epsilon;
        }
    }

    private MovAvgModelBuilder randomModelBuilder() {
        return randomModelBuilder(0);
    }

    private MovAvgModelBuilder randomModelBuilder(double padding) {
        int rand = randomIntBetween(0,3);

        // HoltWinters is excluded from random generation, because it's "cold start" behavior makes
        // randomized testing too tricky.  Should probably add dedicated, randomized tests just for HoltWinters,
        // which can compensate for the idiosyncrasies
        switch (rand) {
            case 0:
                return new SimpleModel.SimpleModelBuilder();
            case 1:
                return new LinearModel.LinearModelBuilder();
            case 2:
                return new EwmaModel.EWMAModelBuilder().alpha(alpha + padding);
            case 3:
                return new HoltLinearModel.HoltLinearModelBuilder().alpha(alpha + padding).beta(beta + padding);
            default:
                return new SimpleModel.SimpleModelBuilder();
        }
    }

    private ValuesSourceAggregationBuilder<? extends ValuesSource, ? extends ValuesSourceAggregationBuilder<?, ?>> randomMetric(String name,
                                                                                                                                String field) {
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

}

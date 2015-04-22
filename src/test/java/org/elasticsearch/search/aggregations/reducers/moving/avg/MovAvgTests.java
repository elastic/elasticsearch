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

package org.elasticsearch.search.aggregations.reducers.moving.avg;


import com.google.common.collect.EvictingQueue;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.reducers.BucketHelpers;
import org.elasticsearch.search.aggregations.reducers.SimpleValue;
import org.elasticsearch.search.aggregations.reducers.movavg.models.*;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.search.aggregations.reducers.ReducerBuilders.movingAvg;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class MovAvgTests extends ElasticsearchIntegrationTest {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final String SINGLE_VALUED_VALUE_FIELD_NAME = "v_value";
    private static final String GAP_FIELD = "g_value";

    static int interval;
    static int numValueBuckets;
    static int numFilledValueBuckets;
    static int windowSize;
    static BucketHelpers.GapPolicy gapPolicy;

    static long[] docCounts;
    static long[] docValues;
    static Double[] simpleDocCounts;
    static Double[] linearDocCounts;
    static Double[] singleDocCounts;
    static Double[] doubleDocCounts;

    static Double[] simpleDocValues;
    static Double[] linearDocValues;
    static Double[] singleDocValues;
    static Double[] doubleDocValues;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        List<IndexRequestBuilder> builders = new ArrayList<>();

        interval = 5;
        numValueBuckets = randomIntBetween(6, 80);
        numFilledValueBuckets = numValueBuckets;
        windowSize = randomIntBetween(3,10);
        gapPolicy = randomBoolean() ? BucketHelpers.GapPolicy.IGNORE : BucketHelpers.GapPolicy.INSERT_ZEROS;
                
                
        docCounts = new long[numValueBuckets];
        docValues = new long[numValueBuckets];
        for (int i = 0; i < numValueBuckets; i++) {
            docCounts[i] = randomIntBetween(0, 20);
            docValues[i] = randomIntBetween(1,20);    //this will be used as a constant for all values within a bucket
        }

        // Used for the gap tests
        builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder().startObject()
                .field("gap_test", 0)
                .field(GAP_FIELD, 1).endObject()));
        builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder().startObject()
                .field("gap_test", (numValueBuckets - 1) * interval)
                .field(GAP_FIELD, 1).endObject()));

        this.setupSimple();
        this.setupLinear();
        this.setupSingle();
        this.setupDouble();
        
        for (int i = 0; i < numValueBuckets; i++) {
            for (int docs = 0; docs < docCounts[i]; docs++) {
                builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder().startObject()
                        .field(SINGLE_VALUED_FIELD_NAME, i * interval)
                        .field(SINGLE_VALUED_VALUE_FIELD_NAME, docValues[i]).endObject()));
            }
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    private void setupSimple() {
        simpleDocCounts = new Double[numValueBuckets];
        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < numValueBuckets; i++) {
            if (docCounts[i] == 0 && gapPolicy.equals(BucketHelpers.GapPolicy.IGNORE)) {
                continue;
            }
            window.offer((double)docCounts[i]);

            double movAvg = 0;
            for (double value : window) {
                movAvg += value;
            }
            movAvg /= window.size();

            simpleDocCounts[i] = movAvg;
        }

        window.clear();
        simpleDocValues = new Double[numValueBuckets];
        for (int i = 0; i < numValueBuckets; i++) {
            if (docCounts[i] == 0) {
                // If there was a gap in doc counts and we are ignoring, just skip this bucket
                if (gapPolicy.equals(BucketHelpers.GapPolicy.IGNORE)) {
                    continue;
                } else if (gapPolicy.equals(BucketHelpers.GapPolicy.INSERT_ZEROS)) {
                    // otherwise insert a zero instead of the true value
                    window.offer(0.0);
                } else {
                    window.offer((double) docValues[i]);
                }
            } else {
                //if there are docs in this bucket, insert the regular value
                window.offer((double) docValues[i]);
            }

            double movAvg = 0;
            for (double value : window) {
                movAvg += value;
            }
            movAvg /= window.size();

            simpleDocValues[i] = movAvg;

        }

    }

    private void setupLinear() {
        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        linearDocCounts = new Double[numValueBuckets];
        window.clear();
        for (int i = 0; i < numValueBuckets; i++) {
            if (docCounts[i] == 0 && gapPolicy.equals(BucketHelpers.GapPolicy.IGNORE)) {
                continue;
            }
            window.offer((double)docCounts[i]);

            double avg = 0;
            long totalWeight = 1;
            long current = 1;

            for (double value : window) {
                avg += value * current;
                totalWeight += current;
                current += 1;
            }
            linearDocCounts[i] = avg / totalWeight;
        }

        window.clear();
        linearDocValues = new Double[numValueBuckets];

        for (int i = 0; i < numValueBuckets; i++) {
            if (docCounts[i] == 0) {
                // If there was a gap in doc counts and we are ignoring, just skip this bucket
                if (gapPolicy.equals(BucketHelpers.GapPolicy.IGNORE)) {
                    continue;
                } else if (gapPolicy.equals(BucketHelpers.GapPolicy.INSERT_ZEROS)) {
                    // otherwise insert a zero instead of the true value
                    window.offer(0.0);
                } else {
                    window.offer((double) docValues[i]);
                }
            } else {
                //if there are docs in this bucket, insert the regular value
                window.offer((double) docValues[i]);
            }

            double avg = 0;
            long totalWeight = 1;
            long current = 1;

            for (double value : window) {
                avg += value * current;
                totalWeight += current;
                current += 1;
            }
            linearDocValues[i] = avg / totalWeight;
        }
    }

    private void setupSingle() {
        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        singleDocCounts = new Double[numValueBuckets];
        for (int i = 0; i < numValueBuckets; i++) {
            if (docCounts[i] == 0 && gapPolicy.equals(BucketHelpers.GapPolicy.IGNORE)) {
                continue;
            }
            window.offer((double)docCounts[i]);

            double avg = 0;
            double alpha = 0.5;
            boolean first = true;

            for (double value : window) {
                if (first) {
                    avg = value;
                    first = false;
                } else {
                    avg = (value * alpha) + (avg * (1 - alpha));
                }
            }
            singleDocCounts[i] = avg ;
        }

        singleDocValues = new Double[numValueBuckets];
        window.clear();

        for (int i = 0; i < numValueBuckets; i++) {
            if (docCounts[i] == 0) {
                // If there was a gap in doc counts and we are ignoring, just skip this bucket
                if (gapPolicy.equals(BucketHelpers.GapPolicy.IGNORE)) {
                    continue;
                } else if (gapPolicy.equals(BucketHelpers.GapPolicy.INSERT_ZEROS)) {
                    // otherwise insert a zero instead of the true value
                    window.offer(0.0);
                } else {
                    window.offer((double) docValues[i]);
                }
            } else {
                //if there are docs in this bucket, insert the regular value
                window.offer((double) docValues[i]);
            }

            double avg = 0;
            double alpha = 0.5;
            boolean first = true;

            for (double value : window) {
                if (first) {
                    avg = value;
                    first = false;
                } else {
                    avg = (value * alpha) + (avg * (1 - alpha));
                }
            }
            singleDocValues[i] = avg ;
        }

    }

    private void setupDouble() {
        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        doubleDocCounts = new Double[numValueBuckets];

        for (int i = 0; i < numValueBuckets; i++) {
            if (docCounts[i] == 0 && gapPolicy.equals(BucketHelpers.GapPolicy.IGNORE)) {
                continue;
            }
            window.offer((double)docCounts[i]);

            double s = 0;
            double last_s = 0;

            // Trend value
            double b = 0;
            double last_b = 0;

            double alpha = 0.5;
            double beta = 0.5;
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

            doubleDocCounts[i] = s + (0 * b) ;
        }

        doubleDocValues = new Double[numValueBuckets];
        window.clear();

        for (int i = 0; i < numValueBuckets; i++) {
            if (docCounts[i] == 0) {
                // If there was a gap in doc counts and we are ignoring, just skip this bucket
                if (gapPolicy.equals(BucketHelpers.GapPolicy.IGNORE)) {
                    continue;
                } else if (gapPolicy.equals(BucketHelpers.GapPolicy.INSERT_ZEROS)) {
                    // otherwise insert a zero instead of the true value
                    window.offer(0.0);
                } else {
                    window.offer((double) docValues[i]);
                }
            } else {
                //if there are docs in this bucket, insert the regular value
                window.offer((double) docValues[i]);
            }

            double s = 0;
            double last_s = 0;

            // Trend value
            double b = 0;
            double last_b = 0;

            double alpha = 0.5;
            double beta = 0.5;
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

            doubleDocValues[i] = s + (0 * b) ;
        }
    }

    /**
     * test simple moving average on single value field
     */
    @Test
    public void simpleSingleValuedField() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(movingAvg("movavg_counts")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("_count"))
                                .subAggregation(movingAvg("movavg_values")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_metric"))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("Bucket " + i, bucket, i * interval, docCounts[i]);
            SimpleValue docCountMovAvg = bucket.getAggregations().get("movavg_counts");
            assertThat(docCountMovAvg, notNullValue());
            assertThat(docCountMovAvg.value(), equalTo(simpleDocCounts[i]));

            SimpleValue valuesMovAvg = bucket.getAggregations().get("movavg_values");
            assertThat(valuesMovAvg, notNullValue());
            assertThat(valuesMovAvg.value(), equalTo(simpleDocValues[i]));
        }
    }

    /**
     * test linear moving average on single value field
     */
    @Test
    public void linearSingleValuedField() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(movingAvg("movavg_counts")
                                        .window(windowSize)
                                        .modelBuilder(new LinearModel.LinearModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("_count"))
                                .subAggregation(movingAvg("movavg_values")
                                        .window(windowSize)
                                        .modelBuilder(new LinearModel.LinearModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_metric"))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("Bucket " + i, bucket, i * interval, docCounts[i]);
            SimpleValue docCountMovAvg = bucket.getAggregations().get("movavg_counts");
            assertThat(docCountMovAvg, notNullValue());
            assertThat(docCountMovAvg.value(), equalTo(linearDocCounts[i]));

            SimpleValue valuesMovAvg = bucket.getAggregations().get("movavg_values");
            assertThat(valuesMovAvg, notNullValue());
            assertThat(valuesMovAvg.value(), equalTo(linearDocValues[i]));
        }
    }

    /**
     * test single exponential moving average on single value field
     */
    @Test
    public void singleExpSingleValuedField() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(movingAvg("movavg_counts")
                                        .window(windowSize)
                                        .modelBuilder(new SingleExpModel.SingleExpModelBuilder().alpha(0.5))
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("_count"))
                                .subAggregation(movingAvg("movavg_values")
                                        .window(windowSize)
                                        .modelBuilder(new SingleExpModel.SingleExpModelBuilder().alpha(0.5))
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_metric"))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("Bucket " + i, bucket, i * interval, docCounts[i]);
            SimpleValue docCountMovAvg = bucket.getAggregations().get("movavg_counts");
            assertThat(docCountMovAvg, notNullValue());
            assertThat(docCountMovAvg.value(), equalTo(singleDocCounts[i]));

            SimpleValue valuesMovAvg = bucket.getAggregations().get("movavg_values");
            assertThat(valuesMovAvg, notNullValue());
            assertThat(valuesMovAvg.value(), equalTo(singleDocValues[i]));
        }
    }

    /**
     * test double exponential moving average on single value field
     */
    @Test
    public void doubleExpSingleValuedField() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(movingAvg("movavg_counts")
                                        .window(windowSize)
                                        .modelBuilder(new DoubleExpModel.DoubleExpModelBuilder().alpha(0.5).beta(0.5))
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("_count"))
                                .subAggregation(movingAvg("movavg_values")
                                        .window(windowSize)
                                        .modelBuilder(new DoubleExpModel.DoubleExpModelBuilder().alpha(0.5).beta(0.5))
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_metric"))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("Bucket " + i, bucket, i * interval, docCounts[i]);
            SimpleValue docCountMovAvg = bucket.getAggregations().get("movavg_counts");
            assertThat(docCountMovAvg, notNullValue());
            assertThat(docCountMovAvg.value(), equalTo(doubleDocCounts[i]));

            SimpleValue valuesMovAvg = bucket.getAggregations().get("movavg_values");
            assertThat(valuesMovAvg, notNullValue());
            assertThat(valuesMovAvg.value(), equalTo(doubleDocValues[i]));
        }
    }

    @Test
    public void testSizeZeroWindow() {
        try {
            client()
                    .prepareSearch("idx")
                    .addAggregation(
                            histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                    .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                    .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                    .subAggregation(movingAvg("movavg_counts")
                                            .window(0)
                                            .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                            .gapPolicy(gapPolicy)
                                            .setBucketsPaths("the_metric"))
                    ).execute().actionGet();
            fail("MovingAvg should not accept a window that is zero");

        } catch (SearchPhaseExecutionException exception) {
            //Throwable rootCause = exception.unwrapCause();
            //assertThat(rootCause, instanceOf(SearchParseException.class));
            //assertThat("[window] value must be a positive, non-zero integer.  Value supplied was [0] in [movingAvg].", equalTo(exception.getMessage()));
        }
    }

    @Test
    public void testBadParent() {
        try {
            client()
                    .prepareSearch("idx")
                    .addAggregation(
                            range("histo").field(SINGLE_VALUED_FIELD_NAME).addRange(0, 10)
                                    .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                    .subAggregation(movingAvg("movavg_counts")
                                            .window(0)
                                            .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                            .gapPolicy(gapPolicy)
                                            .setBucketsPaths("the_metric"))
                    ).execute().actionGet();
            fail("MovingAvg should not accept non-histogram as parent");

        } catch (SearchPhaseExecutionException exception) {
           // All good
        }
    }

    @Test
    public void testNegativeWindow() {
        try {
            client()
                    .prepareSearch("idx")
                    .addAggregation(
                            histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                    .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                    .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                    .subAggregation(movingAvg("movavg_counts")
                                            .window(-10)
                                            .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                            .gapPolicy(gapPolicy)
                                            .setBucketsPaths("_count"))
                    ).execute().actionGet();
            fail("MovingAvg should not accept a window that is negative");

        } catch (SearchPhaseExecutionException exception) {
            //Throwable rootCause = exception.unwrapCause();
            //assertThat(rootCause, instanceOf(SearchParseException.class));
            //assertThat("[window] value must be a positive, non-zero integer.  Value supplied was [0] in [movingAvg].", equalTo(exception.getMessage()));
        }
    }

    @Test
    public void testNoBucketsInHistogram() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field("test").interval(interval).minDocCount(0)
                                .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(movingAvg("movavg_counts")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_metric"))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(0));
    }

    @Test
    public void testZeroPrediction() {
        try {
            client()
                    .prepareSearch("idx")
                    .addAggregation(
                            histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                    .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                    .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                    .subAggregation(movingAvg("movavg_counts")
                                            .window(windowSize)
                                            .modelBuilder(randomModelBuilder())
                                            .gapPolicy(gapPolicy)
                                            .predict(0)
                                            .setBucketsPaths("the_metric"))
                    ).execute().actionGet();
            fail("MovingAvg should not accept a prediction size that is zero");

        } catch (SearchPhaseExecutionException exception) {
           // All Good
        }
    }

    @Test
    public void testNegativePrediction() {
        try {
            client()
                    .prepareSearch("idx")
                    .addAggregation(
                            histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                    .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                    .subAggregation(randomMetric("the_metric", SINGLE_VALUED_VALUE_FIELD_NAME))
                                    .subAggregation(movingAvg("movavg_counts")
                                            .window(windowSize)
                                            .modelBuilder(randomModelBuilder())
                                            .gapPolicy(gapPolicy)
                                            .predict(-10)
                                            .setBucketsPaths("the_metric"))
                    ).execute().actionGet();
            fail("MovingAvg should not accept a prediction size that is negative");

        } catch (SearchPhaseExecutionException exception) {
            // All Good
        }
    }

    /**
     * This test uses the "gap" dataset, which is simply a doc at the beginning and end of
     * the SINGLE_VALUED_FIELD_NAME range.  These docs have a value of 1 in the `g_field`.
     * This test verifies that large gaps don't break things, and that the mov avg roughly works
     * in the correct manner (checks direction of change, but not actual values)
     */
    @Test
    public void testGiantGap() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field("gap_test").interval(interval).minDocCount(0)
                                .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                .subAggregation(randomMetric("the_metric", GAP_FIELD))
                                .subAggregation(movingAvg("movavg_counts")
                                        .window(windowSize)
                                        .modelBuilder(randomModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_metric"))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        double lastValue = ((SimpleValue)(buckets.get(0).getAggregations().get("movavg_counts"))).value();
        assertThat(Double.compare(lastValue, 0.0d), greaterThanOrEqualTo(0));

        double currentValue;
        for (int i = 1; i < numValueBuckets - 2; i++) {
            currentValue = ((SimpleValue)(buckets.get(i).getAggregations().get("movavg_counts"))).value();

            // Since there are only two values in this test, at the beginning and end, the moving average should
            // decrease every step (until it reaches zero).  Crude way to check that it's doing the right thing
            // without actually verifying the computed values.  Should work for all types of moving avgs and
            // gap policies
            assertThat(Double.compare(lastValue, currentValue), greaterThanOrEqualTo(0));
            lastValue = currentValue;
        }

        // The last bucket has a real value, so this should always increase the moving avg
        currentValue = ((SimpleValue)(buckets.get(numValueBuckets - 1).getAggregations().get("movavg_counts"))).value();
        assertThat(Double.compare(lastValue, currentValue), equalTo(-1));
    }

    /**
     * Big gap, but with prediction at the end.
     */
    @Test
    public void testGiantGapWithPredict() {

        MovAvgModelBuilder model = randomModelBuilder();
        int numPredictions = randomIntBetween(1, 10);
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field("gap_test").interval(interval).minDocCount(0)
                                .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                .subAggregation(randomMetric("the_metric", GAP_FIELD))
                                .subAggregation(movingAvg("movavg_counts")
                                        .window(windowSize)
                                        .modelBuilder(model)
                                        .gapPolicy(gapPolicy)
                                        .predict(numPredictions)
                                        .setBucketsPaths("the_metric"))
                ).execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets + numPredictions));

        double lastValue = ((SimpleValue)(buckets.get(0).getAggregations().get("movavg_counts"))).value();
        assertThat(Double.compare(lastValue, 0.0d), greaterThanOrEqualTo(0));

        double currentValue;
        for (int i = 1; i < numValueBuckets - 2; i++) {
            currentValue = ((SimpleValue)(buckets.get(i).getAggregations().get("movavg_counts"))).value();

            // Since there are only two values in this test, at the beginning and end, the moving average should
            // decrease every step (until it reaches zero).  Crude way to check that it's doing the right thing
            // without actually verifying the computed values.  Should work for all types of moving avgs and
            // gap policies
            assertThat(Double.compare(lastValue, currentValue), greaterThanOrEqualTo(0));
            lastValue = currentValue;
        }

        // The last bucket has a real value, so this should always increase the moving avg
        currentValue = ((SimpleValue)(buckets.get(numValueBuckets - 1).getAggregations().get("movavg_counts"))).value();
        assertThat(Double.compare(lastValue, currentValue), equalTo(-1));

        // Now check predictions
        for (int i = numValueBuckets; i < numValueBuckets + numPredictions; i++) {
            // Unclear at this point which direction the predictions will go, just verify they are
            // not null, and that we don't have the_metric anymore
            assertThat((buckets.get(i).getAggregations().get("movavg_counts")), notNullValue());
            assertThat((buckets.get(i).getAggregations().get("the_metric")), nullValue());
        }
    }

    /**
     * This test filters the "gap" data so that the first doc is excluded.  This leaves a long stretch of empty
     * buckets until the final bucket.  The moving avg should be zero up until the last bucket, and should work
     * regardless of mov avg type or gap policy.
     */
    @Test
    public void testLeftGap() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        filter("filtered").filter(new RangeFilterBuilder("gap_test").from(1)).subAggregation(
                                histogram("histo").field("gap_test").interval(interval).minDocCount(0)
                                        .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                        .subAggregation(randomMetric("the_metric", GAP_FIELD))
                                        .subAggregation(movingAvg("movavg_counts")
                                                .window(windowSize)
                                                .modelBuilder(randomModelBuilder())
                                                .gapPolicy(gapPolicy)
                                                .setBucketsPaths("the_metric"))
                        )

                ).execute().actionGet();

        assertSearchResponse(response);

        InternalFilter filtered = response.getAggregations().get("filtered");
        assertThat(filtered, notNullValue());
        assertThat(filtered.getName(), equalTo("filtered"));

        InternalHistogram<Bucket> histo = filtered.getAggregations().get("histo");

        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        double currentValue;
        double lastValue = 0.0;
        for (int i = 0; i < numValueBuckets - 1; i++) {
            currentValue = ((SimpleValue)(buckets.get(i).getAggregations().get("movavg_counts"))).value();

            assertThat(Double.compare(lastValue, currentValue), lessThanOrEqualTo(0));
            lastValue = currentValue;
        }

    }

    @Test
    public void testLeftGapWithPrediction() {

        int numPredictions = randomIntBetween(0, 10);
        
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        filter("filtered").filter(new RangeFilterBuilder("gap_test").from(1)).subAggregation(
                                histogram("histo").field("gap_test").interval(interval).minDocCount(0)
                                        .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                        .subAggregation(randomMetric("the_metric", GAP_FIELD))
                                        .subAggregation(movingAvg("movavg_counts")
                                                .window(windowSize)
                                                .modelBuilder(randomModelBuilder())
                                                .gapPolicy(gapPolicy)
                                                .predict(numPredictions)
                                                .setBucketsPaths("the_metric"))
                        )

                ).execute().actionGet();

        assertSearchResponse(response);

        InternalFilter filtered = response.getAggregations().get("filtered");
        assertThat(filtered, notNullValue());
        assertThat(filtered.getName(), equalTo("filtered"));

        InternalHistogram<Bucket> histo = filtered.getAggregations().get("histo");

        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets + numPredictions));

        double currentValue;
        double lastValue = 0.0;
        for (int i = 0; i < numValueBuckets - 1; i++) {
            currentValue = ((SimpleValue)(buckets.get(i).getAggregations().get("movavg_counts"))).value();

            assertThat(Double.compare(lastValue, currentValue), lessThanOrEqualTo(0));
            lastValue = currentValue;
        }

        // Now check predictions
        for (int i = numValueBuckets; i < numValueBuckets + numPredictions; i++) {
            // Unclear at this point which direction the predictions will go, just verify they are
            // not null, and that we don't have the_metric anymore
            assertThat((buckets.get(i).getAggregations().get("movavg_counts")), notNullValue());
            assertThat((buckets.get(i).getAggregations().get("the_metric")), nullValue());
        }
    }

    /**
     * This test filters the "gap" data so that the last doc is excluded.  This leaves a long stretch of empty
     * buckets after the first bucket.  The moving avg should be one at the beginning, then zero for the rest
     * regardless of mov avg type or gap policy.
     */
    @Test
    public void testRightGap() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        filter("filtered").filter(new RangeFilterBuilder("gap_test").to((interval * (numValueBuckets - 1) - interval))).subAggregation(
                                histogram("histo").field("gap_test").interval(interval).minDocCount(0)
                                        .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                        .subAggregation(randomMetric("the_metric", GAP_FIELD))
                                        .subAggregation(movingAvg("movavg_counts")
                                                .window(windowSize)
                                                .modelBuilder(randomModelBuilder())
                                                .gapPolicy(gapPolicy)
                                                .setBucketsPaths("the_metric"))
                        )

                ).execute().actionGet();

        assertSearchResponse(response);

        InternalFilter filtered = response.getAggregations().get("filtered");
        assertThat(filtered, notNullValue());
        assertThat(filtered.getName(), equalTo("filtered"));

        InternalHistogram<Bucket> histo = filtered.getAggregations().get("histo");

        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        double currentValue;
        double lastValue = ((SimpleValue)(buckets.get(0).getAggregations().get("movavg_counts"))).value();
        for (int i = 1; i < numValueBuckets - 1; i++) {
            currentValue = ((SimpleValue)(buckets.get(i).getAggregations().get("movavg_counts"))).value();

            assertThat(Double.compare(lastValue, currentValue), greaterThanOrEqualTo(0));
            lastValue = currentValue;
        }

    }

    @Test
    public void testRightGapWithPredictions() {

        int numPredictions = randomIntBetween(0, 10);

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        filter("filtered").filter(new RangeFilterBuilder("gap_test").to((interval * (numValueBuckets - 1) - interval))).subAggregation(
                                histogram("histo").field("gap_test").interval(interval).minDocCount(0)
                                        .extendedBounds(0L, (long) (interval * (numValueBuckets - 1)))
                                        .subAggregation(randomMetric("the_metric", GAP_FIELD))
                                        .subAggregation(movingAvg("movavg_counts")
                                                .window(windowSize)
                                                .modelBuilder(randomModelBuilder())
                                                .gapPolicy(gapPolicy)
                                                .predict(numPredictions)
                                                .setBucketsPaths("the_metric"))
                        )

                ).execute().actionGet();

        assertSearchResponse(response);

        InternalFilter filtered = response.getAggregations().get("filtered");
        assertThat(filtered, notNullValue());
        assertThat(filtered.getName(), equalTo("filtered"));

        InternalHistogram<Bucket> histo = filtered.getAggregations().get("histo");

        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets + numPredictions));

        double currentValue;
        double lastValue = ((SimpleValue)(buckets.get(0).getAggregations().get("movavg_counts"))).value();
        for (int i = 1; i < numValueBuckets - 1; i++) {
            currentValue = ((SimpleValue)(buckets.get(i).getAggregations().get("movavg_counts"))).value();

            assertThat(Double.compare(lastValue, currentValue), greaterThanOrEqualTo(0));
            lastValue = currentValue;
        }

        // Now check predictions
        for (int i = numValueBuckets; i < numValueBuckets + numPredictions; i++) {
            // Unclear at this point which direction the predictions will go, just verify they are
            // not null, and that we don't have the_metric anymore
            assertThat((buckets.get(i).getAggregations().get("movavg_counts")), notNullValue());
            assertThat((buckets.get(i).getAggregations().get("the_metric")), nullValue());
        }
    }

    @Test
    public void testPredictWithNoBuckets() {

        int numPredictions = randomIntBetween(0, 10);

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        // Filter so we are above all values
                        filter("filtered").filter(new RangeFilterBuilder("gap_test").from((interval * (numValueBuckets - 1) + interval))).subAggregation(
                                histogram("histo").field("gap_test").interval(interval).minDocCount(0)
                                        .subAggregation(randomMetric("the_metric", GAP_FIELD))
                                        .subAggregation(movingAvg("movavg_counts")
                                                .window(windowSize)
                                                .modelBuilder(randomModelBuilder())
                                                .gapPolicy(gapPolicy)
                                                .predict(numPredictions)
                                                .setBucketsPaths("the_metric"))
                        )

                ).execute().actionGet();

        assertSearchResponse(response);

        InternalFilter filtered = response.getAggregations().get("filtered");
        assertThat(filtered, notNullValue());
        assertThat(filtered.getName(), equalTo("filtered"));

        InternalHistogram<Bucket> histo = filtered.getAggregations().get("histo");

        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(0));
    }


    private void checkBucketKeyAndDocCount(final String msg, final Histogram.Bucket bucket, final long expectedKey,
                                           long expectedDocCount) {
        if (expectedDocCount == -1) {
            expectedDocCount = 0;
        }
        assertThat(msg, bucket, notNullValue());
        assertThat(msg + " key", ((Number) bucket.getKey()).longValue(), equalTo(expectedKey));
        assertThat(msg + " docCount", bucket.getDocCount(), equalTo(expectedDocCount));
    }

    private MovAvgModelBuilder randomModelBuilder() {
        int rand = randomIntBetween(0,3);

        switch (rand) {
            case 0:
                return new SimpleModel.SimpleModelBuilder();
            case 1:
                return new LinearModel.LinearModelBuilder();
            case 2:
                return new SingleExpModel.SingleExpModelBuilder().alpha(randomDouble());
            case 3:
                return new DoubleExpModel.DoubleExpModelBuilder().alpha(randomDouble()).beta(randomDouble());
            default:
                return new SimpleModel.SimpleModelBuilder();
        }
    }
    
    private ValuesSourceMetricsAggregationBuilder randomMetric(String name, String field) {
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

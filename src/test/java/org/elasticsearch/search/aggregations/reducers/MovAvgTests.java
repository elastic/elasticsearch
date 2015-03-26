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

package org.elasticsearch.search.aggregations.reducers;


import com.google.common.collect.EvictingQueue;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.Bucket;
import org.elasticsearch.search.aggregations.reducers.smooth.models.DoubleExpModel;
import org.elasticsearch.search.aggregations.reducers.smooth.models.LinearModel;
import org.elasticsearch.search.aggregations.reducers.smooth.models.SimpleModel;
import org.elasticsearch.search.aggregations.reducers.smooth.models.SingleExpModel;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.search.aggregations.reducers.ReducerBuilders.smooth;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class MovAvgTests extends ElasticsearchIntegrationTest {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final String SINGLE_VALUED_VALUE_FIELD_NAME = "v_value";

    static int interval;
    static int numValueBuckets;
    static int numFilledValueBuckets;
    static int windowSize;
    static BucketHelpers.GapPolicy gapPolicy;

    static long[] docCounts;
    static long[] valueCounts;
    static Double[] simpleMovAvgCounts;
    static Double[] linearMovAvgCounts;
    static Double[] singleExpMovAvgCounts;
    static Double[] doubleExpMovAvgCounts;

    static Double[] simpleMovAvgValueCounts;
    static Double[] linearMovAvgValueCounts;
    static Double[] singleExpMovAvgValueCounts;
    static Double[] doubleExpMovAvgValueCounts;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        interval = 5;
        numValueBuckets = randomIntBetween(6, 80);
        numFilledValueBuckets = numValueBuckets;
        windowSize = randomIntBetween(3,10);
        gapPolicy = randomBoolean() ? BucketHelpers.GapPolicy.IGNORE : BucketHelpers.GapPolicy.INSERT_ZEROS;

        docCounts = new long[numValueBuckets];
        valueCounts = new long[numValueBuckets];
        for (int i = 0; i < numValueBuckets; i++) {
            docCounts[i] = randomIntBetween(0, 20);
            valueCounts[i] = randomIntBetween(1,20);    //this will be used as a constant for all values within a bucket
        }

        this.setupSimple();
        this.setupLinear();
        this.setupSingle();
        this.setupDouble();


        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numValueBuckets; i++) {
            for (int docs = 0; docs < docCounts[i]; docs++) {
                builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder().startObject()
                        .field(SINGLE_VALUED_FIELD_NAME, i * interval)
                        .field(SINGLE_VALUED_VALUE_FIELD_NAME, 1).endObject()));
            }
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    private void setupSimple() {
        simpleMovAvgCounts = new Double[numValueBuckets];
        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        for (int i = 0; i < numValueBuckets; i++) {
            double thisValue = docCounts[i];
            window.offer(thisValue);

            double movAvg = 0;
            for (double value : window) {
                movAvg += value;
            }
            movAvg /= window.size();

            simpleMovAvgCounts[i] = movAvg;
        }

        window.clear();
        simpleMovAvgValueCounts = new Double[numValueBuckets];
        for (int i = 0; i < numValueBuckets; i++) {
            window.offer((double)docCounts[i]);

            double movAvg = 0;
            for (double value : window) {
                movAvg += value;
            }
            movAvg /= window.size();

            simpleMovAvgValueCounts[i] = movAvg;

        }

    }

    private void setupLinear() {
        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        linearMovAvgCounts = new Double[numValueBuckets];
        window.clear();
        for (int i = 0; i < numValueBuckets; i++) {
            double thisValue = docCounts[i];
            if (thisValue == -1) {
                thisValue = 0;
            }
            window.offer(thisValue);

            double avg = 0;
            long totalWeight = 1;
            long current = 1;

            for (double value : window) {
                avg += value * current;
                totalWeight += current;
                current += 1;
            }
            linearMovAvgCounts[i] = avg / totalWeight;
        }

        window.clear();
        linearMovAvgValueCounts = new Double[numValueBuckets];

        for (int i = 0; i < numValueBuckets; i++) {
            double thisValue = docCounts[i];
            window.offer(thisValue);

            double avg = 0;
            long totalWeight = 1;
            long current = 1;

            for (double value : window) {
                avg += value * current;
                totalWeight += current;
                current += 1;
            }
            linearMovAvgValueCounts[i] = avg / totalWeight;
        }
    }

    private void setupSingle() {
        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        singleExpMovAvgCounts = new Double[numValueBuckets];
        for (int i = 0; i < numValueBuckets; i++) {
            double thisValue = docCounts[i];
            if (thisValue == -1) {
                thisValue = 0;
            }
            window.offer(thisValue);

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
            singleExpMovAvgCounts[i] = avg ;
        }

        singleExpMovAvgValueCounts = new Double[numValueBuckets];
        window.clear();

        for (int i = 0; i < numValueBuckets; i++) {
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
            singleExpMovAvgCounts[i] = avg ;
        }

    }

    private void setupDouble() {
        EvictingQueue<Double> window = EvictingQueue.create(windowSize);
        doubleExpMovAvgCounts = new Double[numValueBuckets];

        for (int i = 0; i < numValueBuckets; i++) {
            double thisValue = docCounts[i];
            if (thisValue == -1) {
                thisValue = 0;
            }
            window.offer(thisValue);

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

            doubleExpMovAvgCounts[i] = s + (0 * b) ;
        }

        doubleExpMovAvgValueCounts = new Double[numValueBuckets];
        window.clear();

        for (int i = 0; i < numValueBuckets; i++) {
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

            doubleExpMovAvgValueCounts[i] = s + (0 * b) ;
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
                                .subAggregation(sum("the_sum").field(SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(smooth("smooth")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("_count"))
                                .subAggregation(smooth("movavg_values")
                                        .window(windowSize)
                                        .modelBuilder(new SimpleModel.SimpleModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_sum"))
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
            SimpleValue docCountMovAvg = bucket.getAggregations().get("smooth");
            assertThat(docCountMovAvg, notNullValue());
            assertThat(docCountMovAvg.value(), equalTo(simpleMovAvgCounts[i]));

            SimpleValue valuesMovAvg = bucket.getAggregations().get("movavg_values");
            assertThat(valuesMovAvg, notNullValue());
            assertThat(valuesMovAvg.value(), equalTo(simpleMovAvgCounts[i]));
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
                                .subAggregation(sum("the_sum").field(SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(smooth("smooth")
                                        .window(windowSize)
                                        .modelBuilder(new LinearModel.LinearModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("_count"))
                                .subAggregation(smooth("movavg_values")
                                        .window(windowSize)
                                        .modelBuilder(new LinearModel.LinearModelBuilder())
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_sum"))
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
            SimpleValue docCountMovAvg = bucket.getAggregations().get("smooth");
            assertThat(docCountMovAvg, notNullValue());
            assertThat(docCountMovAvg.value(), equalTo(linearMovAvgCounts[i]));

            SimpleValue valuesMovAvg = bucket.getAggregations().get("movavg_values");
            assertThat(valuesMovAvg, notNullValue());
            assertThat(valuesMovAvg.value(), equalTo(linearMovAvgCounts[i]));
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
                                .subAggregation(sum("the_sum").field(SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(smooth("smooth")
                                        .window(windowSize)
                                        .modelBuilder(new SingleExpModel.SingleExpModelBuilder().alpha(0.5))
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("_count"))
                                .subAggregation(smooth("movavg_values")
                                        .window(windowSize)
                                        .modelBuilder(new SingleExpModel.SingleExpModelBuilder().alpha(0.5))
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_sum"))
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
            SimpleValue docCountMovAvg = bucket.getAggregations().get("smooth");
            assertThat(docCountMovAvg, notNullValue());
            assertThat(docCountMovAvg.value(), equalTo(singleExpMovAvgCounts[i]));

            SimpleValue valuesMovAvg = bucket.getAggregations().get("movavg_values");
            assertThat(valuesMovAvg, notNullValue());
            assertThat(valuesMovAvg.value(), equalTo(singleExpMovAvgCounts[i]));
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
                                .subAggregation(sum("the_sum").field(SINGLE_VALUED_VALUE_FIELD_NAME))
                                .subAggregation(smooth("smooth")
                                        .window(windowSize)
                                        .modelBuilder(new DoubleExpModel.DoubleExpModelBuilder().alpha(0.5).beta(0.5))
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("_count"))
                                .subAggregation(smooth("movavg_values")
                                        .window(windowSize)
                                        .modelBuilder(new DoubleExpModel.DoubleExpModelBuilder().alpha(0.5).beta(0.5))
                                        .gapPolicy(gapPolicy)
                                        .setBucketsPaths("the_sum"))
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
            SimpleValue docCountMovAvg = bucket.getAggregations().get("smooth");
            assertThat(docCountMovAvg, notNullValue());
            assertThat(docCountMovAvg.value(), equalTo(doubleExpMovAvgCounts[i]));

            SimpleValue valuesMovAvg = bucket.getAggregations().get("movavg_values");
            assertThat(valuesMovAvg, notNullValue());
            assertThat(valuesMovAvg.value(), equalTo(doubleExpMovAvgCounts[i]));
        }
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

}

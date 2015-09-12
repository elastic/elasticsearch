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

package org.elasticsearch.search.aggregations.pipeline;


import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;

/**
 * Provides helper methods and classes for use in PipelineAggregation tests,
 * such as creating mock histograms or computing simple metrics
 */
public class PipelineAggregationHelperTests extends ESTestCase {

    /**
     * Generates a mock histogram to use for testing.  Each MockBucket holds a doc count, key and document values
     * which can later be used to compute metrics and compare against the real aggregation results.  Gappiness can be
     * controlled via parameters
     *
     * @param interval          Interval between bucket keys
     * @param size              Size of mock histogram to generate (in buckets)
     * @param gapProbability    Probability of generating an empty bucket. 0.0-1.0 inclusive
     * @param runProbability    Probability of extending a gap once one has been created.  0.0-1.0 inclusive
     * @return
     */
    public static ArrayList<MockBucket> generateHistogram(int interval, int size, double gapProbability, double runProbability) {
        ArrayList<MockBucket> values = new ArrayList<>(size);

        boolean lastWasGap = false;
        boolean emptyHisto = true;

        for (int i = 0; i < size; i++) {
            MockBucket bucket = new MockBucket();
            if (randomDouble() < gapProbability) {
                // start a gap
                bucket.count = 0;
                bucket.docValues = new double[0];

                lastWasGap = true;

            } else if (lastWasGap && randomDouble() < runProbability) {
                // add to the existing gap
                bucket.count = 0;
                bucket.docValues = new double[0];

                lastWasGap = true;
            } else {
                bucket.count = randomIntBetween(1, 50);
                bucket.docValues = new double[bucket.count];
                for (int j = 0; j < bucket.count; j++) {
                    bucket.docValues[j] = randomDouble() * randomIntBetween(-20, 20);
                }
                lastWasGap = false;
                emptyHisto = false;
            }

            bucket.key = i * interval;
            values.add(bucket);
        }

        if (emptyHisto) {
            int idx = randomIntBetween(0, values.size()-1);
            MockBucket bucket = values.get(idx);
            bucket.count = randomIntBetween(1, 50);
            bucket.docValues = new double[bucket.count];
            for (int j = 0; j < bucket.count; j++) {
                bucket.docValues[j] = randomDouble() * randomIntBetween(-20, 20);
            }
            values.set(idx, bucket);
        }

        return values;
    }

    /**
     * Simple mock bucket container
     */
    public static class MockBucket {
        public int count;
        public double[] docValues;
        public long key;
    }

    /**
     * Computes a simple agg metric (min, sum, etc) from the provided values
     *
     * @param values Array of values to compute metric for
     * @param metric A metric builder which defines what kind of metric should be returned for the values
     * @return
     */
    public static double calculateMetric(double[] values, ValuesSourceMetricsAggregationBuilder metric) {

        if (metric instanceof MinBuilder) {
            double accumulator = Double.POSITIVE_INFINITY;
            for (double value : values) {
                accumulator = Math.min(accumulator, value);
            }
            return accumulator;
        } else if (metric instanceof MaxBuilder) {
            double accumulator = Double.NEGATIVE_INFINITY;
            for (double value : values) {
                accumulator = Math.max(accumulator, value);
            }
            return accumulator;
        } else if (metric instanceof SumBuilder) {
            double accumulator = 0;
            for (double value : values) {
                accumulator += value;
            }
            return accumulator;
        } else if (metric instanceof AvgBuilder) {
            double accumulator = 0;
            for (double value : values) {
                accumulator += value;
            }
            return accumulator / values.length;
        }

        return 0.0;
    }
}

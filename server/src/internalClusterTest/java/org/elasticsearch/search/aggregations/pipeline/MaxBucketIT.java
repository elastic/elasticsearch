/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntToDoubleFunction;

import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.maxBucket;
import static org.hamcrest.Matchers.equalTo;

public class MaxBucketIT extends BucketMetricsPipeLineAggregationTestCase<InternalBucketMetricValue> {

    @Override
    protected MaxBucketPipelineAggregationBuilder BucketMetricsPipelineAgg(String name, String bucketsPath) {
        return maxBucket(name, bucketsPath);
    }

    @Override
    protected void assertResult(
        IntToDoubleFunction bucketValues,
        Function<Integer, String> bucketKeys,
        int numBuckets,
        InternalBucketMetricValue pipelineBucket
    ) {
        List<String> maxKeys = new ArrayList<>();
        double maxValue = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < numBuckets; ++i) {
            double bucketValue = bucketValues.applyAsDouble(i);
            if (bucketValue > maxValue) {
                maxValue = bucketValue;
                maxKeys = new ArrayList<>();
                maxKeys.add(bucketKeys.apply(i));
            } else if (bucketValue == maxValue) {
                maxKeys.add(bucketKeys.apply(i));
            }
        }
        assertThat(pipelineBucket.value(), equalTo(maxValue));
        assertThat(pipelineBucket.keys(), equalTo(maxKeys.toArray(new String[0])));
    }

    @Override
    protected String nestedMetric() {
        return "value";
    }

    @Override
    protected double getNestedMetric(InternalBucketMetricValue bucket) {
        return bucket.value();
    }
}

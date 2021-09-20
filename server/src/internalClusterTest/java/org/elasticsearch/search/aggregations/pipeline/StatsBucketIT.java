/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import java.util.function.Function;
import java.util.function.IntToDoubleFunction;

import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.statsBucket;
import static org.hamcrest.Matchers.equalTo;

public class StatsBucketIT extends BucketMetricsPipeLineAggregationTestCase<StatsBucket> {

    @Override
    protected StatsBucketPipelineAggregationBuilder BucketMetricsPipelineAgg(String name, String bucketsPath) {
        return statsBucket(name, bucketsPath);
    }

    @Override
    protected void assertResult(
        IntToDoubleFunction bucketValues,
        Function<Integer, String> bucketKeys,
        int numBuckets,
        StatsBucket pipelineBucket
    ) {
        double sum = 0;
        int count = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < numBuckets; ++i) {
            double bucketValue = bucketValues.applyAsDouble(i);
            count++;
            sum += bucketValue;
            min = Math.min(min, bucketValue);
            max = Math.max(max, bucketValue);
        }

        double avgValue = count == 0 ? Double.NaN : (sum / count);
        assertThat(pipelineBucket.getAvg(), equalTo(avgValue));
        assertThat(pipelineBucket.getMin(), equalTo(min));
        assertThat(pipelineBucket.getMax(), equalTo(max));
    }

    @Override
    protected String nestedMetric() {
        return "avg";
    }

    @Override
    protected double getNestedMetric(StatsBucket bucket) {
        return bucket.getAvg();
    }
}

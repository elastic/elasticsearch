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

import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.sumBucket;
import static org.hamcrest.Matchers.equalTo;

public class SumBucketIT extends BucketMetricsPipeLineAggregationTestCase<InternalSimpleValue> {

    @Override
    protected SumBucketPipelineAggregationBuilder BucketMetricsPipelineAgg(String name, String bucketsPath) {
        return sumBucket(name, bucketsPath);
    }

    @Override
    protected void assertResult(
        IntToDoubleFunction bucketValues,
        Function<Integer, String> bucketKeys,
        int numBuckets,
        InternalSimpleValue pipelineBucket
    ) {
        double sum = 0;
        for (int i = 0; i < numBuckets; ++i) {
            sum += bucketValues.applyAsDouble(i);
        }
        assertThat(pipelineBucket.value(), equalTo(sum));
    }

    @Override
    protected String nestedMetric() {
        return "value";
    }

    @Override
    protected double getNestedMetric(InternalSimpleValue bucket) {
        return bucket.value();
    }
}

package org.elasticsearch.search.aggregations.pipeline;

import java.util.function.Function;
import java.util.function.IntToDoubleFunction;

import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.avgBucket;
import static org.hamcrest.Matchers.equalTo;

public class AvgBucketIT extends BucketMetricsPipeLineAggregationTestCase<InternalSimpleValue> {

    @Override
    protected AvgBucketPipelineAggregationBuilder BucketMetricsPipelineAgg(String name, String bucketsPath) {
        return avgBucket(name, bucketsPath);
    }

    @Override
    protected void assertResult(
        IntToDoubleFunction bucketValues,
        Function<Integer, String> bucketKeys,
        int numBuckets,
        InternalSimpleValue pipelineBucket
    ) {
        double sum = 0;
        int count = 0;
        for (int i = 0; i < numBuckets; ++i) {
            count++;
            sum += bucketValues.applyAsDouble(i);
        }
        double avgValue = count == 0 ? Double.NaN : (sum / count);
        assertThat(pipelineBucket.value(), equalTo(avgValue));
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

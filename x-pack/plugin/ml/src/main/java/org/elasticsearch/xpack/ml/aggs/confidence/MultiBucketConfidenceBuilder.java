/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class MultiBucketConfidenceBuilder extends ConfidenceBuilder {

    private final Map<String, BucketConfidenceBuilder> innerConfidenceBuilders;

    MultiBucketConfidenceBuilder(String name, InternalMultiBucketAggregation<?, ?> calculatedValue) {
        super(name, calculatedValue);
        this.innerConfidenceBuilders = new LinkedHashMap<>();
        calculatedValue.getBuckets()
            .forEach(bucket -> innerConfidenceBuilders.put(bucket.getKeyAsString(), new BucketConfidenceBuilder(bucket)));
    }

    @Override
    InternalConfidenceAggregation.ConfidenceBucket build(double probability, double pUpper, double pLower, boolean keyed) {
        values.forEach(agg -> {
            InternalMultiBucketAggregation<?, ?> bootStrapped = (InternalMultiBucketAggregation<?, ?>) agg;
            bootStrapped.getBuckets().forEach(b -> {
                BucketConfidenceBuilder builder = innerConfidenceBuilders.get(b.getKeyAsString());
                if (builder == null) {
                    throw new AggregationExecutionException(
                        "cannot calculate confidence for multi-bucket agg ["
                            + name
                            + "] sampled aggregation has unexpected bucket key ["
                            + b.getKeyAsString()
                            + "]"
                    );
                }
                builder.addBucket(b);
            });
        });
        Map<String, ConfidenceValue> upperBucketValues = new LinkedHashMap<>();
        Map<String, ConfidenceValue> calculatedBucketValues = new LinkedHashMap<>();
        Map<String, ConfidenceValue> lowerBucketValues = new LinkedHashMap<>();
        innerConfidenceBuilders.forEach((bucketKey, builder) -> {
            InternalConfidenceAggregation.ConfidenceBucket intermediateResult = builder.build(probability, pUpper, pLower, keyed);
            upperBucketValues.put(bucketKey, intermediateResult.upper);
            lowerBucketValues.put(bucketKey, intermediateResult.lower);
            calculatedBucketValues.put(bucketKey, intermediateResult.value);
        });
        return new InternalConfidenceAggregation.ConfidenceBucket(
            name,
            keyed,
            new MultiBucketConfidenceValue(upperBucketValues),
            new MultiBucketConfidenceValue(lowerBucketValues),
            new MultiBucketConfidenceValue(calculatedBucketValues)
        );
    }

    static class MultiBucketConfidenceValue implements ConfidenceValue {
        static final String NAME = "single_bucket_confidence_value";
        private final Map<String, ConfidenceValue> bucketConfidenceValues;

        MultiBucketConfidenceValue(Map<String, ConfidenceValue> innerValue) {
            this.bucketConfidenceValues = innerValue;
        }

        MultiBucketConfidenceValue(StreamInput in) throws IOException {
            this.bucketConfidenceValues = in.readOrderedMap(StreamInput::readString, i -> i.readNamedWriteable(ConfidenceValue.class));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Aggregation.CommonFields.BUCKETS.getPreferredName(), bucketConfidenceValues);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(bucketConfidenceValues, StreamOutput::writeString, StreamOutput::writeNamedWriteable);
        }
    }

    private static class BucketConfidenceBuilder {
        private final Map<String, ConfidenceBuilder> innerBucketConfidenceBuilders;
        private final MultiBucketsAggregation.Bucket bucketValue;

        private BucketConfidenceBuilder(MultiBucketsAggregation.Bucket trueBucketValue) {
            this.bucketValue = trueBucketValue;
            this.innerBucketConfidenceBuilders = new LinkedHashMap<>();
            trueBucketValue.getAggregations()
                .forEach(
                    agg -> innerBucketConfidenceBuilders.put(
                        agg.getName(),
                        ConfidenceBuilder.factory((InternalAggregation) agg, agg.getName(), bucketValue.getDocCount())
                    )
                );
        }

        private void addBucket(MultiBucketsAggregation.Bucket bucket) {
            assert bucket.getKey().equals(bucketValue.getKey())
                : "cannot build confidence between mismatched buckets ["
                    + bucketValue.getKeyAsString()
                    + "] ["
                    + bucket.getKeyAsString()
                    + "]";
            bucket.getAggregations().forEach(agg -> innerBucketConfidenceBuilders.get(agg.getName()).addAgg((InternalAggregation) agg));
        }

        private InternalConfidenceAggregation.ConfidenceBucket build(double probability, double pUpper, double pLower, boolean keyed) {
            Map<String, ConfidenceValue> upperValues = new HashMap<>();
            Map<String, ConfidenceValue> calculatedValues = new HashMap<>();
            Map<String, ConfidenceValue> lowerValues = new HashMap<>();
            innerBucketConfidenceBuilders.forEach((aggName, builder) -> {
                InternalConfidenceAggregation.ConfidenceBucket intermediateResult = builder.build(probability, pUpper, pLower, keyed);
                upperValues.put(aggName, intermediateResult.upper);
                lowerValues.put(aggName, intermediateResult.lower);
                calculatedValues.put(aggName, intermediateResult.value);
            });
            InternalConfidenceAggregation.ConfidenceBucket intermediateResult = SingleMetricConfidenceBuilder.fromCount(
                bucketValue.getKeyAsString(),
                bucketValue.getDocCount(),
                probability,
                pUpper,
                pLower,
                keyed
            );
            return new InternalConfidenceAggregation.ConfidenceBucket(
                bucketValue.getKeyAsString(),
                keyed,
                new ConfidenceValue.BucketConfidenceValue(
                    ((SingleMetricConfidenceBuilder.SingleMetricConfidenceValue) intermediateResult.upper).value(),
                    upperValues
                ),
                new ConfidenceValue.BucketConfidenceValue(
                    ((SingleMetricConfidenceBuilder.SingleMetricConfidenceValue) intermediateResult.lower).value(),
                    lowerValues
                ),
                new ConfidenceValue.BucketConfidenceValue(
                    ((SingleMetricConfidenceBuilder.SingleMetricConfidenceValue) intermediateResult.value).value(),
                    calculatedValues
                )
            );
        }
    }
}

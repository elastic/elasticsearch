/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;

import java.util.HashMap;
import java.util.Map;

public class SingleBucketConfidenceBuilder extends ConfidenceBuilder {

    private final Map<String, ConfidenceBuilder> innerConfidenceBuilders;

    SingleBucketConfidenceBuilder(String name, InternalAggregations internalAggregations, InternalSingleBucketAggregation calculatedValue) {
        super(name, calculatedValue);
        this.innerConfidenceBuilders = new HashMap<>();
        internalAggregations.asList()
            .forEach(
                agg -> innerConfidenceBuilders.put(
                    agg.getName(),
                    ConfidenceBuilder.factory((InternalAggregation) agg, agg.getName(), calculatedValue.getDocCount())
                )
            );
    }

    @Override
    InternalConfidenceAggregation.ConfidenceBucket build(double probability, double pUpper, double pLower, boolean keyed) {
        Map<String, ConfidenceValue> upperValues = new HashMap<>();
        Map<String, ConfidenceValue> calculatedValues = new HashMap<>();
        Map<String, ConfidenceValue> lowerValues = new HashMap<>();
        values.forEach(agg -> {
            InternalSingleBucketAggregation singleBucketAggregation = (InternalSingleBucketAggregation) agg;
            singleBucketAggregation.getAggregations().forEach(innerAgg -> {
                ConfidenceBuilder builder = innerConfidenceBuilders.get(innerAgg.getName());
                if (builder == null) {
                    throw new AggregationExecutionException(
                        "calculating confidence for aggregation [" + name + "] encountered missing sub-agg [" + innerAgg.getName() + "]"
                    );
                }
                builder.addAgg((InternalAggregation) innerAgg);
            });
        });
        innerConfidenceBuilders.forEach((aggName, builder) -> {
            InternalConfidenceAggregation.ConfidenceBucket intermediateResult = builder.build(probability, pUpper, pLower, keyed);
            upperValues.put(aggName, intermediateResult.upper);
            lowerValues.put(aggName, intermediateResult.lower);
            calculatedValues.put(aggName, intermediateResult.value);
        });
        InternalConfidenceAggregation.ConfidenceBucket intermediateResult = SingleMetricConfidenceBuilder.fromCount(
            name,
            ((SingleBucketAggregation) calculatedValue).getDocCount(),
            probability,
            pUpper,
            pLower,
            keyed
        );
        return new InternalConfidenceAggregation.ConfidenceBucket(
            name,
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentiles;

import java.util.ArrayList;
import java.util.List;

abstract class ConfidenceBuilder {
    protected final String name;
    protected final InternalAggregation calculatedValue;
    protected final List<InternalAggregation> values = new ArrayList<>();

    static ConfidenceBuilder factory(InternalAggregation internalAggregation, String name, long docCount) {
        if (internalAggregation instanceof InternalCardinality) {
            throw new AggregationExecutionException("aggregations of type [cardinality] are not supported");
        }
        if (internalAggregation instanceof InternalNumericMetricsAggregation.SingleValue singleValue) {
            return SingleMetricConfidenceBuilder.factory(singleValue, name, docCount);
        } else if (internalAggregation instanceof Percentiles) {
            return new MultiMetricConfidenceBuilder.MeanMultiMetricConfidenceBuilder(name, internalAggregation, agg -> {
                List<Double> values = new ArrayList<>();
                ((Percentiles) agg).iterator().forEachRemaining(p -> values.add(p.getValue()));
                return values.stream().mapToDouble(Double::doubleValue).toArray();
            }, agg -> {
                List<String> keys = new ArrayList<>();
                ((Percentiles) agg).iterator().forEachRemaining(p -> keys.add(Double.toString(p.getPercent())));
                return keys.toArray(String[]::new);
            });
        } else if (internalAggregation instanceof InternalSingleBucketAggregation singleBucketAggregation) {
            return new SingleBucketConfidenceBuilder(name, singleBucketAggregation.getAggregations(), singleBucketAggregation);
        } else if (internalAggregation instanceof InternalMultiBucketAggregation<?, ?> bucketsAggregation) {
            return new MultiBucketConfidenceBuilder(name, bucketsAggregation);
        }
        throw new AggregationExecutionException("aggregations of type [" + internalAggregation.getType() + "] are not supported");
    }

    ConfidenceBuilder(String name, InternalAggregation calculatedValue) {
        this.name = name;
        this.calculatedValue = calculatedValue;
    }

    void addAgg(InternalAggregation agg) {
        values.add(agg);
    }

    abstract InternalConfidenceAggregation.ConfidenceBucket build(double probability, double pUpper, double pLower, boolean keyed);

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Map;

public class BucketCorrelationAggregator extends SiblingPipelineAggregator {

    private final CorrelationFunction correlationFunction;

    public BucketCorrelationAggregator(
        String name,
        CorrelationFunction correlationFunction,
        String bucketsPath,
        Map<String, Object> metadata
    ) {
        super(name, new String[] { bucketsPath }, metadata);
        this.correlationFunction = correlationFunction;
    }

    @Override
    public InternalAggregation doReduce(Aggregations aggregations, InternalAggregation.ReduceContext context) {
        MlAggsHelper.DoubleBucketValues bucketPathValue = MlAggsHelper.extractDoubleBucketedValues(bucketsPaths()[0], aggregations)
            .orElseThrow(
                () -> new AggregationExecutionException(
                    "unable to find valid bucket values in path [" + bucketsPaths()[0] + "] for agg [" + name() + "]"
                )
            );

        return new InternalSimpleValue(name(), correlationFunction.execute(bucketPathValue, aggregations), DocValueFormat.RAW, metadata());
    }

}

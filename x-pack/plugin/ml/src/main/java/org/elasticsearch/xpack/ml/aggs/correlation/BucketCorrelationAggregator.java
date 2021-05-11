/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BucketCorrelationAggregator extends SiblingPipelineAggregator {

    private final CorrelationFunction correlationFunction;

    public BucketCorrelationAggregator(String name,
                                       CorrelationFunction correlationFunction,
                                       String bucketsPath,
                                       Map<String, Object> metadata) {
        super(name, new String[]{ bucketsPath }, metadata);
        this.correlationFunction = correlationFunction;
    }

    @Override
    public InternalAggregation doReduce(Aggregations aggregations, InternalAggregation.ReduceContext context) {
        CountCorrelationIndicator bucketPathValue = null;
        List<String> parsedPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(parsedPath.get(0))) {
                List<String> sublistedPath = parsedPath.subList(1, parsedPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                List<Double> values = new ArrayList<>(buckets.size());
                long docCount = 0;
                for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                    Double bucketValue = BucketHelpers.resolveBucketValue(
                        multiBucketsAgg,
                        bucket,
                        sublistedPath,
                        BucketHelpers.GapPolicy.INSERT_ZEROS
                    );
                    if (bucketValue != null && Double.isNaN(bucketValue) == false) {
                        values.add(bucketValue);
                    }
                    docCount += bucket.getDocCount();
                }
                bucketPathValue = new CountCorrelationIndicator(
                    values.stream().mapToDouble(Double::doubleValue).toArray(),
                    null,
                    docCount
                );
                break;
            }
        }
        if (bucketPathValue == null) {
            throw new AggregationExecutionException(
                "unable to find valid bucket values in path [" + bucketsPaths()[0] + "] for agg [" + name() + "]"
            );
        }

        return new InternalSimpleValue(name(), correlationFunction.execute(bucketPathValue), DocValueFormat.RAW, metadata());
    }

}

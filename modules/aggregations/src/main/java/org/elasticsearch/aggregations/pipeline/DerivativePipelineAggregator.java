/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class DerivativePipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final GapPolicy gapPolicy;
    private final Double xAxisUnits;

    DerivativePipelineAggregator(
        String name,
        String[] bucketsPaths,
        DocValueFormat formatter,
        GapPolicy gapPolicy,
        Long xAxisUnits,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.xAxisUnits = xAxisUnits == null ? null : (double) xAxisUnits;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, AggregationReduceContext reduceContext) {
        @SuppressWarnings("rawtypes")
        InternalMultiBucketAggregation<
            ? extends InternalMultiBucketAggregation,
            ? extends InternalMultiBucketAggregation.InternalBucket> histo = (InternalMultiBucketAggregation<
                ? extends InternalMultiBucketAggregation,
                ? extends InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>();
        Number lastBucketKey = null;
        Double lastBucketValue = null;
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Number thisBucketKey = factory.getKey(bucket);
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);
            if (lastBucketValue != null && thisBucketValue != null) {
                double gradient = thisBucketValue - lastBucketValue;
                double xDiff = -1;
                if (xAxisUnits != null) {
                    xDiff = (thisBucketKey.doubleValue() - lastBucketKey.doubleValue()) / xAxisUnits;
                }
                newBuckets.add(
                    factory.createBucket(
                        factory.getKey(bucket),
                        bucket.getDocCount(),
                        InternalAggregations.append(
                            bucket.getAggregations(),
                            new Derivative(name(), gradient, xDiff, formatter, metadata())
                        )
                    )
                );
            } else {
                newBuckets.add(bucket);
            }
            lastBucketKey = thisBucketKey;
            lastBucketValue = thisBucketValue;
        }
        return factory.createAggregation(newBuckets);
    }
}

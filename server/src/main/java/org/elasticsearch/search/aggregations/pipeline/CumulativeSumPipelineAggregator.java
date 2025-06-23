/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class CumulativeSumPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;

    CumulativeSumPipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
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
        List<Bucket> newBuckets = new ArrayList<>(buckets.size());
        double sum = 0;
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], GapPolicy.INSERT_ZEROS);

            // Only increment the sum if it's a finite value, otherwise "increment by zero" is correct
            if (thisBucketValue != null && thisBucketValue.isInfinite() == false && thisBucketValue.isNaN() == false) {
                sum += thisBucketValue;
            }
            newBuckets.add(
                factory.createBucket(
                    factory.getKey(bucket),
                    bucket.getDocCount(),
                    InternalAggregations.append(bucket.getAggregations(), new InternalSimpleValue(name(), sum, formatter, metadata()))
                )
            );
        }
        return factory.createAggregation(newBuckets);
    }
}

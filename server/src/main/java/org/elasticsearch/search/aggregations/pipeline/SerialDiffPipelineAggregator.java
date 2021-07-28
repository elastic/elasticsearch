/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class SerialDiffPipelineAggregator extends PipelineAggregator {
    private DocValueFormat formatter;
    private GapPolicy gapPolicy;
    private int lag;

    SerialDiffPipelineAggregator(String name, String[] bucketsPaths, @Nullable DocValueFormat formatter, GapPolicy gapPolicy,
                                 int lag, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.lag = lag;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        @SuppressWarnings("rawtypes")
        InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends InternalMultiBucketAggregation.InternalBucket>
                histo = (InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends
                InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>();
        EvictingQueue<Double> lagWindow = new EvictingQueue<>(lag);
        int counter = 0;

        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);
            Bucket newBucket = bucket;

            counter += 1;

            // Still under the initial lag period, add nothing and move on
            Double lagValue;
            if (counter <= lag) {
                lagValue = Double.NaN;
            } else {
                lagValue = lagWindow.peek();  // Peek here, because we rely on add'ing to always move the window
            }

            // Normalize null's to NaN
            if (thisBucketValue == null) {
                thisBucketValue = Double.NaN;
            }

            // Both have values, calculate diff and replace the "empty" bucket
            if (Double.isNaN(thisBucketValue) == false && Double.isNaN(lagValue) == false) {
                double diff = thisBucketValue - lagValue;

                List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map(
                        (p) -> (InternalAggregation) p).collect(Collectors.toList());
                aggs.add(new InternalSimpleValue(name(), diff, formatter, metadata()));
                newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(aggs));
            }

            newBuckets.add(newBucket);
            lagWindow.add(thisBucketValue);
        }
        return factory.createAggregation(newBuckets);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.normalize;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class NormalizePipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final Function<double[], DoubleUnaryOperator> normalizerSupplier;

    NormalizePipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter,
                                Function<double[], DoubleUnaryOperator> normalizerSupplier,
                                Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.normalizerSupplier = normalizerSupplier;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends InternalMultiBucketAggregation.InternalBucket>
                histo = (InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends
                InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;
        List<Bucket> newBuckets = new ArrayList<>(buckets.size());

        double[] values = buckets.stream()
            .mapToDouble(bucket -> resolveBucketValue(histo, bucket, bucketsPaths()[0], GapPolicy.SKIP)).toArray();

        DoubleUnaryOperator normalizer = normalizerSupplier.apply(values);

        for (int i = 0; i < buckets.size(); i++) {
            InternalMultiBucketAggregation.InternalBucket bucket = buckets.get(i);

            final double normalizedBucketValue;

            // Only account for finite values
            if (Double.isNaN(values[i])) {
                normalizedBucketValue = Double.NaN;
            } else {
                normalizedBucketValue = normalizer.applyAsDouble(values[i]);
            }

            List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                .map((p) -> (InternalAggregation) p)
                .collect(Collectors.toList());
            aggs.add(new InternalSimpleValue(name(), normalizedBucketValue, formatter, metadata()));
            Bucket newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), new InternalAggregations(aggs));
            newBuckets.add(newBucket);
        }

        return factory.createAggregation(newBuckets);
    }
}

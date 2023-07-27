/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.movingPercentiles;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MovingPercentilesPipelineAggregator extends PipelineAggregator {

    private final int window;
    private final int shift;

    MovingPercentilesPipelineAggregator(String name, String[] bucketsPaths, int window, int shift, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.window = window;
        this.shift = shift;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, AggregationReduceContext reduceContext) {
        @SuppressWarnings("unchecked")
        InternalMultiBucketAggregation<?, ?> histo = (InternalMultiBucketAggregation<?, ?>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>(buckets.size());
        if (buckets.size() == 0) {
            return factory.createAggregation(newBuckets);
        }
        PercentileConfig config = resolvePercentileConfig(histo, buckets.get(0), bucketsPaths()[0]);
        switch (config.method) {
            case TDIGEST -> reduceTDigest(buckets, histo, newBuckets, factory, config);
            case HDR -> reduceHDR(buckets, histo, newBuckets, factory, config);
            default -> throw new AggregationExecutionException(
                AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
                    + " references an unknown percentile aggregation method: ["
                    + config.method
                    + "]"
            );
        }
        return factory.createAggregation(newBuckets);
    }

    private void reduceTDigest(
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets,
        MultiBucketsAggregation histo,
        List<Bucket> newBuckets,
        HistogramFactory factory,
        PercentileConfig config
    ) {

        List<TDigestState> values = buckets.stream()
            .map(b -> resolveTDigestBucketValue(histo, b, bucketsPaths()[0]))
            .filter(v -> v != null)
            .toList();

        int index = 0;
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {

            // Default is to reuse existing bucket. Simplifies the rest of the logic,
            // since we only change newBucket if we can add to it
            MultiBucketsAggregation.Bucket newBucket = bucket;

            TDigestState state = null;
            int fromIndex = clamp(index - window + shift, values.size());
            int toIndex = clamp(index + shift, values.size());
            for (int i = fromIndex; i < toIndex; i++) {
                TDigestState bucketState = values.get(i);
                if (bucketState != null) {
                    if (state == null) {
                        // We have to create a new TDigest histogram because otherwise it will alter the
                        // existing histogram and bucket value
                        state = TDigestState.createUsingParamsFrom(bucketState);
                    }
                    state.add(bucketState);

                }
            }

            if (state != null) {
                List<InternalAggregation> aggs = bucket.getAggregations()
                    .asList()
                    .stream()
                    .map((p) -> (InternalAggregation) p)
                    .collect(Collectors.toList());
                aggs.add(new InternalTDigestPercentiles(name(), config.keys, state, config.keyed, config.formatter, metadata()));
                newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(aggs));
            }
            newBuckets.add(newBucket);
            index++;
        }
    }

    private void reduceHDR(
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets,
        MultiBucketsAggregation histo,
        List<Bucket> newBuckets,
        HistogramFactory factory,
        PercentileConfig config
    ) {

        List<DoubleHistogram> values = buckets.stream()
            .map(b -> resolveHDRBucketValue(histo, b, bucketsPaths()[0]))
            .filter(v -> v != null)
            .toList();

        int index = 0;
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            DoubleHistogram state = null;

            // Default is to reuse existing bucket. Simplifies the rest of the logic,
            // since we only change newBucket if we can add to it
            MultiBucketsAggregation.Bucket newBucket = bucket;

            int fromIndex = clamp(index - window + shift, values.size());
            int toIndex = clamp(index + shift, values.size());
            for (int i = fromIndex; i < toIndex; i++) {
                DoubleHistogram bucketState = values.get(i);
                if (bucketState != null) {
                    if (state == null) {
                        // We have to create a new HDR histogram because otherwise it will alter the
                        // existing histogram and bucket value
                        state = new DoubleHistogram(bucketState.getNumberOfSignificantValueDigits());
                    }
                    state.add(bucketState);

                }
            }

            if (state != null) {
                List<InternalAggregation> aggs = bucket.getAggregations()
                    .asList()
                    .stream()
                    .map((p) -> (InternalAggregation) p)
                    .collect(Collectors.toList());
                aggs.add(new InternalHDRPercentiles(name(), config.keys, state, config.keyed, config.formatter, metadata()));
                newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(aggs));
            }
            newBuckets.add(newBucket);
            index++;
        }
    }

    private static PercentileConfig resolvePercentileConfig(
        MultiBucketsAggregation agg,
        InternalMultiBucketAggregation.InternalBucket bucket,
        String aggPath
    ) {
        List<String> aggPathsList = AggregationPath.parse(aggPath).getPathElementsAsStringList();
        Object propertyValue = bucket.getProperty(agg.getName(), aggPathsList);
        if (propertyValue == null) {
            throw buildResolveError(agg, aggPathsList, propertyValue, "percentiles");
        }

        if (propertyValue instanceof InternalTDigestPercentiles internalTDigestPercentiles) {
            return new PercentileConfig(
                PercentilesMethod.TDIGEST,
                internalTDigestPercentiles.getKeys(),
                internalTDigestPercentiles.keyed(),
                internalTDigestPercentiles.formatter()
            );
        }
        if (propertyValue instanceof InternalHDRPercentiles internalHDRPercentiles) {
            return new PercentileConfig(
                PercentilesMethod.HDR,
                internalHDRPercentiles.getKeys(),
                internalHDRPercentiles.keyed(),
                internalHDRPercentiles.formatter()
            );
        }
        throw buildResolveError(agg, aggPathsList, propertyValue, "percentiles");
    }

    private static TDigestState resolveTDigestBucketValue(
        MultiBucketsAggregation agg,
        InternalMultiBucketAggregation.InternalBucket bucket,
        String aggPath
    ) {
        List<String> aggPathsList = AggregationPath.parse(aggPath).getPathElementsAsStringList();
        Object propertyValue = bucket.getProperty(agg.getName(), aggPathsList);
        if (propertyValue == null || (propertyValue instanceof InternalTDigestPercentiles) == false) {
            throw buildResolveError(agg, aggPathsList, propertyValue, "TDigest");
        }
        return ((InternalTDigestPercentiles) propertyValue).getState();
    }

    private static DoubleHistogram resolveHDRBucketValue(
        MultiBucketsAggregation agg,
        InternalMultiBucketAggregation.InternalBucket bucket,
        String aggPath
    ) {
        List<String> aggPathsList = AggregationPath.parse(aggPath).getPathElementsAsStringList();
        Object propertyValue = bucket.getProperty(agg.getName(), aggPathsList);
        if (propertyValue == null || (propertyValue instanceof InternalHDRPercentiles) == false) {
            throw buildResolveError(agg, aggPathsList, propertyValue, "HDR");
        }
        return ((InternalHDRPercentiles) propertyValue).getState();
    }

    private static IllegalArgumentException buildResolveError(
        MultiBucketsAggregation agg,
        List<String> aggPathsList,
        Object propertyValue,
        String method
    ) {
        if (propertyValue == null) {
            return new IllegalArgumentException(
                AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
                    + " must reference a "
                    + method
                    + " percentile aggregation"
            );
        } else {
            String currentAggName;
            if (aggPathsList.isEmpty()) {
                currentAggName = agg.getName();
            } else {
                currentAggName = aggPathsList.get(0);
            }
            return new IllegalArgumentException(
                AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
                    + " must reference a "
                    + method
                    + " percentiles aggregation, got: ["
                    + propertyValue.getClass().getSimpleName()
                    + "] at aggregation ["
                    + currentAggName
                    + "]"
            );
        }
    }

    private static int clamp(int index, int length) {
        if (index < 0) {
            return 0;
        }
        if (index > length) {
            return length;
        }
        return index;
    }

    // TODO: replace this with the PercentilesConfig that's used by the percentiles builder.
    // The config isn't available through the Internal objects

    /**
     * helper record to collect the percentile's configuration
     */
    private record PercentileConfig(PercentilesMethod method, double[] keys, boolean keyed, DocValueFormat formatter) {}
}

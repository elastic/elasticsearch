/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.bucket.range;

import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.aggregations.support.ExponentialHistogramValuesSource;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;

import java.io.IOException;
import java.util.Map;

/**
 * Class for supporting range aggregation on exponential histogram mapped fields.
 */
public abstract class ExponentialHistogramBackedRangeAggregator extends RangeAggregator {

    record SearchBounds(int lo, int hi) {}

    public static ExponentialHistogramBackedRangeAggregator build(
        String name,
        AggregatorFactories factories,
        ValuesSourceConfig valuesSourceConfig,
        InternalRange.Factory<?, ?> rangeFactory,
        RangeAggregator.Range[] ranges,
        boolean keyed,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        final double avgRange = ((double) context.searcher().getIndexReader().maxDoc()) / ranges.length;
        if (hasOverlap(ranges)) {
            return new Overlap(
                name,
                factories,
                valuesSourceConfig.getValuesSource(),
                valuesSourceConfig.format(),
                rangeFactory,
                ranges,
                avgRange,
                keyed,
                context,
                parent,
                cardinality,
                metadata
            );
        }
        return new NoOverlap(
            name,
            factories,
            valuesSourceConfig.getValuesSource(),
            valuesSourceConfig.format(),
            rangeFactory,
            ranges,
            avgRange,
            keyed,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    @SuppressWarnings("this-escape")
    public ExponentialHistogramBackedRangeAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource valuesSource,
        DocValueFormat format,
        InternalRange.Factory<?, ?> rangeFactory,
        Range[] ranges,
        double averageDocsPerRange,
        boolean keyed,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(
            name,
            factories,
            valuesSource,
            format,
            rangeFactory,
            ranges,
            averageDocsPerRange,
            keyed,
            context,
            parent,
            cardinality,
            metadata
        );
        if (subAggregators().length > 0) {
            throw new IllegalArgumentException("Range aggregation on exponential_histogram fields does not support sub-aggregations");
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        if ((valuesSource instanceof ExponentialHistogramValuesSource.ExponentialHistogram) == false) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final ExponentialHistogramValuesSource.ExponentialHistogram expHistoSource =
            (ExponentialHistogramValuesSource.ExponentialHistogram) this.valuesSource;
        final ExponentialHistogramValuesReader values = expHistoSource.getHistogramValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final ExponentialHistogram histo = values.histogramValue();

                    // Negative bucket centers are emitted in descending order (most negative first),
                    // so we only narrow hi from each result.
                    int hi = ranges.length - 1;
                    BucketIterator negIt = histo.negativeBuckets().iterator();
                    while (negIt.hasNext()) {
                        double center = -ExponentialScaleUtils.getPointOfLeastRelativeError(negIt.peekIndex(), negIt.scale());
                        center = Math.clamp(center, histo.min(), histo.max());
                        final long count = negIt.peekCount() - docCountProvider.getDocCount(doc);
                        hi = ExponentialHistogramBackedRangeAggregator.this.collect(
                            sub,
                            doc,
                            center,
                            bucket,
                            new SearchBounds(0, hi),
                            count
                        ).hi;
                        negIt.advance();
                    }

                    // Positive bucket centers (including zero bucket) are emitted in ascending order (smallest first),
                    // so we only narrow lo from each result.
                    int lo = 0;
                    if (histo.zeroBucket().count() > 0) {
                        final long count = histo.zeroBucket().count() - docCountProvider.getDocCount(doc);
                        lo = ExponentialHistogramBackedRangeAggregator.this.collect(
                            sub,
                            doc,
                            0.0,
                            bucket,
                            new SearchBounds(0, ranges.length - 1),
                            count
                        ).lo;
                    }

                    BucketIterator posIt = histo.positiveBuckets().iterator();
                    while (posIt.hasNext()) {
                        double center = ExponentialScaleUtils.getPointOfLeastRelativeError(posIt.peekIndex(), posIt.scale());
                        center = Math.clamp(center, histo.min(), histo.max());
                        final long count = posIt.peekCount() - docCountProvider.getDocCount(doc);
                        lo = ExponentialHistogramBackedRangeAggregator.this.collect(
                            sub,
                            doc,
                            center,
                            bucket,
                            new SearchBounds(lo, ranges.length - 1),
                            count
                        ).lo;
                        posIt.advance();
                    }
                }
            }
        };
    }

    /**
     * Collect a value into matching range buckets. The search is bounded by the given {@link SearchBounds},
     * and returns updated bounds to narrow subsequent searches when values are processed in monotonic order.
     */
    abstract SearchBounds collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, SearchBounds bounds, long count)
        throws IOException;

    private static class NoOverlap extends ExponentialHistogramBackedRangeAggregator {

        private NoOverlap(
            String name,
            AggregatorFactories factories,
            ValuesSource valuesSource,
            DocValueFormat format,
            InternalRange.Factory<?, ?> rangeFactory,
            Range[] ranges,
            double averageDocsPerRange,
            boolean keyed,
            AggregationContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(
                name,
                factories,
                valuesSource,
                format,
                rangeFactory,
                ranges,
                averageDocsPerRange,
                keyed,
                context,
                parent,
                cardinality,
                metadata
            );
        }

        @Override
        SearchBounds collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, SearchBounds bounds, long count)
            throws IOException {
            int lo = bounds.lo, hi = bounds.hi;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                if (value < ranges[mid].getFrom()) {
                    hi = mid - 1;
                } else if (value >= ranges[mid].getTo()) {
                    lo = mid + 1;
                } else {
                    long bucketOrd = subBucketOrdinal(owningBucketOrdinal, mid);
                    collectBucket(sub, doc, bucketOrd);
                    incrementBucketDocCount(bucketOrd, count);
                    // Multiple values may fall in the same range, so don't advance past mid
                    return new SearchBounds(mid, mid);
                }
            }
            return new SearchBounds(lo, hi);
        }
    }

    private static class Overlap extends ExponentialHistogramBackedRangeAggregator {

        private final double[] maxTo;

        Overlap(
            String name,
            AggregatorFactories factories,
            ValuesSource valuesSource,
            DocValueFormat format,
            InternalRange.Factory<?, ?> rangeFactory,
            Range[] ranges,
            double averageDocsPerRange,
            boolean keyed,
            AggregationContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(
                name,
                factories,
                valuesSource,
                format,
                rangeFactory,
                ranges,
                averageDocsPerRange,
                keyed,
                context,
                parent,
                cardinality,
                metadata
            );
            maxTo = new double[ranges.length];
            maxTo[0] = ranges[0].getTo();
            for (int i = 1; i < ranges.length; ++i) {
                maxTo[i] = Math.max(ranges[i].getTo(), maxTo[i - 1]);
            }
        }

        @Override
        SearchBounds collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, SearchBounds bounds, long count)
            throws IOException {
            int lo = bounds.lo, hi = bounds.hi;
            int mid = (lo + hi) >>> 1;
            while (lo <= hi) {
                if (value < ranges[mid].getFrom()) {
                    hi = mid - 1;
                } else if (value >= maxTo[mid]) {
                    lo = mid + 1;
                } else {
                    break;
                }
                mid = (lo + hi) >>> 1;
            }
            if (lo > hi) {
                return new SearchBounds(lo, hi);
            }

            // binary search the lower bound
            int startLo = lo, startHi = mid;
            while (startLo <= startHi) {
                final int startMid = (startLo + startHi) >>> 1;
                if (value >= maxTo[startMid]) {
                    startLo = startMid + 1;
                } else {
                    startHi = startMid - 1;
                }
            }

            // binary search the upper bound
            int endLo = mid, endHi = hi;
            while (endLo <= endHi) {
                final int endMid = (endLo + endHi) >>> 1;
                if (value < ranges[endMid].getFrom()) {
                    endHi = endMid - 1;
                } else {
                    endLo = endMid + 1;
                }
            }

            for (int i = startLo; i <= endHi; ++i) {
                if (ranges[i].matches(value)) {
                    long bucketOrd = subBucketOrdinal(owningBucketOrdinal, i);
                    collectBucket(sub, doc, bucketOrd);
                    incrementBucketDocCount(bucketOrd, count);
                }
            }
            return new SearchBounds(startLo, endHi);
        }
    }
}

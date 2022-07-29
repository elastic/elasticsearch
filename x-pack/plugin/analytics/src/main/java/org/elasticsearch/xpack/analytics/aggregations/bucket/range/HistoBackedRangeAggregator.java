/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.bucket.range;

import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
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
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * Class for supporting range aggregation on histogram mapped fields
 */
public abstract class HistoBackedRangeAggregator extends RangeAggregator {

    // TODO it would be good one day to possibly interpolate between ranges in the histogram fields
    // If we knew the underlying data structure that created the histogram value, we could provide more accurate
    // data counts for the ranges
    public static HistoBackedRangeAggregator build(
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

    public HistoBackedRangeAggregator(
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
            throw new IllegalArgumentException("Range aggregation on histogram fields does not support sub-aggregations");
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        if ((valuesSource instanceof HistogramValuesSource.Histogram) == false) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final HistogramValuesSource.Histogram valuesSource = (HistogramValuesSource.Histogram) this.valuesSource;
        final HistogramValues values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();
                    double previousValue = Double.NEGATIVE_INFINITY;
                    int lo = 0;
                    // Histogram values are expected to be in ascending order.
                    while (sketch.next()) {
                        final double value = sketch.value();
                        assert previousValue <= value : "histogram field [" + name + "] unexpectedly out of order";
                        previousValue = value;
                        // Collecting the bucket automatically increments the count by the docCountProvider,
                        // account for that here
                        final int count = sketch.count() - docCountProvider.getDocCount(doc);
                        lo = HistoBackedRangeAggregator.this.collect(sub, doc, value, bucket, lo, count);
                    }
                }
            }
        };
    }

    abstract int collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, int lowBound, int count)
        throws IOException;

    private static class NoOverlap extends HistoBackedRangeAggregator {

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
        public int collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, int lowBound, int count)
            throws IOException {
            int lo = lowBound, hi = ranges.length - 1;
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
                    // It could be that multiple histogram values fall in the same range
                    // So, don't increment the final mid here to catch those values
                    return mid;
                }
            }
            return lo;
        }
    }

    private static class Overlap extends HistoBackedRangeAggregator {

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
        public int collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, int lowBound, int count)
            throws IOException {
            int lo = lowBound, hi = ranges.length - 1; // all candidates are between these indexes
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
            // No candidate range found, return current lo
            if (lo > hi) return lo;

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

            assert startLo == lowBound || value >= maxTo[startLo - 1];
            assert endHi == ranges.length - 1 || value < ranges[endHi + 1].getFrom();

            for (int i = startLo; i <= endHi; ++i) {
                if (ranges[i].matches(value)) {
                    long bucketOrd = subBucketOrdinal(owningBucketOrdinal, i);
                    collectBucket(sub, doc, bucketOrd);
                    incrementBucketDocCount(bucketOrd, count);
                }
            }
            // It could be that multiple histogram values fall in the same range
            // So, return the bottom part of the search
            return startLo;
        }
    }
}

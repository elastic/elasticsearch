/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram.aggregations.bucket.histogram;

import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.histogram.AbstractHistogramAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DoubleBounds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.support.ExponentialHistogramValuesSource;
import org.elasticsearch.xpack.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;

import java.io.IOException;
import java.util.Map;

public final class ExponentialHistogramBackedHistogramAggregator extends AbstractHistogramAggregator {

    private final ExponentialHistogramValuesSource.ExponentialHistogram valuesSource;

    public ExponentialHistogramBackedHistogramAggregator(
        String name,
        AggregatorFactories factories,
        double interval,
        double offset,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        DoubleBounds extendedBounds,
        DoubleBounds hardBounds,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinalityUpperBound,
        Map<String, Object> metadata
    ) throws IOException {
        super(
            name,
            factories,
            interval,
            offset,
            order,
            keyed,
            minDocCount,
            extendedBounds,
            hardBounds,
            valuesSourceConfig.format(),
            context,
            parent,
            cardinalityUpperBound,
            metadata
        );

        this.valuesSource = (ExponentialHistogramValuesSource.ExponentialHistogram) valuesSourceConfig.getValuesSource();

        // Sub aggregations are not allowed when running histogram agg over histograms
        if (subAggregators().length > 0) {
            throw new IllegalArgumentException("Histogram aggregation on histogram fields does not support sub-aggregations");
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        ExponentialHistogramValuesReader values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    ExponentialHistogram histo = values.histogramValue();
                    forEachBucketCenter(histo, (center, count) -> {
                        double key = Math.floor((center - offset) / interval);
                        if (hardBounds == null || hardBounds.contain(key * interval)) {
                            long bucketOrd = bucketOrds.add(owningBucketOrd, Double.doubleToLongBits(key));
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                            // We have added the document already and we have incremented bucket doc_count
                            // by _doc_count times. To compensate for this, we should increment doc_count by
                            // (count - _doc_count) so that we have added it count times.
                            incrementBucketDocCount(bucketOrd, count - docCountProvider.getDocCount(doc));
                        }
                    });
                }
            }
        };
    }

    @FunctionalInterface
    private interface BucketCenterConsumer {
        void accept(double bucketCenter, long count) throws IOException;
    }

    private static void forEachBucketCenter(ExponentialHistogram histo, BucketCenterConsumer consumer) throws IOException {
        BucketIterator negIt = histo.negativeBuckets().iterator();
        while (negIt.hasNext()) {
            double center = -ExponentialScaleUtils.getPointOfLeastRelativeError(negIt.peekIndex(), negIt.scale());
            center = Math.clamp(center, histo.min(), histo.max());
            consumer.accept(center, negIt.peekCount());
            negIt.advance();
        }
        if (histo.zeroBucket().count() > 0) {
            consumer.accept(0.0, histo.zeroBucket().count());
        }
        BucketIterator posIt = histo.positiveBuckets().iterator();
        while (posIt.hasNext()) {
            double center = ExponentialScaleUtils.getPointOfLeastRelativeError(posIt.peekIndex(), posIt.scale());
            center = Math.clamp(center, histo.min(), histo.max());
            consumer.accept(center, posIt.peekCount());
            posIt.advance();
        }
    }

}

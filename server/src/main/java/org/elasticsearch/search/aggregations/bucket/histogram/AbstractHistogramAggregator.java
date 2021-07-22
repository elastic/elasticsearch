/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.EmptyBucketInfo;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.search.aggregations.bucket.histogram.DoubleBounds.getEffectiveMax;
import static org.elasticsearch.search.aggregations.bucket.histogram.DoubleBounds.getEffectiveMin;

/**
 * Base class for functionality shared between aggregators for this
 * {@code histogram} aggregation.
 */
public abstract class AbstractHistogramAggregator extends BucketsAggregator {
    protected final DocValueFormat formatter;
    protected final double interval;
    protected final double offset;
    protected final BucketOrder order;
    protected final boolean keyed;
    protected final long minDocCount;
    protected final DoubleBounds extendedBounds;
    protected final DoubleBounds hardBounds;
    protected final LongKeyedBucketOrds bucketOrds;

    public AbstractHistogramAggregator(
        String name,
        AggregatorFactories factories,
        double interval,
        double offset,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        DoubleBounds extendedBounds,
        DoubleBounds hardBounds,
        DocValueFormat formatter,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinalityUpperBound,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        if (interval <= 0) {
            throw new IllegalArgumentException("interval must be positive, got: " + interval);
        }
        this.interval = interval;
        this.offset = offset;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
        this.formatter = formatter;
        bucketOrds = LongKeyedBucketOrds.build(bigArrays(), cardinalityUpperBound);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds,
            (bucketValue, docCount, subAggregationResults) -> {
                double roundKey = Double.longBitsToDouble(bucketValue);
                double key = roundKey * interval + offset;
                return new InternalHistogram.Bucket(key, docCount, keyed, formatter, subAggregationResults);
            }, (owningBucketOrd, buckets) -> {
                // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                EmptyBucketInfo emptyBucketInfo = null;
                if (minDocCount == 0) {
                    emptyBucketInfo = new EmptyBucketInfo(interval, offset, getEffectiveMin(extendedBounds),
                        getEffectiveMax(extendedBounds), buildEmptySubAggregations());
                }
                return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed, metadata());
            });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = null;
        if (minDocCount == 0) {
            emptyBucketInfo = new InternalHistogram.EmptyBucketInfo(interval, offset, getEffectiveMin(extendedBounds),
                getEffectiveMax(extendedBounds), buildEmptySubAggregations());
        }
        return new InternalHistogram(name, Collections.emptyList(), order, minDocCount, emptyBucketInfo, formatter, keyed, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("total_buckets", bucketOrds.size());
        super.collectDebugInfo(add);
    }
}

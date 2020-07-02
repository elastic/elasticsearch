/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.EmptyBucketInfo;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An aggregator for numeric values. For a given {@code interval},
 * {@code offset} and {@code value}, it returns the highest number that can be
 * written as {@code interval * x + offset} and yet is less than or equal to
 * {@code value}.
 */
public class HistoBackedHistogramAggregator extends BucketsAggregator {

    private final HistogramValuesSource.Histogram valuesSource;
    protected final DocValueFormat formatter;
    protected final double interval;
    protected final double offset;
    protected final BucketOrder order;
    protected final boolean keyed;
    protected final long minDocCount;
    protected final double minBound;
    protected final double maxBound;
    protected final LongKeyedBucketOrds bucketOrds;

    public HistoBackedHistogramAggregator(
        String name,
        AggregatorFactories factories,
        double interval,
        double offset,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        double minBound,
        double maxBound,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext context,
        Aggregator parent,
        boolean collectsFromSingleBucket,
        Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, metadata);
        if (interval <= 0) {
            throw new IllegalArgumentException("interval must be positive, got: " + interval);
        }
        this.interval = interval;
        this.offset = offset;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.minBound = minBound;
        this.maxBound = maxBound;
        this.formatter = valuesSourceConfig.format();
        this.bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), collectsFromSingleBucket);
        // TODO: Stop using null here
        this.valuesSource = valuesSourceConfig.hasValues() ? (HistogramValuesSource.Histogram) valuesSourceConfig.getValuesSource() : null;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final HistogramValues values = valuesSource.getHistogramValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();

                    double previousKey = Double.NEGATIVE_INFINITY;
                    while (sketch.next()) {
                        final int valuesCount = sketch.count();
                        double value = sketch.value();

                        for (int i = 0; i < valuesCount; ++i) {
                            double key = Math.floor((value - offset) / interval);
                            assert key >= previousKey;
                            if (key == previousKey) {
                                continue;
                            }
                            long bucketOrd = bucketOrds.add(owningBucketOrd, Double.doubleToLongBits(key));
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                            previousKey = key;
                        }
                    }
                }
            }
        };
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
                    emptyBucketInfo = new EmptyBucketInfo(interval, offset, minBound, maxBound, buildEmptySubAggregations());
                }
                return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed, metadata());
            });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = null;
        if (minDocCount == 0) {
            emptyBucketInfo = new InternalHistogram.EmptyBucketInfo(interval, offset, minBound, maxBound, buildEmptySubAggregations());
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

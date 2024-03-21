/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregatorsReducer;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.ArrayList;
import java.util.List;

/**
 *  Reduces aggregations where buckets are represented by a long key. It uses a {@link LongObjectPagedHashMap}
 *  to keep track of the different buckets.
 */
abstract class LongKeyedMultiBucketsAggregatorReducer<B extends MultiBucketsAggregation.Bucket> implements Releasable {

    private final AggregationReduceContext reduceContext;
    private final int size;
    private final long minDocCount;
    private final LongObjectPagedHashMap<MultiBucketAggregatorsReducer> bucketsReducer;
    int consumeBucketCount = 0;

    LongKeyedMultiBucketsAggregatorReducer(AggregationReduceContext reduceContext, int size, long minDocCount) {
        this.reduceContext = reduceContext;
        this.size = size;
        this.minDocCount = minDocCount;
        bucketsReducer = new LongObjectPagedHashMap<>(size, reduceContext.bigArrays());
    }

    /**
     * The bucket to reduce with its corresponding long key.
     */
    public final void accept(long key, B bucket) {
        MultiBucketAggregatorsReducer reducer = bucketsReducer.get(key);
        if (reducer == null) {
            reducer = new MultiBucketAggregatorsReducer(reduceContext, size);
            bucketsReducer.put(key, reducer);
        }
        consumeBucketsAndMaybeBreak(reducer, bucket);
        reducer.accept(bucket);
    }

    private void consumeBucketsAndMaybeBreak(MultiBucketAggregatorsReducer reducer, B bucket) {
        if (reduceContext.isFinalReduce() == false || minDocCount == 0) {
            if (reducer.getDocCount() == 0 && bucket.getDocCount() > 0) {
                consumeBucketsAndMaybeBreak();
            }
        } else {
            if (reducer.getDocCount() < minDocCount && (reducer.getDocCount() + bucket.getDocCount()) >= minDocCount) {
                consumeBucketsAndMaybeBreak();
            }
        }
    }

    private void consumeBucketsAndMaybeBreak() {
        if (consumeBucketCount++ >= InternalMultiBucketAggregation.REPORT_EMPTY_EVERY) {
            reduceContext.consumeBucketsAndMaybeBreak(consumeBucketCount);
            consumeBucketCount = 0;
        }
    }

    /**
     * Returns the reduced buckets.
     */
    public final List<B> get() {
        reduceContext.consumeBucketsAndMaybeBreak(consumeBucketCount);
        final List<B> reducedBuckets = new ArrayList<>((int) bucketsReducer.size());
        bucketsReducer.iterator().forEachRemaining(entry -> {
            if (reduceContext.isFinalReduce() == false || entry.value.getDocCount() >= minDocCount) {
                reducedBuckets.add(createBucket(entry.key, entry.value.getDocCount(), entry.value.get()));
            }
        });
        return reducedBuckets;
    }

    /**
     * Builds a bucket provided the key, the number of documents and the sub-aggregations.
     */
    protected abstract B createBucket(long key, long docCount, InternalAggregations aggregations);

    @Override
    public final void close() {
        bucketsReducer.iterator().forEachRemaining(r -> Releasables.close(r.value));
        Releasables.close(bucketsReducer);
    }
}

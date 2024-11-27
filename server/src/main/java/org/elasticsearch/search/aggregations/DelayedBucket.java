/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import java.util.List;
import java.util.function.BiFunction;

/**
 * A wrapper around reducing buckets with the same key that can delay that reduction
 * as long as possible. It's stateful and not even close to thread safe.
 * <p>
 * It is responsibility of the caller to account for buckets created using DelayedBucket.
 * It should call {@link #nonCompetitive} to release any possible sub-bucket creation if
 * a bucket is rejected from the final response.
 */
public final class DelayedBucket<B extends InternalMultiBucketAggregation.InternalBucket> {
    /**
     * The buckets to reduce or {@code null} if we've already reduced the buckets.
     */
    private List<B> toReduce;
    /**
     * The result of reducing the buckets or {@code null} if they haven't yet been
     * reduced.
     */
    private B reduced;
    /**
     * The count of documents. Calculated on the fly the first time its needed and
     * cached.
     */
    private long docCount = -1;

    /**
     * Build a delayed bucket.
     */
    public DelayedBucket(List<B> toReduce) {
        this.toReduce = toReduce;
    }

    /**
     * The reduced bucket. If the bucket hasn't been reduced already this
     * will reduce the sub-aggs and throw out the list to reduce.
     */
    public B reduced(BiFunction<List<B>, AggregationReduceContext, B> reduce, AggregationReduceContext reduceContext) {
        if (reduced == null) {
            reduced = reduce.apply(toReduce, reduceContext);
            toReduce = null;
        }
        return reduced;
    }

    /**
     * Count the documents in the buckets.
     */
    public long getDocCount() {
        if (docCount < 0) {
            if (reduced == null) {
                docCount = 0;
                for (B bucket : toReduce) {
                    docCount += bucket.getDocCount();
                }
            } else {
                docCount = reduced.getDocCount();
            }
        }
        return docCount;
    }

    /**
     * Compare the keys of two buckets.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" }) // The funny casting here is sad, but this is how buckets are compared.
    int compareKey(DelayedBucket<?> rhs) {
        return ((KeyComparable) representativeBucket()).compareKey(rhs.representativeBucket());
    }

    /**
     * A representative of the buckets used to acess the key.
     */
    private B representativeBucket() {
        return reduced == null ? toReduce.get(0) : reduced;
    }

    @Override
    public String toString() {
        return "Delayed[" + representativeBucket().getKeyAsString() + "]";
    }

    /**
     * Called to mark a bucket as non-competitive so it can release it can release
     * any sub-buckets from the breaker.
     */
    void nonCompetitive(AggregationReduceContext reduceContext) {
        if (reduced != null) {
            // -countInnerBucket for all the sub-buckets.
            reduceContext.consumeBucketsAndMaybeBreak(-InternalMultiBucketAggregation.countInnerBucket(reduced));
        }
    }
}

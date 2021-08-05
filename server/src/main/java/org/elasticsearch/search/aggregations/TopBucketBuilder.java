/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Merges many buckets into the "top" buckets as sorted by {@link BucketOrder}.
 */
public class TopBucketBuilder<B extends InternalMultiBucketAggregation.InternalBucket> {
    private final Consumer<DelayedBucket<B>> nonCompetitive;
    private final PriorityQueue<DelayedBucket<B>> queue;

    /**
     * Create a {@link TopBucketBuilder} to build a list of the top buckets.
     * @param size the requested size of the list
     * @param order the sort order of the buckets
     * @param nonCompetitive called with non-competitive buckets
     */
    public TopBucketBuilder(int size, BucketOrder order, Consumer<DelayedBucket<B>> nonCompetitive) {
        this.nonCompetitive = nonCompetitive;
        queue = new PriorityQueue<DelayedBucket<B>>(size) {
            private final Comparator<DelayedBucket<? extends Bucket>> comparator = order.delayedBucketComparator();

            @Override
            protected boolean lessThan(DelayedBucket<B> a, DelayedBucket<B> b) {
                return comparator.compare(a, b) > 0;
            }
        };
    }

    /**
     * Add a bucket if it is competitive. If there isn't space but the
     * bucket is competitive then this will drop the least competitive bucket
     * to make room for the new bucket.
     * <p>
     * Instead of operating on complete buckets we this operates on a
     * wrapper containing what we need to merge the buckets called
     * {@link DelayedBucket}. We can evaluate some common sort criteria
     * directly on the {@linkplain DelayedBucket}s so we only need to
     * merge <strong>exactly</strong> the sub-buckets we need.
     */
    public void add(DelayedBucket<B> bucket) {
        DelayedBucket<B> removed = queue.insertWithOverflow(bucket);
        if (removed != null) {
            nonCompetitive.accept(removed);
            removed.nonCompetitive();
        }
    }

    /**
     * Return the most competitive buckets sorted by the comparator.
     */
    public List<B> build() {
        List<B> result = new ArrayList<>(queue.size());
        for (int i = queue.size() - 1; i >= 0; i--) {
            result.add(queue.pop().reduced());
        }
        Collections.reverse(result);
        return result;
    }
}

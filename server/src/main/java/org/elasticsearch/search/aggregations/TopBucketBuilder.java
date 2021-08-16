/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.ArrayUtil;
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
public abstract class TopBucketBuilder<B extends InternalMultiBucketAggregation.InternalBucket> {
    /**
     * The number of buckets required before we switch to the
     * {@link BufferingTopBucketBuilder}. If we need fewer buckets we use
     * {@link PriorityQueueTopBucketBuilder}.
     */
    static final int USE_BUFFERING_BUILDER = 1024;

    /**
     * Create a {@link TopBucketBuilder} to build a list of the top buckets.
     * <p>
     * If there are few required results we use a {@link PriorityQueueTopBucketBuilder}
     * which is simpler and when the priority queue is full but allocates {@code size + 1}
     * slots in an array. If there are many required results we prefer a
     * {@link BufferingTopBucketBuilder} which doesn't preallocate and is faster for the
     * first {@code size} results. But it's a little slower when the priority queue is full.
     * <p>
     * It's important for this <strong>not</strong> to preallocate a bunch of memory when
     * {@code size} is very very large because this backs the reduction of the {@code terms}
     * aggregation and folks often set the {@code size} of that to something quite large.
     * The choice in the paragraph above handles this case.
     *
     * @param size the requested size of the list
     * @param order the sort order of the buckets
     * @param nonCompetitive called with non-competitive buckets
     */
    public static <B extends InternalMultiBucketAggregation.InternalBucket> TopBucketBuilder<B> build(
        int size,
        BucketOrder order,
        Consumer<DelayedBucket<B>> nonCompetitive
    ) {
        if (size < USE_BUFFERING_BUILDER) {
            return new PriorityQueueTopBucketBuilder<>(size, order, nonCompetitive);
        }
        return new BufferingTopBucketBuilder<>(size, order, nonCompetitive);
    }

    protected final Consumer<DelayedBucket<B>> nonCompetitive;

    private TopBucketBuilder(Consumer<DelayedBucket<B>> nonCompetitive) {
        this.nonCompetitive = nonCompetitive;
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
    public abstract void add(DelayedBucket<B> bucket);

    /**
     * Return the most competitive buckets sorted by the comparator.
     */
    public abstract List<B> build();

    /**
     * Collects the "top" buckets by adding them directly to a {@link PriorityQueue}.
     * This is always going to be faster than {@link BufferingTopBucketBuilder}
     * but it requires allocating an array of {@code size + 1}.
     */
    static class PriorityQueueTopBucketBuilder<B extends InternalMultiBucketAggregation.InternalBucket> extends TopBucketBuilder<B> {
        private final PriorityQueue<DelayedBucket<B>> queue;

        PriorityQueueTopBucketBuilder(int size, BucketOrder order, Consumer<DelayedBucket<B>> nonCompetitive) {
            super(nonCompetitive);
            if (size >= ArrayUtil.MAX_ARRAY_LENGTH) {
                throw new IllegalArgumentException("can't reduce more than [" + ArrayUtil.MAX_ARRAY_LENGTH + "] buckets");
            }
            queue = new PriorityQueue<DelayedBucket<B>>(size) {
                private final Comparator<DelayedBucket<? extends Bucket>> comparator = order.delayedBucketComparator();

                @Override
                protected boolean lessThan(DelayedBucket<B> a, DelayedBucket<B> b) {
                    return comparator.compare(a, b) > 0;
                }
            };
        }

        @Override
        public void add(DelayedBucket<B> bucket) {
            DelayedBucket<B> removed = queue.insertWithOverflow(bucket);
            if (removed != null) {
                nonCompetitive.accept(removed);
                removed.nonCompetitive();
            }
        }

        @Override
        public List<B> build() {
            List<B> result = new ArrayList<>(queue.size());
            for (int i = queue.size() - 1; i >= 0; i--) {
                result.add(queue.pop().reduced());
            }
            Collections.reverse(result);
            return result;
        }
    }

    /**
     * Collects the "top" buckets by adding them to a {@link List} that grows
     * as more buckets arrive and is converting into a
     * {@link PriorityQueueTopBucketBuilder} when {@code size} buckets arrive.
     */
    private static class BufferingTopBucketBuilder<B extends InternalMultiBucketAggregation.InternalBucket> extends TopBucketBuilder<B> {
        private final int size;
        private final BucketOrder order;

        private List<DelayedBucket<B>> buffer;
        private PriorityQueueTopBucketBuilder<B> next;

        BufferingTopBucketBuilder(int size, BucketOrder order, Consumer<DelayedBucket<B>> nonCompetitive) {
            super(nonCompetitive);
            this.size = size;
            this.order = order;
            buffer = new ArrayList<>();
        }

        @Override
        public void add(DelayedBucket<B> bucket) {
            if (next != null) {
                assert buffer == null;
                next.add(bucket);
                return;
            }
            buffer.add(bucket);
            if (buffer.size() < size) {
                return;
            }
            next = new PriorityQueueTopBucketBuilder<>(size, order, nonCompetitive);
            for (DelayedBucket<B> b : buffer) {
                next.queue.add(b);
            }
            buffer = null;
        }

        @Override
        public List<B> build() {
            if (next != null) {
                assert buffer == null;
                return next.build();
            }
            List<B> result = new ArrayList<>(buffer.size());
            for (DelayedBucket<B> b : buffer) {
                result.add(b.reduced());
            }
            result.sort(order.comparator());
            return result;
        }
    }
}

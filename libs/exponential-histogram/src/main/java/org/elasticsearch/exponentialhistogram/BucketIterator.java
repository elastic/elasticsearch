/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

/**
 * An iterator over the non-empty buckets of the histogram for either the positive or negative range.
 * <ul>
 *     <li>The iterator always iterates from the lowest bucket index to the highest.</li>
 *     <li>The iterator never returns duplicate buckets (buckets with the same index).</li>
 *     <li>The iterator never returns empty buckets ({@link #peekCount()} is never zero).</li>
 * </ul>
 */
public interface BucketIterator {
    /**
     * Checks if there are any buckets remaining to be visited by this iterator.
     * If the end has been reached, it is illegal to call {@link #peekCount()}, {@link #peekIndex()}, or {@link #advance()}.
     *
     * @return {@code true} if the iterator has more elements, {@code false} otherwise
     */
    boolean hasNext();

    /**
     * The number of items in the bucket at the current iterator position. Does not advance the iterator.
     * Must not be called if {@link #hasNext()} returns {@code false}.
     *
     * @return the number of items in the bucket, always greater than zero
     */
    long peekCount();

    /**
     * The index of the bucket at the current iterator position. Does not advance the iterator.
     * Must not be called if {@link #hasNext()} returns {@code false}.
     *
     * @return the index of the bucket, guaranteed to be in the range
     *         [{@link ExponentialHistogram#MIN_INDEX},
     *          {@link ExponentialHistogram#MAX_INDEX}]
     */
    long peekIndex();

    /**
     * Moves the iterator to the next, non-empty bucket.
     * If {@link #hasNext()} is {@code true} after calling {@link #advance()}, {@link #peekIndex()} is guaranteed to return a value
     * greater than the value returned prior to the {@link #advance()} call.
     */
    void advance();

    /**
     * Provides the scale that can be used to convert indices returned by {@link #peekIndex()} to the bucket boundaries,
     * e.g., via {@link ExponentialScaleUtils#getLowerBucketBoundary(long, int)}.
     *
     * @return the scale, which is guaranteed to be constant over the lifetime of this iterator
     */
    int scale();
}

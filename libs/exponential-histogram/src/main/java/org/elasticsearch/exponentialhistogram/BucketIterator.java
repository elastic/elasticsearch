/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
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

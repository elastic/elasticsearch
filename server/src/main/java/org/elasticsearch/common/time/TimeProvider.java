/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

/**
 * An interface encapsulating the different methods for getting relative and absolute time. The main
 * implementation of this is {@link org.elasticsearch.threadpool.ThreadPool}. To make it clear that a
 * {@code ThreadPool} is being passed around only to get time, it is preferred to use this interface.
 */
public interface TimeProvider {

    /**
     * Returns a value of milliseconds that may be used for relative time calculations.
     *
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    long relativeTimeInMillis();

    /**
     * Returns a value of nanoseconds that may be used for relative time calculations.
     *
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    long relativeTimeInNanos();

    /**
     * Returns a value of milliseconds that may be used for relative time calculations. Similar to {@link #relativeTimeInMillis()} except
     * that this method is more expensive: the return value is computed directly from {@link System#nanoTime} and is not cached. You should
     * use {@link #relativeTimeInMillis()} unless the extra accuracy offered by this method is worth the costs.
     *
     * When computing a time interval by comparing relative times in milliseconds, you should make sure that both endpoints use cached
     * values returned from {@link #relativeTimeInMillis()} or that they both use raw values returned from this method. It doesn't really
     * make sense to compare a raw value to a cached value, even if in practice the result of such a comparison will be approximately
     * sensible.
     */
    long rawRelativeTimeInMillis();

    /**
     * Returns the value of milliseconds since UNIX epoch.
     *
     * This method should only be used for exact date/time formatting. For calculating
     * time deltas that should not suffer from negative deltas, which are possible with
     * this method, see {@link #relativeTimeInMillis()}.
     */
    long absoluteTimeInMillis();
}

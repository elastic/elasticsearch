/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.RateLimiter;

import java.util.concurrent.atomic.LongAdder;

/**
 * A {@link RateLimiter.SimpleRateLimiter} that also counts the time callers spend paused. It does not count bytes: it only sees them in
 * batches of {@link #getMinPauseCheckBytes()}, so bytes below that are never passed to it.
 */
public class CountingRateLimiter extends RateLimiter {

    private final RateLimiter.SimpleRateLimiter delegate;
    private final LongAdder pauseNanos = new LongAdder();

    public CountingRateLimiter(double mbPerSec) {
        this.delegate = new RateLimiter.SimpleRateLimiter(mbPerSec);
    }

    @Override
    public void setMBPerSec(double mbPerSec) {
        delegate.setMBPerSec(mbPerSec);
    }

    @Override
    public double getMBPerSec() {
        return delegate.getMBPerSec();
    }

    @Override
    public long getMinPauseCheckBytes() {
        return delegate.getMinPauseCheckBytes();
    }

    @Override
    public long pause(long bytes) {
        final long paused = delegate.pause(bytes);
        pauseNanos.add(paused);
        return paused;
    }

    /**
     * Total nanoseconds callers have spent paused in this limiter so far.
     */
    public long getPauseNanos() {
        return pauseNanos.sum();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A rate limiter designed for multiple concurrent users.
 */
public class CombinedRateLimiter {

    // TODO: This rate limiter has some concurrency issues between the two maybePause operations

    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final RateLimiter.SimpleRateLimiter rateLimiter;
    private volatile boolean rateLimit;

    public CombinedRateLimiter(ByteSizeValue maxBytesPerSec) {
        rateLimit = maxBytesPerSec.getBytes() > 0;
        rateLimiter = new RateLimiter.SimpleRateLimiter(maxBytesPerSec.getMbFrac());
    }

    public long maybePause(int bytes) {
        if (rateLimit) {
            long bytesSincePause = bytesSinceLastPause.addAndGet(bytes);
            if (bytesSincePause > rateLimiter.getMinPauseCheckBytes()) {
                // Time to pause
                bytesSinceLastPause.addAndGet(-bytesSincePause);
                return Math.max(rateLimiter.pause(bytesSincePause), 0);
            }
        }
        return 0;
    }

    public void setMBPerSec(ByteSizeValue maxBytesPerSec) {
        rateLimit = maxBytesPerSec.getBytes() > 0;
        rateLimiter.setMBPerSec(maxBytesPerSec.getMbFrac());
    }

    public double getMBPerSec() {
        return rateLimiter.getMBPerSec();
    }
}

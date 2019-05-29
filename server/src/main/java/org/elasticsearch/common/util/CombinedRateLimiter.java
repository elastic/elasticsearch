/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
}

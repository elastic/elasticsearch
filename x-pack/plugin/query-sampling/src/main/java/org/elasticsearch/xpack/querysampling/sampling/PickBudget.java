/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import java.util.function.LongSupplier;

/**
 * Token bucket limiting picks per hour. It holds one minute of tokens so a quiet spell cannot bank a large burst;
 * a limit of zero means unlimited.
 */
public final class PickBudget {

    private static final double SECONDS_PER_HOUR = 3600;
    private static final double SECONDS_PER_BURST = 60;

    private final LongSupplier nanoTime;
    private double tokensPerSecond; // guarded by this, 0 for no limit
    private double capacity; // guarded by this
    private double tokens; // guarded by this
    private long lastRefill; // guarded by this

    public PickBudget(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        this.lastRefill = nanoTime.getAsLong();
    }

    public synchronized void perHour(long picksPerHour) {
        boolean wasLimited = tokensPerSecond > 0;
        tokensPerSecond = picksPerHour / SECONDS_PER_HOUR;
        capacity = Math.max(1, tokensPerSecond * SECONDS_PER_BURST);
        lastRefill = nanoTime.getAsLong();
        // a newly set limit starts with a full bucket, a changed one keeps its tokens
        tokens = wasLimited ? Math.min(tokens, capacity) : capacity;
    }

    public synchronized boolean available() {
        if (tokensPerSecond <= 0) {
            return true;
        }
        refill();
        return tokens >= 1;
    }

    public synchronized void take() {
        if (tokensPerSecond > 0) {
            refill();
            tokens = Math.max(0, tokens - 1);
        }
    }

    private void refill() {
        long now = nanoTime.getAsLong();
        tokens = Math.min(capacity, tokens + (now - lastRefill) / 1e9 * tokensPerSecond);
        lastRefill = now;
    }
}

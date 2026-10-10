/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import java.util.function.LongSupplier;

/**
 * Limits how many queries are picked per hour, as a token bucket: every pick takes a token, tokens come back at a
 * steady pace, and when there is none the sampler gives the queries that arrive no chance to be picked.
 * <p>
 * The bucket holds what is earned in a minute, so a quiet spell does not store up more than a short burst. A limit of
 * zero means there is none.
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

    /**
     * @param picksPerHour the most queries that are picked per hour, or 0 for no limit
     */
    public synchronized void perHour(long picksPerHour) {
        boolean wasLimited = tokensPerSecond > 0;
        tokensPerSecond = picksPerHour / SECONDS_PER_HOUR;
        capacity = Math.max(1, tokensPerSecond * SECONDS_PER_BURST);
        lastRefill = nanoTime.getAsLong();
        // a limit that has just been set starts with a full bucket, one that is changed keeps what it has
        tokens = wasLimited ? Math.min(tokens, capacity) : capacity;
    }

    /**
     * Whether a query may be picked now.
     */
    public synchronized boolean available() {
        if (tokensPerSecond <= 0) {
            return true;
        }
        refill();
        return tokens >= 1;
    }

    /**
     * A query was picked.
     */
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

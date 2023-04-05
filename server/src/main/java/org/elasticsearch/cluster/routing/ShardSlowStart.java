/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.Randomness;

public class ShardSlowStart {

    private final long movedToStartedNanoTime;
    private final long durationNanos;
    private final double invertedAggression;

    public ShardSlowStart(long movedToStartedNanoTime, long durationNanos, double aggression) {
        this.movedToStartedNanoTime = movedToStartedNanoTime;
        this.durationNanos = durationNanos;
        this.invertedAggression = 1.0 / aggression;
    }

    public boolean finished(long currentNanoTime) {
        return currentNanoTime < movedToStartedNanoTime + durationNanos;
    }

    public boolean slowed(long currentNanoTime) {
        long remaining = durationNanos - (currentNanoTime - movedToStartedNanoTime);
        return remaining > 0 && Randomness.get().nextDouble() <
            Math.pow((double) remaining / durationNanos, invertedAggression);
    }
}

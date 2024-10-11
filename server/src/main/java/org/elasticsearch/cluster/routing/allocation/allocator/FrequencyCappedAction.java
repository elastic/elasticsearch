/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.core.TimeValue;

import java.util.function.LongSupplier;

/**
 * Execute an action at most once per time interval
 */
public class FrequencyCappedAction {

    private final LongSupplier currentTimeMillisSupplier;
    private TimeValue minInterval;

    private long next;

    public FrequencyCappedAction(LongSupplier currentTimeMillisSupplier, TimeValue initialDelay) {
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.minInterval = TimeValue.MAX_VALUE;
        this.next = currentTimeMillisSupplier.getAsLong() + initialDelay.getMillis();
    }

    public void setMinInterval(TimeValue minInterval) {
        this.minInterval = minInterval;
    }

    public void maybeExecute(Runnable runnable) {
        var current = currentTimeMillisSupplier.getAsLong();
        if (current >= next) {
            next = current + minInterval.millis();
            runnable.run();
        }
    }
}

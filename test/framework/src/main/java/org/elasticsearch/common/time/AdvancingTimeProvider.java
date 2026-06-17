/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A time-provider that advances each time it's asked the time
 */
public class AdvancingTimeProvider implements TimeProvider {

    private final AtomicLong currentTimeMillis = new AtomicLong(System.currentTimeMillis());

    public void advanceByMillis(long milliseconds) {
        currentTimeMillis.addAndGet(milliseconds);
    }

    @Override
    public long relativeTimeInMillis() {
        return currentTimeMillis.incrementAndGet();
    }

    @Override
    public long relativeTimeInNanos() {
        return NANOSECONDS.toNanos(relativeTimeInMillis());
    }

    @Override
    public long rawRelativeTimeInMillis() {
        return relativeTimeInMillis();
    }

    @Override
    public long absoluteTimeInMillis() {
        throw new UnsupportedOperationException("not supported");
    }
}

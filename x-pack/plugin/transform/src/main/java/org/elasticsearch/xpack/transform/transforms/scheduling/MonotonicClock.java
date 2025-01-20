/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link MonotonicClock} class ensures that subsequent calls of {@link Clock#instant} yield non-decreasing sequence of instants.
 */
class MonotonicClock extends Clock {

    private final Clock internalClock;
    // Keeps track of what was the latest time seen so far.
    private final AtomicReference<Instant> latestInstant;

    MonotonicClock(Clock clock) {
        this.internalClock = Objects.requireNonNull(clock);
        this.latestInstant = new AtomicReference<>(clock.instant());
    }

    @Override
    public ZoneId getZone() {
        return internalClock.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return internalClock.withZone(zone);
    }

    @Override
    public Instant instant() {
        Instant current = internalClock.instant();
        // Either return the just-fetched current time or the previously recorded latest time, whichever is later.
        return latestInstant.updateAndGet(previousLatest -> current.isAfter(previousLatest) ? current : previousLatest);
    }
}

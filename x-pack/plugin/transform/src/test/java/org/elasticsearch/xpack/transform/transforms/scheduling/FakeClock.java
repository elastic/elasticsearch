/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

/**
 * {@link FakeClock} class in a test implementation of {@link Clock} and provides the possibility to set arbitrary current time.
 */
class FakeClock extends Clock {

    private Instant currentTime;

    FakeClock(Instant time) {
        currentTime = Objects.requireNonNull(time);
    }

    /**
     * Sets the current time to an arbitrary time.
     * @param time arbitrary time, may be earlier than the previously-set current time
     */
    public void setCurrentTime(Instant time) {
        currentTime = Objects.requireNonNull(time);
    }

    /**
     * Advances the current time by an arbitrary duration.
     * @param duration arbitrary duration, may be negative
     */
    public void advanceTimeBy(Duration duration) {
        Objects.requireNonNull(duration);
        setCurrentTime(currentTime.plus(duration));
    }

    @Override
    public Instant instant() {
        return currentTime;
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}

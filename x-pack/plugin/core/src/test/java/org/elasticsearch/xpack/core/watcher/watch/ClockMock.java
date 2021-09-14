/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.watch;

import org.elasticsearch.core.TimeValue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * A clock that can be modified for testing.
 */
public class ClockMock extends Clock {

    private volatile Clock wrappedClock;

    public ClockMock() {
        this(Clock.systemUTC());
    }

    /**
     * a utility method to create a new {@link ClockMock} and immediately call its {@link #freeze()} method
     */
    public static ClockMock frozen() {
        return new ClockMock().freeze();
    }

    private ClockMock(Clock wrappedClock) {
        this.wrappedClock = wrappedClock;
    }


    @Override
    public ZoneId getZone() {
        return wrappedClock.getZone();
    }

    @Override
    public synchronized Clock withZone(ZoneId zoneId) {
        if (zoneId.equals(wrappedClock.getZone())) {
            return this;
        }
        return new ClockMock(wrappedClock.withZone(zoneId));
    }

    @Override
    public long millis() {
        return wrappedClock.millis();
    }

    @Override
    public Instant instant() {
        return wrappedClock.instant();
    }

    public synchronized void setTime(ZonedDateTime now) {
        setTime(now.toInstant());
    }

    private void setTime(Instant now) {
        assert Thread.holdsLock(this);
        this.wrappedClock = Clock.fixed(now, getZone());
    }

    /** freeze the time for this clock, preventing it from advancing */
    public synchronized ClockMock freeze() {
        setTime(instant());
        return this;
    }

    /** the clock will be reset to current time and will advance from now */
    public synchronized ClockMock unfreeze() {
        wrappedClock = Clock.system(getZone());
        return this;
    }

    /** freeze the clock if not frozen and advance it by the given time */
    public synchronized void fastForward(TimeValue timeValue) {
        setTime(instant().plusMillis(timeValue.getMillis()));
    }

    /** freeze the clock if not frozen and advance it by the given amount of seconds */
    public void fastForwardSeconds(int seconds) {
        fastForward(TimeValue.timeValueSeconds(seconds));
    }

    /** freeze the clock if not frozen and rewind it by the given time */
    public synchronized void rewind(TimeValue timeValue) {
        setTime(instant().minusMillis((int) timeValue.millis()));
    }

    /** freeze the clock if not frozen and rewind it by the given number of seconds */
    public void rewindSeconds(int seconds) {
        rewind(TimeValue.timeValueSeconds(seconds));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.support.clock;

import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * A clock that can be modified for testing.
 */
public class ClockMock extends Clock {

    private final ZoneId zoneId;
    private DateTime now;

    public ClockMock() {
        zoneId = ZoneOffset.UTC;
        now = DateTime.now(DateTimeZone.UTC);
    }

    private ClockMock(ZoneId zoneId) {
        this.zoneId = zoneId;
        now = DateTime.now(DateTimeZone.forID(zoneId.getId()));
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zoneId) {
        if (zoneId.equals(this.zoneId)) {
            return this;
        }

        return new ClockMock(zoneId);
    }

    @Override
    public long millis() {
        return now.getMillis();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(now.getMillis());
    }

    public ClockMock setTime(DateTime now) {
        this.now = now;
        return this;
    }

    public ClockMock fastForward(TimeValue timeValue) {
        return setTime(now.plusMillis((int) timeValue.millis()));
    }

    public ClockMock fastForwardSeconds(int seconds) {
        return fastForward(TimeValue.timeValueSeconds(seconds));
    }

    public ClockMock rewind(TimeValue timeValue) {
        return setTime(now.minusMillis((int) timeValue.millis()));
    }

    public ClockMock rewindSeconds(int seconds) {
        return rewind(TimeValue.timeValueSeconds(seconds));
    }
}

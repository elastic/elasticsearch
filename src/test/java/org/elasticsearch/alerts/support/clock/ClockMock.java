/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.clock;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.Duration;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ClockMock implements Clock {

    private DateTime now = DateTime.now();

    @Override
    public long millis() {
        return now.getMillis();
    }

    @Override
    public long nanos() {
        return TimeUnit.MILLISECONDS.toNanos(now.getMillis());
    }

    @Override
    public DateTime now() {
        return now;
    }

    @Override
    public TimeValue timeElapsedSince(DateTime time) {
        return TimeValue.timeValueMillis(new Duration(time, now).getMillis());
    }

    public void setTime(DateTime now) {
        this.now = now;
    }

    public void fastForward(TimeValue timeValue) {
        setTime(now.plusMillis((int) timeValue.millis()));
    }

    public void rewind(TimeValue timeValue) {
        setTime(now.minusMillis((int) timeValue.millis()));
    }

}

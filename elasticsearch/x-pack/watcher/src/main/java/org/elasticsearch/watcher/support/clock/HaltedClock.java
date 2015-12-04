/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.clock;

import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 */
public class HaltedClock implements Clock {

    private final DateTime now;

    public HaltedClock(DateTime now) {
        this.now = now.toDateTime(DateTimeZone.UTC);
    }

    @Override
    public long millis() {
        return now.getMillis();
    }

    @Override
    public long nanos() {
        return millis() * 1000000;
    }

    @Override
    public DateTime nowUTC() {
        return now;
    }

    @Override
    public DateTime now(DateTimeZone timeZone) {
        return now.toDateTime(timeZone);
    }

    @Override
    public TimeValue timeElapsedSince(DateTime time) {
        return TimeValue.timeValueMillis(millis() - time.getMillis());
    }
}

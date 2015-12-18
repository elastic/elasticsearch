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
public final class SystemClock implements Clock {

    public static final SystemClock INSTANCE = new SystemClock();

    private SystemClock() {
    }

    @Override
    public long millis() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanos() {
        return System.nanoTime();
    }

    @Override
    public DateTime nowUTC() {
        return now(DateTimeZone.UTC);
    }

    @Override
    public DateTime now(DateTimeZone timeZone) {
        return DateTime.now(timeZone);
    }


    @Override
    public TimeValue timeElapsedSince(DateTime time) {
        return TimeValue.timeValueMillis(millis() - time.getMillis());
    }

}

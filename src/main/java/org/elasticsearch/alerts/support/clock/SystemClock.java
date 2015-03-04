/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.clock;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;

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
    public DateTime now() {
        return DateTime.now();
    }

    @Override
    public TimeValue timeElapsedSince(DateTime time) {
        return TimeValue.timeValueMillis(System.currentTimeMillis() - time.getMillis());
    }

}

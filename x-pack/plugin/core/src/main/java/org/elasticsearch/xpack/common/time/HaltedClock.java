/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.time;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class HaltedClock extends Clock {

    private final DateTime now;

    public HaltedClock(DateTime now) {
        this.now = now.toDateTime(DateTimeZone.UTC);
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zoneId) {
        if (zoneId.equals(ZoneOffset.UTC)) {
            return this;
        }

        throw new IllegalArgumentException("Halted clock time zone cannot be changed");
    }

    @Override
    public long millis() {
        return now.getMillis();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(now.getMillis());
    }
}

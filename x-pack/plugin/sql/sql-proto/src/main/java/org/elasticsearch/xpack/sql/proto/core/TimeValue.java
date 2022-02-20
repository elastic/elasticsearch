/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.core;

import java.util.concurrent.TimeUnit;

/**
 * Trimmed down equivalent of TimeValue in ES core.
 * Provides just the minimal functionality required for the JDBC driver to be independent.
 */
public class TimeValue {
    private final long duration;
    private final TimeUnit timeUnit;

    public TimeValue(long duration, TimeUnit timeUnit) {
        this.duration = duration;
        this.timeUnit = timeUnit;
    }

    public static TimeValue timeValueMillis(long millis) {
        return new TimeValue(millis, TimeUnit.MILLISECONDS);
    }

    public static TimeValue timeValueSeconds(long seconds) {
        return new TimeValue(seconds, TimeUnit.SECONDS);
    }

    public static TimeValue timeValueMinutes(long minutes) {
        return new TimeValue(minutes, TimeUnit.MINUTES);
    }

    public static TimeValue timeValueHours(long hours) {
        return new TimeValue(hours, TimeUnit.HOURS);
    }

    public static TimeValue timeValueDays(long days) {
        // 106751.9 days is Long.MAX_VALUE nanoseconds, so we cannot store 106752 days
        if (days > 106751) {
            throw new IllegalArgumentException("time value cannot store values greater than 106751 days");
        }
        return new TimeValue(days, TimeUnit.DAYS);
    }

    public TimeUnit timeUnit() {
        return timeUnit;
    }

    public long duration() {
        return duration;
    }

    @Override
    public String toString() {
        // NB: this is different than the original TimeValue
        return getStringRep();
    }

    public String getStringRep() {
        if (duration < 0) {
            return Long.toString(duration);
        }
        switch (timeUnit) {
            case NANOSECONDS:
                return duration + "nanos";
            case MICROSECONDS:
                return duration + "micros";
            case MILLISECONDS:
                return duration + "ms";
            case SECONDS:
                return duration + "s";
            case MINUTES:
                return duration + "m";
            case HOURS:
                return duration + "h";
            case DAYS:
                return duration + "d";
            default:
                throw new IllegalArgumentException("unknown time unit: " + timeUnit.name());
        }
    }
}

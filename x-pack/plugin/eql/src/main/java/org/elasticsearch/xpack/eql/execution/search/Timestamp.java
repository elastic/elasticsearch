/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

import static java.time.temporal.ChronoUnit.NANOS;

// wrapper for Unix epoch timestamps with different resolutions.
public abstract class Timestamp {
    static final long MILLIS_PER_SECOND = 1_000L;
    static final long NANOS_PER_MILLI = 1_000_000L;
    private static final long[] MICROS_MULTIPLIER = { 0L, 100_000L, 10_000L, 1_000L, 1_00L, 10L };

    private String source;

    public abstract Instant instant();

    int compareTo(Timestamp other) {
        return instant().compareTo(other.instant());
    }

    /**
     * Converts a value extracted from a search hit into a {@link Timestamp}.
     * Sequence matching requests {@code epoch_millis} (or {@code millis.micros}) strings, but the fields
     * API can also return ISO-8601 strings when the timestamp is not mapped as {@code date}, or when a
     * fetch format other than {@code epoch_millis} wins.
     */
    public static Timestamp from(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp;
        }
        if (value instanceof String str) {
            return parseString(str);
        }
        if (value instanceof Number number) {
            return of(Long.toString(number.longValue()));
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return of(Long.toString(zonedDateTime.toInstant().toEpochMilli()));
        }
        if (value == null) {
            throw new EqlIllegalArgumentException("Expected timestamp as a Timestamp but got null");
        }
        throw new EqlIllegalArgumentException("Expected timestamp as a Timestamp but got {}", value.getClass());
    }

    private static Timestamp parseString(String value) {
        try {
            return of(value);
        } catch (NumberFormatException e) {
            try {
                Instant instant = DateFormatters.from(DateUtils.UTC_DATE_TIME_FORMATTER.parse(value)).toInstant();
                return of(Long.toString(instant.toEpochMilli()));
            } catch (DateTimeParseException | IllegalArgumentException parseException) {
                throw new EqlIllegalArgumentException("Expected timestamp as a Timestamp but got {}", value.getClass());
            }
        }
    }

    public static Timestamp of(String milliseconds) {
        Timestamp timestamp;
        // ES will provide a <millis>.<micros> with nano-timestamps
        int dotIndex = milliseconds.lastIndexOf('.');
        if (dotIndex > 0) {
            long millis = Long.parseLong(milliseconds.substring(0, dotIndex));
            int digits = milliseconds.length() - dotIndex - 1;
            long micros = (digits >= 6)
                ? Long.parseLong(milliseconds.substring(dotIndex + 1, dotIndex + 1 + 6))
                : Long.parseLong(milliseconds.substring(dotIndex + 1)) * MICROS_MULTIPLIER[digits];
            timestamp = new NanosTimestamp(millis, micros);
        } else {
            timestamp = new MillisTimestamp(Long.parseLong(milliseconds));
        }

        timestamp.source = milliseconds;
        return timestamp;
    }

    // time delta in nanos between this and other instance
    public long delta(Timestamp other) {
        return other.instant().until(instant(), NANOS);
    }

    @Override
    public String toString() {
        return source != null ? source : asString();
    }

    abstract String asString();
}

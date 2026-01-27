/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class DateTimeTestUtils {

    private DateTimeTestUtils() {}

    public static ZonedDateTime dateTime(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, DateUtils.UTC);
    }

    public static ZonedDateTime dateTime(int year, int month, int day, int hour, int minute, int seconds, int nanos) {
        return dateTime(year, month, day, hour, minute, seconds, nanos, DateUtils.UTC);
    }

    public static ZonedDateTime dateTime(int year, int month, int day, int hour, int minute, int seconds, int nanos, ZoneId zoneId) {
        return ZonedDateTime.of(year, month, day, hour, minute, seconds, nanos, zoneId);
    }

    public static ZonedDateTime dateTime(long millisSinceEpoch) {
        return DateUtils.asDateTimeWithMillis(millisSinceEpoch);
    }

    public static OffsetTime time(long millisSinceEpoch) {
        return DateUtils.asTimeOnly(millisSinceEpoch);
    }

    public static OffsetTime time(int hour, int minute, int second, int nano) {
        return OffsetTime.of(hour, minute, second, nano, ZoneOffset.UTC);
    }

    public static OffsetTime time(int hour, int minute, int second, int nano, ZoneOffset offset) {
        return OffsetTime.of(hour, minute, second, nano, offset);
    }

    public static OffsetTime time(int hour, int minute, int second, int nano, ZoneOffset offset, ZoneId zoneId) {
        OffsetTime ot = OffsetTime.of(hour, minute, second, nano, offset);
        LocalDateTime ldt = ot.atDate(LocalDate.EPOCH).toLocalDateTime();
        return ot.withOffsetSameInstant(zoneId.getRules().getValidOffsets(ldt).get(0));
    }

    public static OffsetTime time(int hour, int minute, int second, int nano, ZoneId zoneId) {
        LocalTime lt = LocalTime.of(hour, minute, second, nano);
        LocalDateTime ldt = lt.atDate(LocalDate.EPOCH);
        return OffsetTime.of(lt, zoneId.getRules().getValidOffsets(ldt).get(0));
    }

    public static ZonedDateTime date(int year, int month, int day, ZoneId zoneId) {
        return LocalDate.of(year, month, day).atStartOfDay(zoneId);
    }

    static ZonedDateTime nowWithMillisResolution() {
        Clock millisResolutionClock = Clock.tick(Clock.systemUTC(), Duration.ofMillis(1));
        return ZonedDateTime.now(millisResolutionClock);
    }
}

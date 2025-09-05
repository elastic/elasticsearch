/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.time.temporal.UnsupportedTemporalTypeException;

/**
 * Provides information on a parsed datetime
 */
record DateTime(
    int years,
    Integer months,
    Integer days,
    Integer hours,
    Integer minutes,
    Integer seconds,
    Integer nanos,
    ZoneId zoneId,
    ZoneOffset offset
) implements TemporalAccessor {

    @Override
    @SuppressWarnings("unchecked")
    public <R> R query(TemporalQuery<R> query) {
        // shortcut a few queries used by DateFormatters.from
        if (query == TemporalQueries.zoneId()) {
            return (R) zoneId;
        }
        if (query == TemporalQueries.offset()) {
            return (R) offset;
        }
        if (query == DateFormatters.LOCAL_DATE_QUERY || query == TemporalQueries.localDate()) {
            if (months != null && days != null) {
                return (R) LocalDate.of(years, months, days);
            }
            return null;
        }
        if (query == TemporalQueries.localTime()) {
            if (hours != null && minutes != null && seconds != null) {
                return (R) LocalTime.of(hours, minutes, seconds, nanos != null ? nanos : 0);
            }
            return null;
        }
        return TemporalAccessor.super.query(query);
    }

    @Override
    public boolean isSupported(TemporalField field) {
        if (field instanceof ChronoField f) {
            return switch (f) {
                case YEAR -> true;
                case MONTH_OF_YEAR -> months != null;
                case DAY_OF_MONTH -> days != null;
                case HOUR_OF_DAY -> hours != null;
                case MINUTE_OF_HOUR -> minutes != null;
                case SECOND_OF_MINUTE -> seconds != null;
                case INSTANT_SECONDS -> months != null && days != null && hours != null && minutes != null && seconds != null;
                // if the time components are there, we just default nanos to 0 if it's not present
                case SECOND_OF_DAY, NANO_OF_SECOND, NANO_OF_DAY -> hours != null && minutes != null && seconds != null;
                case OFFSET_SECONDS -> offset != null;
                default -> false;
            };
        }

        return field.isSupportedBy(this);
    }

    @Override
    public long getLong(TemporalField field) {
        if (field instanceof ChronoField f) {
            switch (f) {
                case YEAR -> {
                    return years;
                }
                case MONTH_OF_YEAR -> {
                    return extractValue(f, months);
                }
                case DAY_OF_MONTH -> {
                    return extractValue(f, days);
                }
                case HOUR_OF_DAY -> {
                    return extractValue(f, hours);
                }
                case MINUTE_OF_HOUR -> {
                    return extractValue(f, minutes);
                }
                case SECOND_OF_MINUTE -> {
                    return extractValue(f, seconds);
                }
                case INSTANT_SECONDS -> {
                    if (isSupported(ChronoField.INSTANT_SECONDS) == false) {
                        throw new UnsupportedTemporalTypeException("No " + f + " value available");
                    }
                    return LocalDateTime.of(years, months, days, hours, minutes, seconds)
                        .toEpochSecond(offset != null ? offset : ZoneOffset.UTC);
                }
                case SECOND_OF_DAY -> {
                    if (isSupported(ChronoField.SECOND_OF_DAY) == false) {
                        throw new UnsupportedTemporalTypeException("No " + f + " value available");
                    }
                    return LocalTime.of(hours, minutes, seconds).toSecondOfDay();
                }
                case NANO_OF_SECOND -> {
                    if (isSupported(ChronoField.NANO_OF_SECOND) == false) {
                        throw new UnsupportedTemporalTypeException("No " + f + " value available");
                    }
                    return nanos != null ? nanos.longValue() : 0L;
                }
                case NANO_OF_DAY -> {
                    if (isSupported(ChronoField.NANO_OF_DAY) == false) {
                        throw new UnsupportedTemporalTypeException("No " + f + " value available");
                    }
                    return LocalTime.of(hours, minutes, seconds, nanos != null ? nanos : 0).toNanoOfDay();
                }
                case OFFSET_SECONDS -> {
                    if (offset == null) {
                        throw new UnsupportedTemporalTypeException("No " + f + " value available");
                    }
                    return offset.getTotalSeconds();
                }
                default -> throw new UnsupportedTemporalTypeException("No " + f + " value available");
            }
        }

        return field.getFrom(this);
    }

    private static long extractValue(ChronoField field, Number value) {
        if (value == null) {
            throw new UnsupportedTemporalTypeException("No " + field + " value available");
        }
        return value.longValue();
    }
}

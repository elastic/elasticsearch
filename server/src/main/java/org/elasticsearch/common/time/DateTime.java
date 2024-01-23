/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.UnsupportedTemporalTypeException;

class DateTime implements TemporalAccessor {

    private final int years;
    private final int months;
    private final int days;
    private final Integer hours;
    private final Integer minutes;
    private final Integer seconds;
    private final Integer nanos;
    private final ZoneOffset offset;

    DateTime(int years, int months, int days, Integer hours, Integer minutes, Integer seconds, Integer nanos, ZoneOffset offset) {
        this.years = years;
        this.months = months;
        this.days = days;
        this.hours = hours;
        this.minutes = minutes;
        this.seconds = seconds;
        this.nanos = nanos;
        this.offset = offset;
    }

    @Override
    public boolean isSupported(TemporalField field) {
        if (field instanceof ChronoField f) {
            return switch (f) {
                case YEAR, MONTH_OF_YEAR, DAY_OF_MONTH -> true;
                case HOUR_OF_DAY -> hours != null;
                case MINUTE_OF_HOUR -> minutes != null;
                case SECOND_OF_MINUTE -> seconds != null;
                case NANO_OF_SECOND -> nanos != null;
                case INSTANT_SECONDS -> hours != null && minutes != null && seconds != null;
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
                    return months;
                }
                case DAY_OF_MONTH -> {
                    return days;
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
                case NANO_OF_SECOND -> {
                    return extractValue(f, nanos);
                }
                case INSTANT_SECONDS -> {
                    return OffsetDateTime.of(years, months, days, hours, minutes, seconds, nanos, offset != null ? offset : ZoneOffset.UTC)
                        .getLong(f);
                }
                case OFFSET_SECONDS -> {
                    if (offset == null) throw new UnsupportedTemporalTypeException("No " + f + " value available");
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.type;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

//FIXME: this class comes from sql-proto
// find a way to share it across or potentially just copy it over
final class StringUtils {

    public static final String EMPTY = "";

    public static final DateTimeFormatter ISO_DATE_WITH_NANOS = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendFraction(NANO_OF_SECOND, 3, 9, true)
            .appendOffsetId()
            .toFormatter(Locale.ROOT);

    public static final DateTimeFormatter ISO_TIME_WITH_NANOS = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendFraction(NANO_OF_SECOND, 3, 9, true)
            .appendOffsetId()
            .toFormatter(Locale.ROOT);

    private static final int SECONDS_PER_MINUTE = 60;
    private static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
    private static final int SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

    private StringUtils() {}

    public static String toString(Object value) {
        if (value == null) {
            return "null";
        }

        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).format(ISO_DATE_WITH_NANOS);
        }
        if (value instanceof OffsetTime) {
            return ((OffsetTime) value).format(ISO_TIME_WITH_NANOS);
        }
        if (value instanceof Timestamp) {
            Timestamp ts = (Timestamp) value;
            return ts.toInstant().toString();
        }

        // handle intervals
        // YEAR/MONTH/YEAR TO MONTH -> YEAR TO MONTH
        if (value instanceof Period) {
            // +yyy-mm - 7 chars
            StringBuilder sb = new StringBuilder(7);
            Period p = (Period) value;
            if (p.isNegative()) {
                sb.append("-");
                p = p.negated();
            } else {
                sb.append("+");
            }
            sb.append(p.getYears());
            sb.append("-");
            sb.append(p.getMonths());
            return sb.toString();
        }

        // DAY/HOUR/MINUTE/SECOND (and variations) -> DAY_TO_SECOND
        if (value instanceof Duration) {
            // +ddd hh:mm:ss.mmmmmmmmm - 23 chars
            StringBuilder sb = new StringBuilder(23);
            Duration d = (Duration) value;
            if (d.isNegative()) {
                sb.append("-");
                d = d.negated();
            } else {
                sb.append("+");
            }

            long durationInSec = d.getSeconds();

            sb.append(durationInSec / SECONDS_PER_DAY);
            sb.append(" ");
            durationInSec = durationInSec % SECONDS_PER_DAY;
            sb.append(indent(durationInSec / SECONDS_PER_HOUR));
            sb.append(":");
            durationInSec = durationInSec % SECONDS_PER_HOUR;
            sb.append(indent(durationInSec / SECONDS_PER_MINUTE));
            sb.append(":");
            durationInSec = durationInSec % SECONDS_PER_MINUTE;
            sb.append(indent(durationInSec));
            sb.append(".");
            sb.append(TimeUnit.NANOSECONDS.toMillis(d.getNano()));
            return sb.toString();
        }

        return Objects.toString(value);
    }

    private static String indent(long timeUnit) {
        return timeUnit < 10 ? "0" + timeUnit : Long.toString(timeUnit);
    }
}

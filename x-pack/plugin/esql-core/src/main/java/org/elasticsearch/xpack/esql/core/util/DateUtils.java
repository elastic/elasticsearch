/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

//FIXME: Taken from sql-proto (StringUtils)
//Ideally it should be shared but the dependencies across projects and and SQL-client make it tricky.
// Maybe a gradle task would fix that...
public class DateUtils {

    public static final ZoneId UTC = ZoneId.of("Z");
    private static final DateTimeFormatter DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL = new DateTimeFormatterBuilder().append(ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral('T')
        .append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withZone(UTC);
    private static final DateTimeFormatter DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE = new DateTimeFormatterBuilder().append(ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral(' ')
        .append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withZone(UTC);

    public static final String EMPTY = "";

    public static final DateTimeFormatter ISO_DATE_WITH_NANOS = new DateTimeFormatterBuilder().parseCaseInsensitive()
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

    public static final DateTimeFormatter ISO_TIME_WITH_NANOS = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .appendOffsetId()
        .toFormatter(Locale.ROOT);

    public static final DateFormatter UTC_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time").withZone(UTC);

    public static final int SECONDS_PER_MINUTE = 60;
    public static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
    public static final int SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

    private DateUtils() {}

    /**
     * Parses the given string into a ZonedDateTime using the provided timezone.
     */
    public static ZonedDateTime asDateTimeWithNanos(String dateFormat, ZoneId zoneId) {
        return DateFormatters.from(ISO_DATE_WITH_NANOS.parse(dateFormat)).withZoneSameInstant(zoneId);
    }

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
        if (value instanceof Timestamp ts) {
            return ts.toInstant().toString();
        }

        // handle intervals
        // YEAR/MONTH/YEAR TO MONTH -> YEAR TO MONTH
        if (value instanceof Period p) {
            // +yyy-mm - 7 chars
            StringBuilder sb = new StringBuilder(7);
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
        if (value instanceof Duration d) {
            // +ddd hh:mm:ss.mmmmmmmmm - 23 chars
            StringBuilder sb = new StringBuilder(23);
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
            long millis = TimeUnit.NANOSECONDS.toMillis(d.getNano());
            if (millis > 0) {
                sb.append(".");
                while (millis % 10 == 0) {
                    millis /= 10;
                }
                sb.append(millis);
            }
            return sb.toString();
        }

        return Objects.toString(value);
    }

    private static String indent(long timeUnit) {
        return timeUnit < 10 ? "0" + timeUnit : Long.toString(timeUnit);
    }

    /**
     * Creates a datetime from the millis since epoch (thus the time-zone is UTC).
     */
    public static ZonedDateTime asDateTime(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
    }

    public static long asMillis(ZonedDateTime zonedDateTime) {
        return zonedDateTime.toInstant().toEpochMilli();
    }

    /**
     * Parses the given string into a DateTime using UTC as a default timezone.
     */
    public static ZonedDateTime asDateTime(String dateFormat) {
        int separatorIdx = dateFormat.indexOf('-'); // Find the first `-` date separator
        if (separatorIdx == 0) { // first char = `-` denotes a negative year
            separatorIdx = dateFormat.indexOf('-', 1); // Find the first `-` date separator past the negative year
        }
        // Find the second `-` date separator and move 3 places past the dayOfYear to find the time separator
        // e.g. 2020-06-01T10:20:30....
        // ^
        // +3 = ^
        separatorIdx = dateFormat.indexOf('-', separatorIdx + 1) + 3;

        // Avoid index out of bounds - it will lead to DateTimeParseException anyways
        if (separatorIdx >= dateFormat.length() || dateFormat.charAt(separatorIdx) == 'T') {
            return DateFormatters.from(DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL.parse(dateFormat)).withZoneSameInstant(UTC);
        } else {
            return DateFormatters.from(DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE.parse(dateFormat)).withZoneSameInstant(UTC);
        }
    }

    public static String toString(ZonedDateTime dateTime) {
        return org.elasticsearch.xpack.esql.core.type.StringUtils.toString(dateTime);
    }
}

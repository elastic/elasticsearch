/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public final class DateUtils {

    public static final ZoneId UTC = ZoneId.of("Z");
    public static final String DATE_PARSE_FORMAT = "epoch_millis";

    private static final DateTimeFormatter DATE_TIME_ESCAPED_LITERAL_FORMATTER = new DateTimeFormatterBuilder()
        .append(ISO_LOCAL_DATE)
        .appendLiteral(" ")
        .append(ISO_LOCAL_TIME)
        .toFormatter().withZone(UTC);

    private static final DateTimeFormatter UTC_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral('T')
        .optionalStart()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .optionalEnd()
        .optionalEnd()
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HHmm", "Z")
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .parseDefaulting(HOUR_OF_DAY, 0)
        .parseDefaulting(MINUTE_OF_HOUR, 0)
        .parseDefaulting(SECOND_OF_MINUTE, 0)
        .toFormatter(Locale.ROOT)
        .withZone(UTC);

    private static final long DAY_IN_MILLIS = 60 * 60 * 24 * 1000;

    private DateUtils() {}

    /**
     * Creates an date for SQL DATE type from the millis since epoch.
     */
    public static ZonedDateTime asDateOnly(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC).toLocalDate().atStartOfDay(UTC);
    }

    /**
     * Creates a datetime from the millis since epoch (thus the time-zone is UTC).
     */
    public static ZonedDateTime asDateTime(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
    }

    /**
     * Creates a datetime from the millis since epoch then translates the date into the given timezone.
     */
    public static ZonedDateTime asDateTime(long millis, ZoneId id) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), id);
    }

    /**
     * Parses the given string into a Date (SQL DATE type) using UTC as a default timezone.
     */
    public static ZonedDateTime asDateOnly(String dateFormat) {
        return LocalDate.parse(dateFormat, ISO_LOCAL_DATE).atStartOfDay(UTC);
    }

    public static ZonedDateTime asDateOnly(ZonedDateTime zdt) {
        return zdt.toLocalDate().atStartOfDay(zdt.getZone());
    }

    /**
     * Parses the given string into a DateTime using UTC as a default timezone.
     */
    public static ZonedDateTime asDateTime(String dateFormat) {
        return DateFormatters.from(UTC_DATE_TIME_FORMATTER.parse(dateFormat)).withZoneSameInstant(UTC);
    }

    public static ZonedDateTime ofEscapedLiteral(String dateFormat) {
        return ZonedDateTime.parse(dateFormat, DATE_TIME_ESCAPED_LITERAL_FORMATTER.withZone(UTC));
    }

    public static String toString(ZonedDateTime dateTime) {
        return StringUtils.toString(dateTime);
    }

    public static String toDateString(ZonedDateTime date) {
        return date.format(ISO_LOCAL_DATE);
    }

    public static long minDayInterval(long l) {
        if (l < DAY_IN_MILLIS ) {
            return DAY_IN_MILLIS;
        }
        return l - (l % DAY_IN_MILLIS);
    }
}

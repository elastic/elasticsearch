/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public final class DateUtils {

    private static final long DAY_IN_MILLIS = 60 * 60 * 24 * 1000;

    // TODO: do we have a java.time based parser we can use instead?
    private static final DateTimeFormatter UTC_DATE_FORMATTER = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC();

    public static final ZoneId UTC = ZoneId.of("Z");
    public static final String DATE_PARSE_FORMAT = "epoch_millis";

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
        return asDateOnly(UTC_DATE_FORMATTER.parseDateTime(dateFormat));
    }

    public static ZonedDateTime asDateOnly(DateTime dateTime) {
        LocalDateTime ldt = LocalDateTime.of(
            dateTime.getYear(),
            dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth(),
            0,
            0,
            0,
            0);

        return ZonedDateTime.ofStrict(ldt,
            ZoneOffset.ofTotalSeconds(dateTime.getZone().getOffset(dateTime) / 1000),
            org.elasticsearch.common.time.DateUtils.dateTimeZoneToZoneId(dateTime.getZone()));
    }

    public static ZonedDateTime asDateOnly(ZonedDateTime zdt) {
        return zdt.toLocalDate().atStartOfDay(zdt.getZone());
    }

    /**
     * Parses the given string into a DateTime using UTC as a default timezone.
     */
    public static ZonedDateTime asDateTime(String dateFormat) {
        return asDateTime(UTC_DATE_FORMATTER.parseDateTime(dateFormat));
    }

    public static ZonedDateTime asDateTime(DateTime dateTime) {
        LocalDateTime ldt = LocalDateTime.of(
                dateTime.getYear(),
                dateTime.getMonthOfYear(),
                dateTime.getDayOfMonth(),
                dateTime.getHourOfDay(),
                dateTime.getMinuteOfHour(),
                dateTime.getSecondOfMinute(),
                dateTime.getMillisOfSecond() * 1_000_000);
        
        return ZonedDateTime.ofStrict(ldt,
                ZoneOffset.ofTotalSeconds(dateTime.getZone().getOffset(dateTime) / 1000),
                org.elasticsearch.common.time.DateUtils.dateTimeZoneToZoneId(dateTime.getZone()));
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

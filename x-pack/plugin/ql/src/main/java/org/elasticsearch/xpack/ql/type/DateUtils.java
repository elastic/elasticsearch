/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_TIME;

//FIXME: Taken from sql-proto.
//Ideally it should be shared but the dependencies across projects and and SQL-client make it tricky.
//Maybe a gradle task would fix that...
public final class DateUtils {

    public static final ZoneId UTC = ZoneId.of("Z");
    // In Java 8 LocalDate.EPOCH is not available, introduced with later Java versions
    public static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);
    public static final long DAY_IN_MILLIS = 60 * 60 * 24 * 1000L;

    private static final DateFormatter UTC_DATE_TIME_FORMATTER = DateFormatter.forPattern("date_optional_time").withZone(UTC);

    static final String DATE_PARSE_FORMAT = "epoch_millis";

    private DateUtils() {}

    /**
     * Creates an date for SQL DATE type from the millis since epoch.
     */
    public static ZonedDateTime asDateOnly(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC).toLocalDate().atStartOfDay(UTC);
    }

    /**
     * Creates an date for SQL TIME type from the millis since epoch.
     */
    public static OffsetTime asTimeOnly(long millis) {
        return OffsetTime.ofInstant(Instant.ofEpochMilli(millis % DAY_IN_MILLIS), UTC);
    }

    /**
     * Creates a datetime from the millis since epoch (thus the time-zone is UTC).
     */
    public static ZonedDateTime asDateTime(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
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

    public static OffsetTime asTimeOnly(String timeFormat) {
        return DateFormatters.from(ISO_TIME.parse(timeFormat)).toOffsetDateTime().toOffsetTime();
    }

    /**
     * Parses the given string into a DateTime using UTC as a default timezone.
     */
    public static ZonedDateTime asDateTime(String dateFormat) {
        return DateFormatters.from(UTC_DATE_TIME_FORMATTER.parse(dateFormat)).withZoneSameInstant(UTC);
    }

    public static String toString(ZonedDateTime dateTime) {
        return StringUtils.toString(dateTime);
    }

    public static String toDateString(ZonedDateTime date) {
        return date.format(ISO_LOCAL_DATE);
    }

    public static String toTimeString(OffsetTime time) {
        return StringUtils.toString(time);
    }
}
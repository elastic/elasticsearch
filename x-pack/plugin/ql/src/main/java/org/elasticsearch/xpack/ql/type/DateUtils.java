/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.common.time.DateFormatters;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

//NB: Taken from sql-proto.
public final class DateUtils {

    public static final ZoneId UTC = ZoneId.of("Z");

    private static final DateTimeFormatter DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE = new DateTimeFormatterBuilder().append(ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral(' ')
        .append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withZone(UTC);
    private static final DateTimeFormatter DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL = new DateTimeFormatterBuilder().append(ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral('T')
        .append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withZone(UTC);

    private DateUtils() {}

    /**
     * Creates a datetime from the millis since epoch (thus the time-zone is UTC).
     */
    public static ZonedDateTime asDateTime(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
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
        return StringUtils.toString(dateTime);
    }
}

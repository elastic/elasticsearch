/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

//NB: Taken from sql-proto.
public final class DateUtils {

    public static final ZoneId UTC = ZoneId.of("Z");

    private static final DateFormatter UTC_DATE_TIME_FORMATTER = DateFormatter.forPattern("date_optional_time").withZone(UTC);

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
        return DateFormatters.from(UTC_DATE_TIME_FORMATTER.parse(dateFormat)).withZoneSameInstant(UTC);
    }

    public static String toString(ZonedDateTime dateTime) {
        return StringUtils.toString(dateTime);
    }
}
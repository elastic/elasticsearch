/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class DateUtils {

    // TODO: do we have a java.time based parser we can use instead?
    private static final DateTimeFormatter UTC_DATE_FORMATTER = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC();

    public static ZoneId UTC = ZoneId.of("UTC");

    private DateUtils() {}


    public static ZonedDateTime of(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
    }
    
    public static ZonedDateTime of(long millis, ZoneId id) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), id);
    }

    public static ZonedDateTime of(String dateFormat) {
        return of(UTC_DATE_FORMATTER.parseDateTime(dateFormat));
    }

    public static ZonedDateTime of(DateTime dateTime) {
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
                dateTime.getZone().toTimeZone().toZoneId());
    }
    
    public static String toString(ZonedDateTime dateTime) {
        // alternative to toOffsetDateTime().toString() - avoids creating the intermediate OffsetDateTime
        return dateTime.toLocalDateTime().toString() + dateTime.getOffset().toString();
    }
}

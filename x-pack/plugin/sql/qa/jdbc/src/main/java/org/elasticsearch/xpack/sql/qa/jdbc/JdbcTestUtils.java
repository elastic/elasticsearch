/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.sql.Date;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;

final class JdbcTestUtils {

    private JdbcTestUtils() {}

    static final ZoneId UTC = ZoneId.of("Z");
    static final String JDBC_TIMEZONE = "timezone";
    static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

    static String of(long millis, String zoneId) {
        return StringUtils.toString(ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(zoneId)));
    }

    static Date asDate(long millis, ZoneId zoneId) {
        return new java.sql.Date(
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId).toLocalDate().atStartOfDay(zoneId).toInstant().toEpochMilli()
        );
    }

    static Time asTime(long millis, ZoneId zoneId) {
        return new Time(
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId)
                .toLocalTime()
                .atDate(JdbcTestUtils.EPOCH)
                .atZone(zoneId)
                .toInstant()
                .toEpochMilli()
        );
    }

    static long convertFromCalendarToUTC(long value, Calendar cal) {
        if (cal == null) {
            return value;
        }
        Calendar c = (Calendar) cal.clone();
        c.setTimeInMillis(value);

        ZonedDateTime convertedDateTime = ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId())
            .withZoneSameLocal(ZoneOffset.UTC);

        return convertedDateTime.toInstant().toEpochMilli();
    }
}

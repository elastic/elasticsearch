/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.proto.StringUtils.ISO_DATE_WITH_MILLIS;
import static org.elasticsearch.xpack.sql.proto.StringUtils.ISO_TIME_WITH_MILLIS;

/**
 * JDBC specific datetime specific utility methods. Because of lack of visibility, this class borrows code
 * from {@code org.elasticsearch.xpack.sql.util.DateUtils} and {@code org.elasticsearch.xpack.sql.proto.StringUtils}.
 */
final class JdbcDateUtils {

    private JdbcDateUtils() {}

    // In Java 8 LocalDate.EPOCH is not available, introduced with later Java versions
    private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

    private static ZonedDateTime asDateTime(String date) {
        return ISO_DATE_WITH_MILLIS.parse(date, ZonedDateTime::from);
    }

    static long dateTimeAsMillisSinceEpoch(String date) {
        return asDateTime(date).toInstant().toEpochMilli();
    }

    static long timeAsMillisSinceEpoch(String date) {
        return ISO_TIME_WITH_MILLIS.parse(date, OffsetTime::from).atDate(EPOCH).toInstant().toEpochMilli();
    }

    static Date asDate(String date) {
        ZonedDateTime zdt = asDateTime(date);
        return new Date(zdt.toLocalDate().atStartOfDay(zdt.getZone()).toInstant().toEpochMilli());
    }

    static Time asTime(String date) {
        ZonedDateTime zdt = asDateTime(date);
        return new Time(zdt.toLocalTime().atDate(EPOCH).atZone(zdt.getZone()).toInstant().toEpochMilli());
    }

    static Time timeAsTime(String date) {
        OffsetTime ot = ISO_TIME_WITH_MILLIS.parse(date, OffsetTime::from);
        return new Time(ot.atDate(EPOCH).toInstant().toEpochMilli());
    }

    static Timestamp asTimestamp(long millisSinceEpoch) {
        return new Timestamp(millisSinceEpoch);
    }

    static Timestamp asTimestamp(String date) {
        return new Timestamp(dateTimeAsMillisSinceEpoch(date));
    }

    static Timestamp timeAsTimestamp(String date) {
        return new Timestamp(timeAsMillisSinceEpoch(date));
    }
    /*
     * Handles the value received as parameter, as either String (a ZonedDateTime formatted in ISO 8601 standard with millis) -
     * date fields being returned formatted like this. Or a Long value, in case of Histograms.
     */
    static <R> R asDateTimeField(Object value, Function<String, R> asDateTimeMethod, Function<Long, R> ctor) {
        if (value instanceof String) {
            return asDateTimeMethod.apply((String) value);
        } else {
            return ctor.apply(((Number) value).longValue());
        }
    }
}

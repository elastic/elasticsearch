/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class DateTimeTestUtils {

    private DateTimeTestUtils() {}

    public static ZonedDateTime dateTime(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, DateUtils.UTC);
    }

    public static ZonedDateTime dateTime(long millisSinceEpoch) {
        return DateUtils.asDateTime(millisSinceEpoch);
    }

    public static ZonedDateTime date(long millisSinceEpoch) {
        return DateUtils.asDateOnly(millisSinceEpoch);
    }

    public static OffsetTime time(long millisSinceEpoch) {
        return DateUtils.asTimeOnly(millisSinceEpoch);
    }

    public static OffsetTime time(int hour, int minute, int second, int nano) {
        return OffsetTime.of(hour, minute, second, nano, ZoneOffset.UTC);
    }
}

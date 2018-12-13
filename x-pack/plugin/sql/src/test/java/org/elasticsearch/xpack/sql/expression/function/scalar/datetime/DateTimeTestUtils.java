/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.util.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class DateTimeTestUtils {

    private DateTimeTestUtils() {}

    public static ZonedDateTime dateTime(int year, int month, int day, int hour, int minute) {
        DateTime dateTime = new DateTime(year, month, day, hour, minute, DateTimeZone.UTC);
        ZonedDateTime zdt = ZonedDateTime.of(year, month, day, hour, minute, 0, 0, DateUtils.UTC);
        assertEquals(dateTime.getMillis() / 1000, zdt.toEpochSecond());
        return zdt;
    }

    public static ZonedDateTime dateTime(long millisSinceEpoch) {
        return DateUtils.of(millisSinceEpoch);
    }
}

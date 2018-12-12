/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.time.DateFormatter;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.time.ZoneOffset;

public class DateUtils {

    private static final DateFormatter dateOnlyFormatter = DateFormatter.forPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private static final DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC();

    public static long endOfTheDay(String date) {
        try {
            // Try parsing using complete date/time format
            return dateTimeFormatter.parseDateTime(date).getMillis();
        } catch (IllegalArgumentException ex) {
            // Fall back to the date only format
            MutableDateTime dateTime = new MutableDateTime(dateOnlyFormatter.parseMillis(date));
            dateTime.millisOfDay().set(dateTime.millisOfDay().getMaximumValue());
            return dateTime.getMillis();
        }
    }

    public static long beginningOfTheDay(String date) {
        try {
            // Try parsing using complete date/time format
            return dateTimeFormatter.parseDateTime(date).getMillis();
        } catch (IllegalArgumentException ex) {
            // Fall back to the date only format
            return dateOnlyFormatter.parseMillis(date);
        }

    }
}

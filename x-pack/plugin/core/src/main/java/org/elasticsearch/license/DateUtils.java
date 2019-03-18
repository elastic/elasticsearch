/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

public class DateUtils {

    private static final DateFormatter dateOnlyFormatter = DateFormatter.forPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateFormatter dateTimeFormatter = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    public static long endOfTheDay(String date) {
        try {
            // Try parsing using complete date/time format
            return dateTimeFormatter.parseMillis(date);
        } catch (ElasticsearchParseException | IllegalArgumentException ex) {
            ZonedDateTime dateTime = DateFormatters.from(dateOnlyFormatter.parse(date));
            dateTime.with(ChronoField.MILLI_OF_DAY, ChronoField.MILLI_OF_DAY.range().getMaximum());
            return dateTime.toInstant().toEpochMilli();
        }
    }

    public static long beginningOfTheDay(String date) {
        try {
            // Try parsing using complete date/time format
            return dateTimeFormatter.parseMillis(date);
        } catch (ElasticsearchParseException | IllegalArgumentException ex) {
            // Fall back to the date only format
            return DateFormatters.from(dateOnlyFormatter.parse(date)).toInstant().toEpochMilli();
        }
    }
}

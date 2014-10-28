/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;

public class DateUtils {

    private final static FormatDateTimeFormatter formatDateTimeFormatter = Joda.forPattern("yyyy-MM-dd");

    private final static DateTimeFormatter dateTimeFormatter = formatDateTimeFormatter.parser();

    public static long endOfTheDay(String date) {
        MutableDateTime dateTime = dateTimeFormatter.parseMutableDateTime(date);
        dateTime.dayOfMonth().roundCeiling();
        return dateTime.getMillis();
    }

    public static long beginningOfTheDay(String date) {
        return dateTimeFormatter.parseDateTime(date).getMillis();
    }
}

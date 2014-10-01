/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {
    public static final DateFormat DATE_FORMAT;
    public static final TimeZone TIME_ZONE = TimeZone.getTimeZone("UTC");

    static {
        DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
        DATE_FORMAT.setTimeZone(DateUtils.TIME_ZONE);
        DATE_FORMAT.setLenient(false);
    }

    public static long longExpiryDateFromDate(long date) {
        Date dateObj = new Date(date);

        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTimeZone(TIME_ZONE);
        calendar.setTimeInMillis(dateObj.getTime());

        calendar.set(Calendar.HOUR, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);

        return calendar.getTimeInMillis();
    }

    public static long longFromDateString(String dateStr) throws ParseException {
        Date dateObj = DATE_FORMAT.parse(dateStr);
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTimeZone(TIME_ZONE);
        calendar.setTimeInMillis(dateObj.getTime());
        return calendar.getTimeInMillis();
    }

    public static long longExpiryDateFromString(String dateStr) throws ParseException {
        return longExpiryDateFromDate(longFromDateString(dateStr));
    }

    public static String dateStringFromLongDate(long date) {
        Date dateObj = new Date(date);
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTimeZone(TIME_ZONE);
        calendar.setTimeInMillis(dateObj.getTime());
        return DATE_FORMAT.format(calendar.getTime());
    }
}

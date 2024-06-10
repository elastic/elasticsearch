/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.util.Maps;

import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.spi.CalendarNameProvider;

public class CompatCalendarNameProvider extends CalendarNameProvider {

    private static final String[] LONG_MONTHS = new String[] {
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December"};
    private static final String[] LONG_DAYS = new String[] {
        "", // Calendar.SUNDAY is 1, not 0
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday"
    };

    @Override
    public String getDisplayName(String calendarType, int field, int value, int style, Locale locale) {
        if (locale == Locale.ROOT && calendarType.equals("gregory")) {
            switch (field) {
                case Calendar.MONTH:
                    switch (style) {
                        case Calendar.LONG_FORMAT:
                        case Calendar.LONG_STANDALONE:
                            return LONG_MONTHS[value];
                    }
                    break;
                case Calendar.DAY_OF_WEEK:
                    switch (style) {
                        case Calendar.LONG_FORMAT:
                        case Calendar.LONG_STANDALONE:
                            return LONG_DAYS[value];
                    }
                    break;
            }
        }

        return null;
    }

    @Override
    public Map<String, Integer> getDisplayNames(String calendarType, int field, int style, Locale locale) {
        if (locale == Locale.ROOT && calendarType.equals("gregory")) {
            switch (field) {
                case Calendar.MONTH:
                    switch (style) {
                        case Calendar.LONG_FORMAT:
                        case Calendar.LONG_STANDALONE:
                            return toMap(LONG_MONTHS);
                    }
                    break;
                case Calendar.DAY_OF_WEEK:
                    switch (style) {
                        case Calendar.LONG_FORMAT:
                        case Calendar.LONG_STANDALONE:
                            return toMap(LONG_DAYS);
                    }
                    break;
            }
        }
        return null;
    }

    private static Map<String, Integer> toMap(String[] array) {
        Map<String, Integer> map = Maps.newHashMapWithExpectedSize(array.length);
        for (int i=0; i< array.length; i++) {
            map.put(array[i], i);
        }
        return map;
    }

    @Override
    public Locale[] getAvailableLocales() {
        return new Locale[] { Locale.ROOT };
    }
}

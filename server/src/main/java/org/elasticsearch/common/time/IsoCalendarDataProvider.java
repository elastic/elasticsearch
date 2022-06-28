/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.time;

import java.util.Calendar;
import java.util.Locale;
import java.util.spi.CalendarDataProvider;

/**
 * This class is loaded by JVM SPI mechanism in order to provide ISO compatible behaviour for week calculations using java.time.
 * It defines start of the week as Monday and requires 4 days in the first week of the year.
 * This class overrides default behaviour for Locale.ROOT (start of the week Sunday, minimum 1 day in the first week of the year).
 * Java SPI mechanism requires java.locale.providers to contain SPI value (i.e. java.locale.providers=SPI,COMPAT)
 * @see <a href="https://en.wikipedia.org/wiki/ISO_week_date">ISO week date</a>
 */
public class IsoCalendarDataProvider extends CalendarDataProvider {

    @Override
    public int getFirstDayOfWeek(Locale locale) {
        return Calendar.MONDAY;
    }

    @Override
    public int getMinimalDaysInFirstWeek(Locale locale) {
        return 4;
    }

    @Override
    public Locale[] getAvailableLocales() {
        return new Locale[] { Locale.ROOT };
    }
}

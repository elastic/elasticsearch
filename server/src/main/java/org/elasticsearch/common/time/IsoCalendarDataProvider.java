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

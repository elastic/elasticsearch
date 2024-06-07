/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import java.text.DateFormatSymbols;
import java.text.spi.DateFormatSymbolsProvider;
import java.util.Locale;

public class CompatDateFormatSymbolsProvider extends DateFormatSymbolsProvider {

    private static final DateFormatSymbols SYMBOLS;

    static {
        SYMBOLS = new DateFormatSymbols(Locale.ROOT);
        SYMBOLS.setWeekdays(new String[] {
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday"
        });
        SYMBOLS.setMonths(new String[] {
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
            "December"
        });
    }

    @Override
    public DateFormatSymbols getInstance(Locale locale) {
        return SYMBOLS;
    }

    @Override
    public Locale[] getAvailableLocales() {
        return new Locale[] { Locale.ROOT };
    }
}

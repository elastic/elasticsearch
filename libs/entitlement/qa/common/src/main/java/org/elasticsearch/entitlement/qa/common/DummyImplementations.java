/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.common;

import java.text.BreakIterator;
import java.text.Collator;
import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.spi.BreakIteratorProvider;
import java.text.spi.CollatorProvider;
import java.text.spi.DateFormatProvider;
import java.text.spi.DateFormatSymbolsProvider;
import java.text.spi.DecimalFormatSymbolsProvider;
import java.text.spi.NumberFormatProvider;
import java.util.Locale;
import java.util.Map;
import java.util.spi.CalendarDataProvider;
import java.util.spi.CalendarNameProvider;
import java.util.spi.CurrencyNameProvider;
import java.util.spi.LocaleNameProvider;
import java.util.spi.LocaleServiceProvider;
import java.util.spi.TimeZoneNameProvider;

/**
 * A collection of concrete subclasses that we can instantiate but that don't actually work.
 * <p>
 * A bit like Mockito but way more painful.
 */
class DummyImplementations {

    static class DummyLocaleServiceProvider extends LocaleServiceProvider {

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyBreakIteratorProvider extends BreakIteratorProvider {

        @Override
        public BreakIterator getWordInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public BreakIterator getLineInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public BreakIterator getCharacterInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public BreakIterator getSentenceInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyCollatorProvider extends CollatorProvider {

        @Override
        public Collator getInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyDateFormatProvider extends DateFormatProvider {

        @Override
        public DateFormat getTimeInstance(int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public DateFormat getDateInstance(int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public DateFormat getDateTimeInstance(int dateStyle, int timeStyle, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyDateFormatSymbolsProvider extends DateFormatSymbolsProvider {

        @Override
        public DateFormatSymbols getInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyDecimalFormatSymbolsProvider extends DecimalFormatSymbolsProvider {

        @Override
        public DecimalFormatSymbols getInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyNumberFormatProvider extends NumberFormatProvider {

        @Override
        public NumberFormat getCurrencyInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public NumberFormat getIntegerInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public NumberFormat getNumberInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public NumberFormat getPercentInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyCalendarDataProvider extends CalendarDataProvider {

        @Override
        public int getFirstDayOfWeek(Locale locale) {
            throw unexpected();
        }

        @Override
        public int getMinimalDaysInFirstWeek(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyCalendarNameProvider extends CalendarNameProvider {

        @Override
        public String getDisplayName(String calendarType, int field, int value, int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public Map<String, Integer> getDisplayNames(String calendarType, int field, int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyCurrencyNameProvider extends CurrencyNameProvider {

        @Override
        public String getSymbol(String currencyCode, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyLocaleNameProvider extends LocaleNameProvider {

        @Override
        public String getDisplayLanguage(String languageCode, Locale locale) {
            throw unexpected();
        }

        @Override
        public String getDisplayCountry(String countryCode, Locale locale) {
            throw unexpected();
        }

        @Override
        public String getDisplayVariant(String variant, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyTimeZoneNameProvider extends TimeZoneNameProvider {

        @Override
        public String getDisplayName(String ID, boolean daylight, int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    private static RuntimeException unexpected() {
        return new IllegalStateException("This method isn't supposed to be called");
    }

}

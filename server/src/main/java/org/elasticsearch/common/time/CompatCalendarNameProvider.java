/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.UpdateForV9;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.spi.CalendarNameProvider;

import static java.util.Calendar.AM_PM;
import static java.util.Calendar.DAY_OF_WEEK;
import static java.util.Calendar.ERA;
import static java.util.Calendar.LONG_FORMAT;
import static java.util.Calendar.LONG_STANDALONE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.NARROW_FORMAT;
import static java.util.Calendar.NARROW_STANDALONE;
import static java.util.Calendar.SHORT_FORMAT;
import static java.util.Calendar.SHORT_STANDALONE;

/**
 * Class containing the english script differences from CLDR that were in the old COMPAT locale set.
 * These have been copied here to ensure date format compatibility for all v8 Elasticsearch releases.
 * <p>
 * The source data can be obtained by iterating through {@link Locale#getAvailableLocales()}
 * for both {@code -Djava.locale.providers=CLDR} and {@code -Djava.locale.providers=COMPAT},
 * obtaining {@link java.util.Calendar#getInstance} for each locale,
 * and calling {@code java.util.Calendar#getDisplayNames} for all valid values of {@code field} and {@code style}.
 * This data can then be written to disk in an appropriate textual format, and compared with a diff tool.
 * <p>
 * Locales with a country extension will just have the same english-base info as {@link Locale#ROOT}, and so can be ignored.
 * We're only concerned about the top-level language differences here.
 */
@UpdateForV9
public class CompatCalendarNameProvider extends CalendarNameProvider {

    private static void addLocaleData(
        Map<Locale, Map<Integer, Map<Integer, List<String>>>> map,
        Locale locale,
        int field,
        int style,
        List<String> values
    ) {
        addLocaleData(map.computeIfAbsent(locale, k -> new HashMap<>()), field, style, values);
    }

    private static void addLocaleData(Map<Integer, Map<Integer, List<String>>> map, int field, int style, List<String> values) {
        final int STANDALONE_MASK = 0x8000;  // value of Calendar.STANDALONE_MASK
        Map<Integer, List<String>> fieldMap = map.computeIfAbsent(field, k -> new HashMap<>());

        if ((style & STANDALONE_MASK) == 0) {
            // not standalone, add for both
            if (fieldMap.putIfAbsent(style, values) != null) {
                throw new IllegalArgumentException(Strings.format("Duplicate values for %s %s", field, style));
            }
            if (fieldMap.putIfAbsent(style | STANDALONE_MASK, values) != null) {   // 0x8000 is Calendar.STANDALONE_MASK
                throw new IllegalArgumentException(Strings.format("Duplicate values for %s %s", field, style));
            }
        } else {
            // standalone, assume standard has already been added
            fieldMap.put(style, values);
        }
    }

    // the locales that use the same data as ROOT
    // not included - ar be bg el
    // TODO: ca cs da es et

    // Maps are locale -> field -> style -> values
    private static final Map<Locale, Map<Integer, Map<Integer, List<String>>>> LOCALE_DATA = new HashMap<>();
    static {
        {
            Map<Integer, Map<Integer, List<String>>> rootData = new HashMap<>();

            addLocaleData(rootData, ERA, LONG_FORMAT, List.of("BC", "AD"));
            addLocaleData(rootData, ERA, SHORT_FORMAT, List.of("BC", "AD"));
            addLocaleData(rootData, ERA, NARROW_FORMAT, List.of("B", "A"));
            addLocaleData(
                rootData,
                MONTH,
                LONG_FORMAT,
                List.of(
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
                )
            );
            addLocaleData(
                rootData,
                DAY_OF_WEEK,
                LONG_FORMAT,
                Arrays.asList(
                    null, // Calendar.SUNDAY is 1, not 0
                    "Sunday",
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday"
                )
            );
            addLocaleData(rootData, AM_PM, NARROW_FORMAT, List.of("a", "p"));

            LOCALE_DATA.put(Locale.ROOT, rootData);
        }
        {
            Map<Integer, Map<Integer, List<String>>> caData = new HashMap<>();
            addLocaleData(caData, ERA, LONG_FORMAT, List.of("BC", "AD"));
            addLocaleData(caData, ERA, NARROW_FORMAT, List.of("B", "A"));
            addLocaleData(
                caData,
                MONTH,
                SHORT_STANDALONE,
                List.of("gen.", "feb.", "març", "abr.", "maig", "juny", "jul.", "ag.", "set.", "oct.", "nov.", "des.")
            );
            addLocaleData(
                caData,
                DAY_OF_WEEK,
                LONG_STANDALONE,
                Arrays.asList(null, "Diumenge", "Dilluns", "Dimarts", "Dimecres", "Dijous", "Divendres", "Dissabte")
            );
            addLocaleData(caData, DAY_OF_WEEK, SHORT_STANDALONE, Arrays.asList(null, "dg", "dl", "dt", "dc", "dj", "dv", "ds"));
            addLocaleData(caData, DAY_OF_WEEK, NARROW_FORMAT, Arrays.asList(null, "G", "L", "T", "C", "J", "V", "S"));
            addLocaleData(caData, DAY_OF_WEEK, NARROW_STANDALONE, Arrays.asList(null, "g", "l", "t", "c", "j", "v", "s"));
            addLocaleData(caData, AM_PM, LONG_FORMAT, List.of("AM", "PM"));
            addLocaleData(caData, AM_PM, SHORT_FORMAT, List.of("AM", "PM"));
            addLocaleData(caData, AM_PM, NARROW_FORMAT, List.of("a", "p"));
            LOCALE_DATA.put(new Locale("ca"), caData);
        }

        addLocaleData(LOCALE_DATA, Locale.ENGLISH, ERA, LONG_STANDALONE, List.of("BC", "AD"));

        {
            Map<Integer, Map<Integer, List<String>>> deData = new HashMap<>();
            addLocaleData(deData, ERA, NARROW_FORMAT, List.of("B", "A"));
            addLocaleData(
                deData,
                MONTH,
                SHORT_FORMAT,
                List.of("Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez")
            );
            addLocaleData(deData, DAY_OF_WEEK, SHORT_FORMAT, Arrays.asList(null, "So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"));
            addLocaleData(deData, AM_PM, NARROW_FORMAT, List.of("a", "p"));
            LOCALE_DATA.put(Locale.GERMAN, deData);
        }
    }

    @Override
    public String getDisplayName(String calendarType, int field, int value, int style, Locale locale) {
        if (calendarType.equals("gregory")) {
            List<String> values = LOCALE_DATA.getOrDefault(locale, Map.of()).getOrDefault(field, Map.of()).getOrDefault(style, List.of());
            if (value < values.size()) {
                return values.get(value);
            }
        }

        return null;
    }

    @Override
    public Map<String, Integer> getDisplayNames(String calendarType, int field, int style, Locale locale) {
        if (calendarType.equals("gregory")) {
            List<String> values = LOCALE_DATA.getOrDefault(locale, Map.of()).getOrDefault(field, Map.of()).get(style);
            if (values != null) {
                return toMap(values);
            }
        }

        return null;
    }

    private static Map<String, Integer> toMap(List<String> values) {
        Map<String, Integer> map = Maps.newHashMapWithExpectedSize(values.size());
        for (int i = 0; i < values.size(); i++) {
            String v = values.get(i);
            if (v != null) {
                map.put(v, i);
            }
        }
        return map;
    }

    @Override
    public Locale[] getAvailableLocales() {
        return LOCALE_DATA.keySet().toArray(Locale[]::new);
    }
}

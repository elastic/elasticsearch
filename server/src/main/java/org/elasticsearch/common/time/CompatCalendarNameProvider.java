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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.spi.CalendarNameProvider;

/**
 * Class containing the differences ONLY from CLDR that were in the old COMPAT locale set.
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

    /*
     * Only enable if the providers are SPI,CLDR (so the SPI here overrides CLDR)
     */
    private static final boolean ENABLE_SHIM = System.getProperty("java.locale.providers", "SPI,CLDR").equals("SPI,CLDR");

    private static void addLocaleData(Map<Integer, Map<Integer, List<String>>> map, int field, int style, List<String> values) {
        addLocaleData(map, field, style, values, true);
    }

    private static void addLocaleData(
        Map<Integer, Map<Integer, List<String>>> map,
        int field,
        int style,
        List<String> values,
        boolean addStandalone
    ) {
        if (field == Calendar.DAY_OF_WEEK) {
            // Calendar.SUNDAY is 1, not 0, so adjust accordingly
            List<String> dayValues = new ArrayList<>(8);
            dayValues.add(null);
            dayValues.addAll(values);
            values = dayValues;
        }

        final int STANDALONE_MASK = 0x8000;  // value of Calendar.STANDALONE_MASK
        Map<Integer, List<String>> fieldMap = map.computeIfAbsent(field, k -> new HashMap<>());

        if ((style & STANDALONE_MASK) == 0) {
            // not standalone, add for both
            if (fieldMap.putIfAbsent(style, values) != null) {
                throw new IllegalArgumentException(Strings.format("Duplicate values for %s %s", field, style));
            }
            if (addStandalone && fieldMap.putIfAbsent(style | STANDALONE_MASK, values) != null) {
                throw new IllegalArgumentException(Strings.format("Duplicate values for %s %s", field, style));
            }
        } else {
            // standalone, standard may have already been added alongside
            fieldMap.put(style, values);
        }
    }

    private static class MemoizedSupplier<T> implements Supplier<T> {
        private final Supplier<T> supplier;
        private volatile T value;

        private MemoizedSupplier(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        @Override
        public T get() {
            if (value == null) {
                synchronized (this) {
                    if (value == null) {
                        value = Objects.requireNonNull(supplier.get());
                    }
                }
            }
            return value;
        }
    }

    private static <T> Supplier<T> memoized(Supplier<T> supplier) {
        return new MemoizedSupplier<>(supplier);
    }

    // Maps are locale -> field -> style -> values
    private static final Map<Locale, Supplier<Map<Integer, Map<Integer, List<String>>>>> LOCALE_DATA = new HashMap<>();
    static {
        LOCALE_DATA.put(Locale.ROOT, memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> rootData = new HashMap<>();
            addLocaleData(rootData, 0, 2, List.of("BC", "AD"));
            addLocaleData(rootData, 0, 1, List.of("BC", "AD"));
            addLocaleData(rootData, 0, 4, List.of("B", "A"));
            addLocaleData(
                rootData,
                2,
                2,
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
            addLocaleData(rootData, 7, 2, List.of("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"));
            addLocaleData(rootData, 9, 4, List.of("a", "p"));
            return rootData;
        }));
    }

    @Override
    public String getDisplayName(String calendarType, int field, int value, int style, Locale locale) {
        if (ENABLE_SHIM && calendarType.equals("gregory")) {
            List<String> values = LOCALE_DATA.getOrDefault(locale, Map::of)
                .get()
                .getOrDefault(field, Map.of())
                .getOrDefault(style, List.of());
            if (value < values.size()) {
                return values.get(value);
            }
        }

        return null;
    }

    @Override
    public Map<String, Integer> getDisplayNames(String calendarType, int field, int style, Locale locale) {
        if (ENABLE_SHIM && calendarType.equals("gregory")) {
            List<String> values = LOCALE_DATA.getOrDefault(locale, Map::of).get().getOrDefault(field, Map.of()).get(style);
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
        return ENABLE_SHIM ? LOCALE_DATA.keySet().toArray(Locale[]::new) : new Locale[0];
    }
}

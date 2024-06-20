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
        LOCALE_DATA.put(new Locale("ar"), memoized(() -> {
            // ar is RTL, escape it to make sure its correctly encoded
            Map<Integer, Map<Integer, List<String>>> arData = new HashMap<>();
            addLocaleData(arData, 0, 32770, List.of("\u0642\u002E\u0645", "\u0645"));
            addLocaleData(arData, 0, 4, List.of("B", "A"));
            addLocaleData(
                arData,
                2,
                1,
                List.of(
                    "\u064A\u0646\u0627",
                    "\u0641\u0628\u0631",
                    "\u0645\u0627\u0631",
                    "\u0623\u0628\u0631",
                    "\u0645\u0627\u064A",
                    "\u064A\u0648\u0646",
                    "\u064A\u0648\u0644",
                    "\u0623\u063A\u0633",
                    "\u0633\u0628\u062A",
                    "\u0623\u0643\u062A",
                    "\u0646\u0648\u0641",
                    "\u062F\u064A\u0633"
                )
            );
            addLocaleData(arData, 7, 1, List.of("\u062D", "\u0646", "\u062B", "\u0631", "\u062E", "\u062C", "\u0633"), false);
            addLocaleData(arData, 9, 4, List.of("a", "p"));
            return arData;
        }));
        LOCALE_DATA.put(new Locale("be"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> beData = new HashMap<>();
            addLocaleData(beData, 0, 2, List.of("да н.е.", "н.е."));
            addLocaleData(beData, 0, 4, List.of("B", "A"));
            addLocaleData(
                beData,
                2,
                2,
                List.of(
                    "студзеня",
                    "лютага",
                    "сакавіка",
                    "красавіка",
                    "мая",
                    "чрвеня",
                    "ліпеня",
                    "жніўня",
                    "верасня",
                    "кастрычніка",
                    "лістапада",
                    "снежня"
                )
            );
            addLocaleData(beData, 2, 1, List.of("стд", "лют", "скв", "крс", "май", "чрв", "лпн", "жнв", "врс", "кст", "ліс", "снж"));
            addLocaleData(beData, 2, 4, List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"));
            addLocaleData(beData, 7, 1, List.of("нд", "пн", "ат", "ср", "чц", "пт", "сб"));
            addLocaleData(beData, 9, 4, List.of("a", "p"));
            return beData;
        }));
        LOCALE_DATA.put(new Locale("bg"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> bgData = new HashMap<>();
            addLocaleData(bgData, 0, 2, List.of("пр.н.е.", "н.е."));
            addLocaleData(bgData, 0, 1, List.of("пр. н. е.", "от н. е."));
            addLocaleData(bgData, 0, 4, List.of("B", "A"));
            addLocaleData(
                bgData,
                2,
                2,
                List.of(
                    "Януари",
                    "Февруари",
                    "Март",
                    "Април",
                    "Май",
                    "Юни",
                    "Юли",
                    "Август",
                    "Септември",
                    "Октомври",
                    "Ноември",
                    "Декември"
                )
            );
            addLocaleData(bgData, 2, 1, List.of("I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"));
            addLocaleData(bgData, 7, 2, List.of("Неделя", "Понеделник", "Вторник", "Сряда", "Четвъртък", "Петък", "Събота"));
            addLocaleData(bgData, 7, 1, List.of("Нд", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"));
            addLocaleData(bgData, 9, 2, List.of("AM", "PM"));
            addLocaleData(bgData, 9, 1, List.of("AM", "PM"));
            addLocaleData(bgData, 9, 4, List.of("a", "p"));
            return bgData;
        }));
        LOCALE_DATA.put(new Locale("ca"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> caData = new HashMap<>();
            addLocaleData(caData, 0, 2, List.of("BC", "AD"));
            addLocaleData(caData, 0, 4, List.of("B", "A"));
            addLocaleData(
                caData,
                2,
                32769,
                List.of("gen.", "feb.", "març", "abr.", "maig", "juny", "jul.", "ag.", "set.", "oct.", "nov.", "des.")
            );
            addLocaleData(caData, 7, 32770, List.of("Diumenge", "Dilluns", "Dimarts", "Dimecres", "Dijous", "Divendres", "Dissabte"));
            addLocaleData(caData, 7, 32769, List.of("dg", "dl", "dt", "dc", "dj", "dv", "ds"));
            addLocaleData(caData, 7, 4, List.of("G", "L", "T", "C", "J", "V", "S"));
            addLocaleData(caData, 7, 32772, List.of("g", "l", "t", "c", "j", "v", "s"));
            addLocaleData(caData, 9, 2, List.of("AM", "PM"));
            addLocaleData(caData, 9, 1, List.of("AM", "PM"));
            addLocaleData(caData, 9, 4, List.of("a", "p"));
            return caData;
        }));
        LOCALE_DATA.put(new Locale("cs"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> csData = new HashMap<>();
            addLocaleData(csData, 0, 2, List.of("př.Kr.", "po Kr."));
            addLocaleData(csData, 0, 4, List.of("př.n.l.", "n. l."));
            addLocaleData(csData, 2, 1, List.of("Led", "Úno", "Bře", "Dub", "Kvě", "Čer", "Čvc", "Srp", "Zář", "Říj", "Lis", "Pro"));
            addLocaleData(csData, 2, 32769, List.of("I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"));
            addLocaleData(csData, 7, 2, List.of("Neděle", "Pondělí", "Úterý", "Středa", "Čtvrtek", "Pátek", "Sobota"), false);
            addLocaleData(csData, 7, 1, List.of("Ne", "Po", "Út", "St", "Čt", "Pá", "So"), false);
            addLocaleData(csData, 9, 4, List.of("a", "p"));
            return csData;
        }));
        LOCALE_DATA.put(new Locale("da"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> daData = new HashMap<>();
            addLocaleData(daData, 0, 4, List.of("B", "A"));
            addLocaleData(daData, 2, 32769, List.of("jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec"));
            addLocaleData(daData, 7, 1, List.of("sø", "ma", "ti", "on", "to", "fr", "lø"));
            return daData;
        }));
        LOCALE_DATA.put(new Locale("de"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> deData = new HashMap<>();
            addLocaleData(deData, 0, 4, List.of("B", "A"));
            addLocaleData(deData, 2, 1, List.of("Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"));
            addLocaleData(deData, 7, 1, List.of("So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"));
            addLocaleData(deData, 9, 4, List.of("a", "p"));
            return deData;
        }));
        LOCALE_DATA.put(new Locale("el"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> elData = new HashMap<>();
            addLocaleData(elData, 0, 2, List.of("BC", "AD"));
            addLocaleData(elData, 0, 4, List.of("B", "A"));
            addLocaleData(
                elData,
                2,
                32770,
                List.of(
                    "Ιανουάριος",
                    "Φεβρουάριος",
                    "Μάρτιος",
                    "Απρίλιος",
                    "Μάϊος",
                    "Ιούνιος",
                    "Ιούλιος",
                    "Αύγουστος",
                    "Σεπτέμβριος",
                    "Οκτώβριος",
                    "Νοέμβριος",
                    "Δεκέμβριος"
                )
            );
            addLocaleData(elData, 2, 1, List.of("Ιαν", "Φεβ", "Μαρ", "Απρ", "Μαϊ", "Ιουν", "Ιουλ", "Αυγ", "Σεπ", "Οκτ", "Νοε", "Δεκ"));
            addLocaleData(elData, 7, 1, List.of("Κυρ", "Δευ", "Τρι", "Τετ", "Πεμ", "Παρ", "Σαβ"));
            addLocaleData(elData, 9, 2, List.of("πμ", "μμ"));
            addLocaleData(elData, 9, 4, List.of("a", "p"));
            return elData;
        }));
        LOCALE_DATA.put(new Locale("en"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> enData = new HashMap<>();
            addLocaleData(enData, 0, 32770, List.of("BC", "AD"));
            return enData;
        }));
        LOCALE_DATA.put(new Locale("es"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> esData = new HashMap<>();
            addLocaleData(esData, 0, 2, List.of("antes de Cristo", "anno Dómini"));
            addLocaleData(esData, 0, 1, List.of("a.C.", "d.C."));
            addLocaleData(esData, 0, 4, List.of("B", "A"));
            addLocaleData(esData, 2, 1, List.of("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"));
            addLocaleData(esData, 9, 2, List.of("AM", "PM"));
            addLocaleData(esData, 9, 1, List.of("AM", "PM"));
            addLocaleData(esData, 9, 4, List.of("a", "p"));
            return esData;
        }));
        LOCALE_DATA.put(new Locale("et"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> etData = new HashMap<>();
            addLocaleData(etData, 0, 2, List.of("e.m.a.", "m.a.j."));
            addLocaleData(etData, 0, 1, List.of("e.m.a.", "m.a.j."));
            addLocaleData(etData, 0, 4, List.of("B", "A"));
            addLocaleData(etData, 7, 2, List.of("pühapäev", "esmaspäev", "teisipäev", "kolmapäev", "neljapäev", "reede", "laupäev"));
            addLocaleData(etData, 9, 4, List.of("a", "p"));
            return etData;
        }));
        LOCALE_DATA.put(new Locale("fi"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> fiData = new HashMap<>();
            addLocaleData(fiData, 0, 2, List.of("eKr.", "jKr."));
            addLocaleData(fiData, 0, 1, List.of("BC", "AD"));
            addLocaleData(fiData, 0, 4, List.of("eK", "jK"));
            addLocaleData(
                fiData,
                2,
                1,
                List.of(
                    "tammikuuta",
                    "helmikuuta",
                    "maaliskuuta",
                    "huhtikuuta",
                    "toukokuuta",
                    "kesäkuuta",
                    "heinäkuuta",
                    "elokuuta",
                    "syyskuuta",
                    "lokakuuta",
                    "marraskuuta",
                    "joulukuuta"
                ),
                false
            );
            addLocaleData(fiData, 7, 2, List.of("sunnuntai", "maanantai", "tiistai", "keskiviikko", "torstai", "perjantai", "lauantai"));
            return fiData;
        }));

        LOCALE_DATA.put(new Locale("fr"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> frData = new HashMap<>();
            addLocaleData(frData, 0, 2, List.of("BC", "ap. J.-C."));
            addLocaleData(frData, 0, 4, List.of("B", "A"));
            addLocaleData(frData, 9, 4, List.of("a", "p"));
            return frData;
        }));
        LOCALE_DATA.put(new Locale("ga"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> gaData = new HashMap<>();
            addLocaleData(gaData, 0, 32770, List.of("RC", "AD"));
            addLocaleData(gaData, 0, 4, List.of("B", "A"));
            addLocaleData(gaData, 9, 2, List.of("a.m.", "p.m."));
            addLocaleData(gaData, 9, 1, List.of("a.m.", "p.m."));
            addLocaleData(gaData, 9, 4, List.of("a", "p"));
            return gaData;
        }));
        LOCALE_DATA.put(new Locale("he"), memoized(() -> {
            // he is RTL, escape it to make sure its correctly encoded
            Map<Integer, Map<Integer, List<String>>> heData = new HashMap<>();
            // the embedded " come straight from the locale output
            addLocaleData(heData, 0, 2, List.of("\u05DC\u05E1\u05D4\"\u05E0", "\u05DC\u05E4\u05E1\u05D4\"\u05E0"));
            addLocaleData(heData, 0, 1, List.of("\u05DC\u05E4\u05E0\u05D4\u05F4\u05E1", "\u05DC\u05E1\u05D4\u05F4\u05E0"));
            addLocaleData(heData, 0, 4, List.of("B", "A"));
            addLocaleData(
                heData,
                2,
                1,
                List.of(
                    "\u05D9\u05E0\u05D5",
                    "\u05E4\u05D1\u05E8",
                    "\u05DE\u05E8\u05E5",
                    "\u05D0\u05E4\u05E8",
                    "\u05DE\u05D0\u05D9",
                    "\u05D9\u05D5\u05E0",
                    "\u05D9\u05D5\u05DC",
                    "\u05D0\u05D5\u05D2",
                    "\u05E1\u05E4\u05D8",
                    "\u05D0\u05D5\u05E7",
                    "\u05E0\u05D5\u05D1",
                    "\u05D3\u05E6\u05DE"
                )
            );
            addLocaleData(
                heData,
                2,
                32769,
                List.of(
                    "\u05D9\u05E0\u05D5\u05F3",
                    "\u05E4\u05D1\u05E8\u05F3",
                    "\u05DE\u05E8\u05E5",
                    "\u05D0\u05E4\u05E8\u05F3",
                    "\u05DE\u05D0\u05D9",
                    "\u05D9\u05D5\u05E0\u05F3",
                    "\u05D9\u05D5\u05DC\u05F3",
                    "\u05D0\u05D5\u05D2\u05F3",
                    "\u05E1\u05E4\u05D8\u05F3",
                    "\u05D0\u05D5\u05E7\u05F3",
                    "\u05E0\u05D5\u05D1\u05F3",
                    "\u05D3\u05E6\u05DE\u05F3"
                )
            );
            addLocaleData(
                heData,
                7,
                2,
                List.of(
                    "\u05D9\u05D5\u05DD\u0020\u05E8\u05D0\u05E9\u05D5\u05DF",
                    "\u05D9\u05D5\u05DD\u0020\u05E9\u05E0\u05D9",
                    "\u05D9\u05D5\u05DD\u0020\u05E9\u05DC\u05D9\u05E9\u05D9",
                    "\u05D9\u05D5\u05DD\u0020\u05E8\u05D1\u05D9\u05E2\u05D9",
                    "\u05D9\u05D5\u05DD\u0020\u05D7\u05DE\u05D9\u05E9\u05D9",
                    "\u05D9\u05D5\u05DD\u0020\u05E9\u05D9\u05E9\u05D9",
                    "\u05E9\u05D1\u05EA"
                )
            );
            addLocaleData(heData, 7, 1, List.of("\u05D0", "\u05D1", "\u05D2", "\u05D3", "\u05D4", "\u05D5", "\u05E9"));
            addLocaleData(heData, 7, 4, List.of("\u05D0", "\u05D1", "\u05D2", "\u05D3", "\u05D4", "\u05D5", "\u05E9"));
            addLocaleData(heData, 9, 2, List.of("AM", "PM"));
            addLocaleData(heData, 9, 1, List.of("AM", "PM"));
            addLocaleData(heData, 9, 4, List.of("a", "p"));
            return heData;
        }));
        LOCALE_DATA.put(new Locale("hi"), memoized(() -> {
            // hi just uses the root locale data (even though its provided by COMPAT)
            Map<Integer, Map<Integer, List<String>>> hiData = new HashMap<>();
            addLocaleData(hiData, 0, 2, List.of("BC", "AD"));
            addLocaleData(hiData, 0, 1, List.of("BC", "AD"));
            addLocaleData(hiData, 0, 4, List.of("B", "A"));
            addLocaleData(
                hiData,
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
            addLocaleData(hiData, 2, 1, List.of("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"));
            addLocaleData(hiData, 2, 4, List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"));
            addLocaleData(hiData, 7, 2, List.of("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"));
            addLocaleData(hiData, 7, 1, List.of("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"));
            addLocaleData(hiData, 9, 2, List.of("AM", "PM"));
            addLocaleData(hiData, 9, 1, List.of("AM", "PM"));
            addLocaleData(hiData, 9, 4, List.of("a", "p"));
            return hiData;
        }));
        LOCALE_DATA.put(new Locale("hr"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> hrData = new HashMap<>();
            addLocaleData(hrData, 0, 2, List.of("Prije Krista", "Poslije Krista"));
            addLocaleData(hrData, 0, 1, List.of("p. n. e.", "A. D."));
            addLocaleData(hrData, 0, 4, List.of("B", "A"));
            addLocaleData(hrData, 2, 32769, List.of("sij", "vel", "ožu", "tra", "svi", "lip", "srp", "kol", "ruj", "lis", "stu", "pro"));
            addLocaleData(hrData, 9, 4, List.of("a", "p"));
            return hrData;
        }));
        LOCALE_DATA.put(new Locale("hu"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> huData = new HashMap<>();
            addLocaleData(huData, 0, 2, List.of("i.e.", "i.u."));
            addLocaleData(huData, 0, 4, List.of("B", "A"));
            addLocaleData(huData, 9, 2, List.of("DE", "DU"));
            addLocaleData(huData, 9, 1, List.of("DE", "DU"));
            addLocaleData(huData, 9, 4, List.of("a", "p"));
            return huData;
        }));
        LOCALE_DATA.put(new Locale("id"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> idData = new HashMap<>();
            addLocaleData(idData, 0, 2, List.of("BCE", "CE"));
            addLocaleData(idData, 0, 1, List.of("BC", "AD"));
            addLocaleData(idData, 0, 4, List.of("B", "A"));
            addLocaleData(idData, 2, 4, List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"));
            addLocaleData(idData, 9, 4, List.of("a", "p"));
            return idData;
        }));
        LOCALE_DATA.put(new Locale("is"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> isData = new HashMap<>();
            addLocaleData(isData, 0, 2, List.of("BC", "AD"));
            addLocaleData(isData, 0, 1, List.of("BC", "AD"));
            addLocaleData(isData, 0, 4, List.of("B", "A"));
            addLocaleData(isData, 9, 2, List.of("AM", "PM"));
            addLocaleData(isData, 9, 1, List.of("AM", "PM"));
            addLocaleData(isData, 9, 4, List.of("a", "p"));
            return isData;
        }));
        LOCALE_DATA.put(new Locale("it"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> itData = new HashMap<>();
            addLocaleData(itData, 0, 2, List.of("BC", "dopo Cristo"));
            addLocaleData(itData, 0, 1, List.of("aC", "dC"));
            addLocaleData(itData, 0, 4, List.of("B", "A"));
            addLocaleData(
                itData,
                2,
                32770,
                List.of(
                    "Gennaio",
                    "Febbraio",
                    "Marzo",
                    "Aprile",
                    "Maggio",
                    "Giugno",
                    "Luglio",
                    "Agosto",
                    "Settembre",
                    "Ottobre",
                    "Novembre",
                    "Dicembre"
                )
            );
            addLocaleData(itData, 7, 32770, List.of("Domenica", "Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato"));
            addLocaleData(itData, 9, 4, List.of("a", "p"));
            return itData;
        }));
        LOCALE_DATA.put(new Locale("ja"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> jaData = new HashMap<>();
            addLocaleData(jaData, 0, 4, List.of("B", "A"));
            addLocaleData(jaData, 2, 1, List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"));
            addLocaleData(jaData, 9, 4, List.of("a", "p"));
            return jaData;
        }));
        LOCALE_DATA.put(new Locale("ko"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> koData = new HashMap<>();
            addLocaleData(koData, 0, 2, List.of("\uAE30\uC6D0\uC804", "\uC11C\uAE30"));
            addLocaleData(koData, 0, 4, List.of("B", "A"));
            addLocaleData(koData, 9, 4, List.of("a", "p"));
            return koData;
        }));
        LOCALE_DATA.put(new Locale("lt"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> ltData = new HashMap<>();
            addLocaleData(ltData, 0, 2, List.of("pr.Kr.", "po.Kr."));
            addLocaleData(ltData, 0, 4, List.of("B", "A"));
            addLocaleData(
                ltData,
                2,
                2,
                List.of(
                    "sausio",
                    "vasaris",
                    "kovas",
                    "balandis",
                    "gegužė",
                    "birželis",
                    "liepa",
                    "rugpjūtis",
                    "rugsėjis",
                    "spalis",
                    "lapkritis",
                    "gruodis"
                )
            );
            addLocaleData(
                ltData,
                2,
                32770,
                List.of(
                    "Sausio",
                    "Vasario",
                    "Kovo",
                    "Balandžio",
                    "Gegužės",
                    "Birželio",
                    "Liepos",
                    "Rugpjūčio",
                    "Rugsėjo",
                    "Spalio",
                    "Lapkričio",
                    "Gruodžio"
                )
            );
            addLocaleData(ltData, 2, 1, List.of("Sau", "Vas", "Kov", "Bal", "Geg", "Bir", "Lie", "Rgp", "Rgs", "Spa", "Lap", "Grd"));
            addLocaleData(
                ltData,
                2,
                32769,
                List.of("Saus.", "Vas.", "Kov.", "Bal.", "Geg.", "Bir.", "Liep.", "Rugp.", "Rugs.", "Spal.", "Lapkr.", "Gruod.")
            );
            addLocaleData(
                ltData,
                7,
                2,
                List.of("Sekmadienis", "Pirmadienis", "Antradienis", "Trečiadienis", "Ketvirtadienis", "Penktadienis", "Šeštadienis"),
                false
            );
            addLocaleData(ltData, 7, 1, List.of("Sk", "Pr", "An", "Tr", "Kt", "Pn", "Št"));
            addLocaleData(ltData, 9, 2, List.of("AM", "PM"));
            addLocaleData(ltData, 9, 1, List.of("AM", "PM"));
            addLocaleData(ltData, 9, 4, List.of("a", "p"));
            return ltData;
        }));
        LOCALE_DATA.put(new Locale("lv"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> lvData = new HashMap<>();
            addLocaleData(lvData, 0, 2, List.of("pmē", "mē"));
            addLocaleData(lvData, 0, 1, List.of("BC", "AD"));
            addLocaleData(lvData, 0, 4, List.of("B", "A"));
            addLocaleData(lvData, 2, 32769, List.of("Jan", "Feb", "Mar", "Apr", "Maijs", "Jūn", "Jūl", "Aug", "Sep", "Okt", "Nov", "Dec"));
            addLocaleData(
                lvData,
                7,
                32770,
                List.of("svētdiena", "pirmdiena", "otrdiena", "trešdiena", "ceturtdiena", "piektdiena", "sestdiena")
            );
            addLocaleData(lvData, 7, 1, List.of("Sv", "P", "O", "T", "C", "Pk", "S"));
            addLocaleData(lvData, 9, 2, List.of("AM", "PM"));
            addLocaleData(lvData, 9, 1, List.of("AM", "PM"));
            addLocaleData(lvData, 9, 4, List.of("a", "p"));
            return lvData;
        }));
        LOCALE_DATA.put(new Locale("mk"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> mkData = new HashMap<>();
            addLocaleData(mkData, 0, 2, List.of("пр.н.е.", "ае."));
            addLocaleData(mkData, 0, 1, List.of("BC", "AD"));
            addLocaleData(mkData, 0, 4, List.of("B", "A"));
            addLocaleData(
                mkData,
                2,
                1,
                List.of("јан.", "фев.", "мар.", "апр.", "мај.", "јун.", "јул.", "авг.", "септ.", "окт.", "ноем.", "декем.")
            );
            addLocaleData(mkData, 7, 1, List.of("нед.", "пон.", "вт.", "сре.", "чет.", "пет.", "саб."));
            addLocaleData(mkData, 9, 2, List.of("AM", "PM"));
            addLocaleData(mkData, 9, 1, List.of("AM", "PM"));
            addLocaleData(mkData, 9, 4, List.of("a", "p"));
            return mkData;
        }));
        LOCALE_DATA.put(new Locale("ms"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> msData = new HashMap<>();
            addLocaleData(msData, 0, 2, List.of("BCE", "CE"));
            addLocaleData(msData, 0, 1, List.of("BC", "AD"));
            addLocaleData(msData, 0, 4, List.of("B", "A"));
            addLocaleData(msData, 2, 1, List.of("Jan", "Feb", "Mac", "Apr", "Mei", "Jun", "Jul", "Ogos", "Sep", "Okt", "Nov", "Dis"));
            addLocaleData(msData, 9, 2, List.of("AM", "PM"));
            addLocaleData(msData, 9, 1, List.of("AM", "PM"));
            return msData;
        }));
        LOCALE_DATA.put(new Locale("mt"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> mtData = new HashMap<>();
            addLocaleData(mtData, 0, 32770, List.of("QK", "WK"));

            addLocaleData(mtData, 0, 1, List.of("BC", "AD"));
            addLocaleData(mtData, 0, 4, List.of("B", "A"));
            addLocaleData(mtData, 9, 2, List.of("QN", "WN"));
            addLocaleData(mtData, 9, 1, List.of("QN", "WN"));
            return mtData;
        }));
        LOCALE_DATA.put(new Locale("nb"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> nbData = new HashMap<>();
            addLocaleData(nbData, 0, 2, List.of("BC", "AD"));
            addLocaleData(nbData, 0, 1, List.of("BC", "AD"));
            addLocaleData(nbData, 0, 4, List.of("B", "A"));
            addLocaleData(nbData, 2, 1, List.of("jan", "feb", "mar", "apr", "mai", "jun", "jul", "aug", "sep", "okt", "nov", "des"));
            addLocaleData(nbData, 7, 1, List.of("sø", "ma", "ti", "on", "to", "fr", "lø"));
            addLocaleData(nbData, 7, 32769, List.of("sø.", "ma.", "ti.", "on.", "to.", "fr.", "lø."));
            addLocaleData(nbData, 9, 2, List.of("AM", "PM"));
            addLocaleData(nbData, 9, 1, List.of("AM", "PM"));
            return nbData;
        }));
        LOCALE_DATA.put(new Locale("nl"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> nlData = new HashMap<>();
            addLocaleData(nlData, 0, 2, List.of("v. Chr.", "n. Chr."));
            addLocaleData(nlData, 0, 1, List.of("BC", "AD"));
            addLocaleData(nlData, 0, 4, List.of("B", "A"));
            addLocaleData(nlData, 2, 1, List.of("jan", "feb", "mrt", "apr", "mei", "jun", "jul", "aug", "sep", "okt", "nov", "dec"));
            addLocaleData(nlData, 9, 2, List.of("AM", "PM"));
            addLocaleData(nlData, 9, 1, List.of("AM", "PM"));
            addLocaleData(nlData, 9, 4, List.of("a", "p"));
            return nlData;
        }));
        LOCALE_DATA.put(new Locale("no"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> noData = new HashMap<>();
            addLocaleData(noData, 0, 2, List.of("BC", "AD"));
            addLocaleData(noData, 0, 1, List.of("BC", "AD"));
            addLocaleData(noData, 0, 4, List.of("B", "A"));
            addLocaleData(noData, 2, 1, List.of("jan", "feb", "mar", "apr", "mai", "jun", "jul", "aug", "sep", "okt", "nov", "des"));
            addLocaleData(noData, 7, 1, List.of("sø", "ma", "ti", "on", "to", "fr", "lø"));
            addLocaleData(noData, 7, 32769, List.of("sø.", "ma.", "ti.", "on.", "to.", "fr.", "lø."));
            addLocaleData(noData, 9, 2, List.of("AM", "PM"));
            addLocaleData(noData, 9, 1, List.of("AM", "PM"));
            return noData;
        }));
        LOCALE_DATA.put(new Locale("pl"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> plData = new HashMap<>();
            addLocaleData(plData, 0, 32770, List.of("p.n.e.", "n.e."));
            addLocaleData(plData, 0, 1, List.of("BC", "AD"));
            addLocaleData(plData, 0, 4, List.of("B", "A"));
            addLocaleData(plData, 7, 1, List.of("N", "Pn", "Wt", "Śr", "Cz", "Pt", "So"), false);
            return plData;
        }));
        LOCALE_DATA.put(new Locale("pt"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> ptData = new HashMap<>();
            addLocaleData(ptData, 0, 32770, List.of("a.C.", "d.C."));
            addLocaleData(ptData, 0, 1, List.of("BC", "AD"));
            addLocaleData(ptData, 0, 4, List.of("B", "A"));
            addLocaleData(
                ptData,
                2,
                2,
                List.of(
                    "Janeiro",
                    "Fevereiro",
                    "Março",
                    "Abril",
                    "Maio",
                    "Junho",
                    "Julho",
                    "Agosto",
                    "Setembro",
                    "Outubro",
                    "Novembro",
                    "Dezembro"
                )
            );
            addLocaleData(ptData, 2, 1, List.of("jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"));
            addLocaleData(
                ptData,
                7,
                2,
                List.of("Domingo", "Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado")
            );
            addLocaleData(ptData, 7, 1, List.of("Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"));
            addLocaleData(ptData, 9, 4, List.of("a", "p"));
            return ptData;
        }));
        LOCALE_DATA.put(new Locale("ro"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> roData = new HashMap<>();
            addLocaleData(roData, 0, 2, List.of("d.C.", "î.d.C."));
            addLocaleData(roData, 0, 1, List.of("BC", "AD"));
            addLocaleData(roData, 0, 4, List.of("B", "A"));
            addLocaleData(roData, 2, 1, List.of("Ian", "Feb", "Mar", "Apr", "Mai", "Iun", "Iul", "Aug", "Sep", "Oct", "Nov", "Dec"), false);
            addLocaleData(roData, 7, 2, List.of("duminică", "luni", "marţi", "miercuri", "joi", "vineri", "sâmbătă"));
            addLocaleData(roData, 7, 1, List.of("D", "L", "Ma", "Mi", "J", "V", "S"));
            addLocaleData(roData, 7, 32769, List.of("Du", "Lu", "Ma", "Mi", "Jo", "Vi", "Sâ"));
            addLocaleData(roData, 9, 2, List.of("AM", "PM"));
            addLocaleData(roData, 9, 1, List.of("AM", "PM"));
            addLocaleData(roData, 9, 4, List.of("a", "p"));
            return roData;
        }));
        LOCALE_DATA.put(new Locale("ru"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> ruData = new HashMap<>();
            addLocaleData(ruData, 0, 2, List.of("до н.э.", "н.э."));
            addLocaleData(ruData, 0, 1, List.of("BC", "AD"));
            addLocaleData(ruData, 0, 4, List.of("B", "A"));
            addLocaleData(
                ruData,
                2,
                32770,
                List.of("Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь")
            );
            addLocaleData(ruData, 2, 1, List.of("янв", "фев", "мар", "апр", "мая", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"));
            addLocaleData(
                ruData,
                2,
                32769,
                List.of("Янв.", "Февр.", "Март", "Апр.", "Май", "Июнь", "Июль", "Авг.", "Сент.", "Окт.", "Нояб.", "Дек.")
            );
            addLocaleData(ruData, 7, 32770, List.of("Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"));
            addLocaleData(ruData, 7, 1, List.of("Вс", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"));
            addLocaleData(ruData, 9, 4, List.of("a", "p"));
            return ruData;
        }));
        LOCALE_DATA.put(new Locale("sk"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> skData = new HashMap<>();
            addLocaleData(skData, 0, 2, List.of("pred n.l.", "n.l."));
            addLocaleData(skData, 0, 1, List.of("BC", "AD"));
            addLocaleData(skData, 0, 4, List.of("B", "A"));
            addLocaleData(skData, 7, 2, List.of("Nedeľa", "Pondelok", "Utorok", "Streda", "Štvrtok", "Piatok", "Sobota"), false);
            addLocaleData(skData, 7, 1, List.of("Ne", "Po", "Ut", "St", "Št", "Pi", "So"), false);
            addLocaleData(skData, 9, 4, List.of("a", "p"));
            return skData;
        }));
        LOCALE_DATA.put(new Locale("sl"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> slData = new HashMap<>();
            addLocaleData(slData, 0, 2, List.of("pr.n.š.", "po Kr."));
            addLocaleData(slData, 0, 1, List.of("BC", "AD"));
            addLocaleData(slData, 0, 4, List.of("B", "A"));
            addLocaleData(slData, 2, 32769, List.of("jan", "feb", "mar", "apr", "maj", "jun", "jul", "avg", "sep", "okt", "nov", "dec"));
            addLocaleData(slData, 7, 2, List.of("Nedelja", "Ponedeljek", "Torek", "Sreda", "Četrtek", "Petek", "Sobota"));
            addLocaleData(slData, 7, 1, List.of("Ned", "Pon", "Tor", "Sre", "Čet", "Pet", "Sob"));
            addLocaleData(slData, 7, 32769, List.of("ned", "pon", "tor", "sre", "čet", "pet", "sob"));
            addLocaleData(slData, 9, 2, List.of("AM", "PM"));
            addLocaleData(slData, 9, 1, List.of("AM", "PM"));
            addLocaleData(slData, 9, 4, List.of("a", "p"));
            return slData;
        }));
        LOCALE_DATA.put(new Locale("sq"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> sqData = new HashMap<>();
            addLocaleData(sqData, 0, 2, List.of("p.e.r.", "n.e.r."));
            addLocaleData(sqData, 0, 1, List.of("BC", "AD"));
            addLocaleData(sqData, 0, 4, List.of("B", "A"));
            addLocaleData(sqData, 2, 1, List.of("Jan", "Shk", "Mar", "Pri", "Maj", "Qer", "Kor", "Gsh", "Sht", "Tet", "Nën", "Dhj"));
            addLocaleData(sqData, 7, 32769, List.of("Die", "Hën", "Mar", "Mër", "Enj", "Pre", "Sht"));
            addLocaleData(sqData, 9, 2, List.of("PD", "MD"));
            addLocaleData(sqData, 9, 1, List.of("PD", "MD"));
            addLocaleData(sqData, 9, 4, List.of("a", "p"));
            return sqData;
        }));
        LOCALE_DATA.put(new Locale("sr"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> srData = new HashMap<>();
            addLocaleData(srData, 0, 2, List.of("п. н. е.", "н. е"));
            addLocaleData(srData, 9, 4, List.of("a", "p"));
            return srData;
        }));
        LOCALE_DATA.put(new Locale("sv"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> svData = new HashMap<>();
            addLocaleData(svData, 0, 2, List.of("före Kristus", "efter Kristus"));
            addLocaleData(svData, 2, 1, List.of("jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec"));
            addLocaleData(svData, 7, 1, List.of("sö", "må", "ti", "on", "to", "fr", "lö"));
            addLocaleData(svData, 7, 32769, List.of("sön", "mån", "tis", "ons", "tor", "fre", "lör"));
            addLocaleData(svData, 9, 4, List.of("f", "e"));
            return svData;
        }));
        LOCALE_DATA.put(new Locale("th"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> thData = new HashMap<>();
            addLocaleData(
                thData,
                0,
                2,
                List.of(
                    "\u0E1B\u0E35\u0E01\u0E48\u0E2D\u0E19\u0E04\u0E23\u0E34\u0E2A\u0E15\u0E4C\u0E01\u0E32\u0E25\u0E17\u0E35\u0E48",
                    "\u0E04\u002E\u0E28\u002E"
                )
            );
            addLocaleData(
                thData,
                2,
                4,
                List.of(
                    "\u0E21\u002E\u0E04\u002E",
                    "\u0E01\u002E\u0E1E\u002E",
                    "\u0E21\u0E35\u002E\u0E04\u002E",
                    "\u0E40\u0E21\u002E\u0E22\u002E",
                    "\u0E1E\u002E\u0E04\u002E",
                    "\u0E21\u0E34\u002E\u0E22",
                    "\u0E01\u002E\u0E04\u002E",
                    "\u0E2A\u002E\u0E04\u002E",
                    "\u0E01\u002E\u0E22\u002E",
                    "\u0E15\u002E\u0E04\u002E",
                    "\u0E1E\u002E\u0E22\u002E",
                    "\u0E18\u002E\u0E04\u002E"
                ),
                false
            );
            return thData;
        }));
        LOCALE_DATA.put(new Locale("tr"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> trData = new HashMap<>();
            addLocaleData(trData, 0, 32770, List.of("MÖ", "MS"));
            addLocaleData(trData, 0, 1, List.of("BC", "AD"));
            addLocaleData(trData, 0, 4, List.of("B", "A"));
            addLocaleData(trData, 9, 2, List.of("AM", "PM"));
            addLocaleData(trData, 9, 1, List.of("AM", "PM"));
            addLocaleData(trData, 9, 4, List.of("a", "p"));
            return trData;
        }));
        LOCALE_DATA.put(new Locale("uk"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> ukData = new HashMap<>();
            addLocaleData(ukData, 0, 2, List.of("до н.е.", "після н.е."));
            addLocaleData(ukData, 0, 1, List.of("BC", "AD"));
            addLocaleData(ukData, 0, 4, List.of("B", "A"));
            addLocaleData(
                ukData,
                2,
                32770,
                List.of(
                    "Січень",
                    "Лютий",
                    "Березень",
                    "Квітень",
                    "Травень",
                    "Червень",
                    "Липень",
                    "Серпень",
                    "Вересень",
                    "Жовтень",
                    "Листопад",
                    "Грудень"
                )
            );
            addLocaleData(
                ukData,
                2,
                32769,
                List.of("січ", "лют", "бер", "квіт", "трав", "черв", "лип", "серп", "вер", "жовт", "лист", "груд")
            );
            addLocaleData(ukData, 7, 2, List.of("неділя", "понеділок", "вівторок", "середа", "четвер", "п'ятниця", "субота"));
            addLocaleData(ukData, 9, 2, List.of("AM", "PM"));
            addLocaleData(ukData, 9, 1, List.of("AM", "PM"));
            addLocaleData(ukData, 9, 4, List.of("a", "p"));
            return ukData;
        }));
        LOCALE_DATA.put(new Locale("vi"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> viData = new HashMap<>();
            addLocaleData(viData, 0, 2, List.of("tr. CN", "sau CN"));
            addLocaleData(
                viData,
                2,
                2,
                List.of(
                    "tháng một",
                    "tháng hai",
                    "tháng ba",
                    "tháng tư",
                    "tháng năm",
                    "tháng sáu",
                    "tháng bảy",
                    "tháng tám",
                    "tháng chín",
                    "tháng mười",
                    "tháng mười một",
                    "tháng mười hai"
                )
            );
            addLocaleData(
                viData,
                2,
                32769,
                List.of("thg 1", "thg 2", "thg 3", "thg 4", "thg 5", "thg 6", "thg 7", "thg 8", "thg 9", "thg 10", "thg 11", "thg 12")
            );
            addLocaleData(viData, 7, 2, List.of("Chủ nhật", "Thứ hai", "Thứ ba", "Thứ tư", "Thứ năm", "Thứ sáu", "Thứ bảy"));
            addLocaleData(viData, 9, 4, List.of("a", "p"));
            return viData;
        }));
        LOCALE_DATA.put(new Locale("zh"), memoized(() -> {
            Map<Integer, Map<Integer, List<String>>> zhData = new HashMap<>();
            addLocaleData(zhData, 0, 4, List.of("B", "A"));
            addLocaleData(
                zhData,
                2,
                1,
                List.of(
                    "\u4E00\u6708",
                    "\u4E8C\u6708",
                    "\u4E09\u6708",
                    "\u56DB\u6708",
                    "\u4E94\u6708",
                    "\u516D\u6708",
                    "\u4E03\u6708",
                    "\u516B\u6708",
                    "\u4E5D\u6708",
                    "\u5341\u6708",
                    "\u5341\u4E00\u6708",
                    "\u5341\u4E8C\u6708"
                )
            );
            addLocaleData(
                zhData,
                2,
                32772,
                List.of(
                    "\u0031\u6708",
                    "\u0032\u6708",
                    "\u0033\u6708",
                    "\u0034\u6708",
                    "\u0035\u6708",
                    "\u0036\u6708",
                    "\u0037\u6708",
                    "\u0038\u6708",
                    "\u0039\u6708",
                    "\u0031\u0030\u6708",
                    "\u0031\u0031\u6708",
                    "\u0031\u0032\u6708"
                )
            );
            addLocaleData(
                zhData,
                7,
                1,
                List.of(
                    "\u661F\u671F\u65E5",
                    "\u661F\u671F\u4E00",
                    "\u661F\u671F\u4E8C",
                    "\u661F\u671F\u4E09",
                    "\u661F\u671F\u56DB",
                    "\u661F\u671F\u4E94",
                    "\u661F\u671F\u516D"
                ),
                false
            );
            addLocaleData(zhData, 9, 4, List.of("a", "p"));
            return zhData;
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

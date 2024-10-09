/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_DAY;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_DAY;

enum DateFormat {
    Iso8601 {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            return (date) -> {
                TemporalAccessor accessor = ISO_8601.parse(date);
                // even though locale could be set to en-us, Locale.ROOT (following iso8601 calendar data rules) should be used
                return DateFormatters.from(accessor, Locale.ROOT, timezone).withZoneSameInstant(timezone);
            };
        }
    },
    Unix {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            return date -> Instant.ofEpochMilli((long) (Double.parseDouble(date) * 1000.0)).atZone(timezone);
        }
    },
    UnixMs {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            return date -> Instant.ofEpochMilli(Long.parseLong(date)).atZone(timezone);
        }
    },
    Tai64n {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            return date -> Instant.ofEpochMilli(parseMillis(date)).atZone(timezone);
        }

        private static long parseMillis(String date) {
            if (date.startsWith("@")) {
                date = date.substring(1);
            }
            long base = Long.parseLong(date.substring(1, 16), 16);
            // 1356138046000
            long rest = Long.parseLong(date.substring(16, 24), 16);
            return ((base * 1000) - 10000) + (rest / 1000000);
        }
    },
    Java {
        private final List<ChronoField> FIELDS = Arrays.asList(
            NANO_OF_SECOND,
            SECOND_OF_DAY,
            MINUTE_OF_DAY,
            HOUR_OF_DAY,
            DAY_OF_MONTH,
            MONTH_OF_YEAR
        );

        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId zoneId, Locale locale) {
            // support the 6.x BWC compatible way of parsing java 8 dates
            if (format.startsWith("8")) {
                format = format.substring(1);
            }

            DateFormatter dateFormatter = DateFormatter.forPattern(format).withLocale(locale);

            final DateFormatter formatter = dateFormatter;
            return text -> {
                TemporalAccessor accessor = formatter.parse(text);
                // if there is no year nor year-of-era, we fall back to the current one and
                // fill the rest of the date up with the parsed date
                if (accessor.isSupported(ChronoField.YEAR) == false
                    && accessor.isSupported(ChronoField.YEAR_OF_ERA) == false
                    && accessor.isSupported(WeekFields.ISO.weekBasedYear()) == false
                    && accessor.isSupported(WeekFields.of(locale).weekBasedYear()) == false
                    && accessor.isSupported(ChronoField.INSTANT_SECONDS) == false) {
                    int year = LocalDate.now(ZoneOffset.UTC).getYear();
                    ZonedDateTime newTime = Instant.EPOCH.atZone(ZoneOffset.UTC).withYear(year);
                    for (ChronoField field : FIELDS) {
                        if (accessor.isSupported(field)) {
                            newTime = newTime.with(field, accessor.get(field));
                        }
                    }

                    accessor = newTime.withZoneSameLocal(zoneId);
                }

                return DateFormatters.from(accessor, locale, zoneId).withZoneSameInstant(zoneId);

            };
        }
    };

    /** It's important to keep this variable as a constant because {@link DateFormatter#forPattern(String)} is an expensive method and,
     * in this case, it's a never changing value.
     * <br>
     * Also, we shouldn't inline it in the {@link DateFormat#Iso8601}'s enum because it'd make useless the cache used
     * at {@link DateProcessor}).
     */
    private static final DateFormatter ISO_8601 = DateFormatter.forPattern("iso8601");

    abstract Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale);

    static DateFormat fromString(String format) {
        // note: the ALL_CAPS format names here (UNIX_MS, etc) are present for historical reasons:
        // they are the format literals that are supported by the logstash date filter plugin
        // (see https://www.elastic.co/guide/en/logstash/current/plugins-filters-date.html#plugins-filters-date-match).
        // don't extend this list with new special keywords (unless logstash has grown the same keyword).
        return switch (format) {
            case "ISO8601" -> Iso8601;
            case "UNIX" -> Unix;
            case "UNIX_MS" -> UnixMs;
            case "TAI64N" -> Tai64n;
            default -> Java;
        };
    }
}

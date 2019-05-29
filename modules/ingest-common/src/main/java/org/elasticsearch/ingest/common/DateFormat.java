/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
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
        Function<String, DateTime> getFunction(String format, DateTimeZone timezone, Locale locale) {
            return ISODateTimeFormat.dateTimeParser().withZone(timezone)::parseDateTime;
        }
    },
    Unix {
        @Override
        Function<String, DateTime> getFunction(String format, DateTimeZone timezone, Locale locale) {
            return (date) -> new DateTime((long)(Double.parseDouble(date) * 1000), timezone);
        }
    },
    UnixMs {
        @Override
        Function<String, DateTime> getFunction(String format, DateTimeZone timezone, Locale locale) {
            return (date) -> new DateTime(Long.parseLong(date), timezone);
        }
    },
    Tai64n {
        @Override
        Function<String, DateTime> getFunction(String format, DateTimeZone timezone, Locale locale) {
            return (date) -> new DateTime(parseMillis(date), timezone);
        }

        private long parseMillis(String date) {
            if (date.startsWith("@")) {
                date = date.substring(1);
            }
            long base = Long.parseLong(date.substring(1, 16), 16);
            // 1356138046000
            long rest = Long.parseLong(date.substring(16, 24), 16);
            return ((base * 1000) - 10000) + (rest/1000000);
        }
    },
    Java {
        private final List<ChronoField> FIELDS =
            Arrays.asList(NANO_OF_SECOND, SECOND_OF_DAY, MINUTE_OF_DAY, HOUR_OF_DAY, DAY_OF_MONTH, MONTH_OF_YEAR);

        @Override
        Function<String, DateTime> getFunction(String format, DateTimeZone timezone, Locale locale) {
            // in case you are wondering why we do not call 'DateFormatter.forPattern(format)' for all cases here, but only for the
            // non java time case:
            // When the joda date formatter parses a date then a year is always set, so that no fallback can be used, like
            // done in the JodaDateFormatter.withYear() code below
            // This means that we leave the existing parsing logic in place, but will fall back to the new java date parsing logic, if an
            // "8" is prepended to the date format string
            int year = LocalDate.now(ZoneOffset.UTC).getYear();
            if (format.startsWith("8")) {
                DateFormatter formatter = DateFormatter.forPattern(format)
                    .withLocale(locale)
                    .withZone(DateUtils.dateTimeZoneToZoneId(timezone));
                return text -> {
                    TemporalAccessor accessor = formatter.parse(text);
                    // if there is no year, we fall back to the current one and
                    // fill the rest of the date up with the parsed date
                    if (accessor.isSupported(ChronoField.YEAR) == false) {
                        ZonedDateTime newTime = Instant.EPOCH.atZone(ZoneOffset.UTC).withYear(year);
                        for (ChronoField field : FIELDS) {
                            if (accessor.isSupported(field)) {
                                newTime = newTime.with(field, accessor.get(field));
                            }
                        }

                        accessor = newTime.withZoneSameLocal(DateUtils.dateTimeZoneToZoneId(timezone));
                    }

                    long millis = DateFormatters.from(accessor).toInstant().toEpochMilli();
                    return new DateTime(millis, timezone);
                };
            } else {
                DateFormatter formatter = Joda.forPattern(format)
                    .withYear(year)
                    .withZone(DateUtils.dateTimeZoneToZoneId(timezone))
                    .withLocale(locale);
                return text -> new DateTime(formatter.parseMillis(text), timezone);
            }
        }
    };

    abstract Function<String, DateTime> getFunction(String format, DateTimeZone timezone, Locale locale);

    static DateFormat fromString(String format) {
        switch (format) {
            case "ISO8601":
                return Iso8601;
            case "UNIX":
                return Unix;
            case "UNIX_MS":
                return UnixMs;
            case "TAI64N":
                return Tai64n;
            default:
                return Java;
        }
    }
}

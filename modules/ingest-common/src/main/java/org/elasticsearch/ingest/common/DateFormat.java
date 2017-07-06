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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;

enum DateFormat {
    Iso8601 {
        @Override
        Function<String, DateTime> getFunction(Supplier<DateTime> currentTimeSupplier, String format,
                                               DateTimeZone timezone, Locale locale) {
            return ISODateTimeFormat.dateTimeParser().withZone(timezone)::parseDateTime;
        }
    },
    Unix {
        @Override
        Function<String, DateTime> getFunction(Supplier<DateTime> currentTimeSupplier, String format,
                                               DateTimeZone timezone, Locale locale) {
            return (date) -> new DateTime((long)(Double.parseDouble(date) * 1000), timezone);
        }
    },
    UnixMs {
        @Override
        Function<String, DateTime> getFunction(Supplier<DateTime> currentTimeSupplier, String format,
                                               DateTimeZone timezone, Locale locale) {
            return (date) -> new DateTime(Long.parseLong(date), timezone);
        }
    },
    Tai64n {
        @Override
        Function<String, DateTime> getFunction(Supplier<DateTime> currentTimeSupplier, String format,
                                               DateTimeZone timezone, Locale locale) {
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
    Joda {
        @Override
        Function<String, DateTime> getFunction(Supplier<DateTime> currentTimeSupplier, String format,
                                               DateTimeZone timezone, Locale locale) {
            DateTimeFormatter parser = DateTimeFormat.forPattern(format).withZone(timezone).withLocale(locale);
            boolean containsYear = format.contains("y") || format.contains("Y");
            boolean containsTimeZone = format.contains("z") || format.contains("Z");
            if (containsYear) {
                return text -> parser.withDefaultYear(currentTimeSupplier.get().getYear()).parseDateTime(text);
            } else {
                return text -> {
                    DateTime currentTime = currentTimeSupplier.get();
                    int currentMonth = currentTime.getMonthOfYear();
                    int eventYear = currentTime.getYear();
                    DateTime dateTime;
                    if (containsTimeZone) {
                        dateTime = parser.parseDateTime(text);
                    } else {
                        dateTime = parser.parseLocalDateTime(text).toDateTime(timezone);
                    }
                    int eventMonth = dateTime.getMonthOfYear();
                    if (eventMonth == 12 && currentMonth == 1) {
                        // Now is January, event is December. Assume it's from last year.
                        eventYear--;
                    } else if (eventMonth == 1 && currentMonth == 12) {
                        // Now is December, event is January. Assume it's from next year.
                        eventYear++;
                    }
                    return dateTime.withYear(eventYear);
                };
            }
        }
    };

    abstract Function<String, DateTime> getFunction(Supplier<DateTime> currentTimeSupplier, String format,
                                                    DateTimeZone timezone, Locale locale);

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
                return Joda;
        }
    }
}

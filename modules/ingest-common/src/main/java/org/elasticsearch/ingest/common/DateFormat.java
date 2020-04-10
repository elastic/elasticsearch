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

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
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
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            return (date) -> DateFormatters.from(DateFormatter.forPattern("iso8601").parse(date), timezone)
                                            .withZoneSameInstant(timezone);

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
        Function<String, ZonedDateTime> getFunction(String format, ZoneId zoneId, Locale locale) {
            // support the 6.x BWC compatible way of parsing java 8 dates
            if (format.startsWith("8")) {
                format = format.substring(1);
            }

            boolean isUtc = ZoneOffset.UTC.equals(zoneId);

            DateFormatter dateFormatter = DateFormatter.forPattern(format)
                .withLocale(locale);
            // if UTC zone is set here, the time zone specified in the format will be ignored, leading to wrong dates
            if (isUtc == false) {
                dateFormatter = dateFormatter.withZone(zoneId);
            }
            final DateFormatter formatter = dateFormatter;
            return text -> {
                TemporalAccessor accessor = formatter.parse(text);
                // if there is no year nor year-of-era, we fall back to the current one and
                // fill the rest of the date up with the parsed date
                if (accessor.isSupported(ChronoField.YEAR) == false && accessor.isSupported(ChronoField.YEAR_OF_ERA) == false ) {
                    int year = LocalDate.now(ZoneOffset.UTC).getYear();
                    ZonedDateTime newTime = Instant.EPOCH.atZone(ZoneOffset.UTC).withYear(year);
                    for (ChronoField field : FIELDS) {
                        if (accessor.isSupported(field)) {
                            newTime = newTime.with(field, accessor.get(field));
                        }
                    }

                    accessor = newTime.withZoneSameLocal(zoneId);
                }

                if (isUtc) {
                    return DateFormatters.from(accessor).withZoneSameInstant(ZoneOffset.UTC);
                } else {
                    return DateFormatters.from(accessor);
                }
            };
        }
    };

    abstract Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale);

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

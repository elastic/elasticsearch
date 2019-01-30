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
import org.elasticsearch.common.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.function.Function;

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
        @Override
        Function<String, DateTime> getFunction(String format, DateTimeZone timezone, Locale locale) {

            // support the 6.x BWC compatible way of parsing java 8 dates
            if (format.startsWith("8")) {
                format = format.substring(1);
            }

            int year = LocalDate.now(ZoneOffset.UTC).getYear();
            DateFormatter formatter = DateFormatter.forPattern(format)
                .withLocale(locale)
                .withZone(DateUtils.dateTimeZoneToZoneId(timezone));
            return text -> {
                ZonedDateTime defaultZonedDateTime = Instant.EPOCH.atZone(ZoneOffset.UTC).withYear(year);
                TemporalAccessor accessor = formatter.parse(text);
                long millis = DateFormatters.toZonedDateTime(accessor, defaultZonedDateTime).toInstant().toEpochMilli();
                return new DateTime(millis, timezone);
            };
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

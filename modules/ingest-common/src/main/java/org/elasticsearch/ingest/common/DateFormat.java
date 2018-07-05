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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.function.Function;

enum DateFormat {
    Iso8601 {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            DateFormatter formatter = DateFormatters.forPattern("date_time_no_millis");
            return (date) -> ZonedDateTime.from(formatter.parse(date)).withZoneSameInstant(timezone);
        }
    },
    Unix {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            return (date) -> Instant.ofEpochMilli(((Double) (Double.parseDouble(date) * 1000)).longValue()).atZone(timezone);
        }
    },
    UnixMs {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            return (date) -> Instant.ofEpochMilli(Long.parseLong(date)).atZone(timezone);
        }
    },
    Tai64n {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            return (date) -> Instant.ofEpochMilli(parseMillis(date)).atZone(timezone);
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
    Time {
        @Override
        Function<String, ZonedDateTime> getFunction(String format, ZoneId timezone, Locale locale) {
            DateFormatter formatter = DateFormatters.forPattern(format, locale).withZone(timezone);
            return text -> {
                TemporalAccessor accessor = formatter.parse(text);
                ZonedDateTime startOfThisYear = DateFormatters.EPOCH_ZONED_DATE_TIME.withYear(ZonedDateTime.now(timezone)
                    .get(ChronoField.YEAR));
                return DateFormatters.toZonedDateTime(accessor, startOfThisYear).withZoneSameLocal(timezone);
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
                return Time;
        }
    }
}

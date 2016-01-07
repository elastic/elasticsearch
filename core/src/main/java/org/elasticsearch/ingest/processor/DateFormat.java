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

package org.elasticsearch.ingest.processor;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

enum DateFormat {
    Iso8601 {
        @Override
        Function<String, DateTime> getFunction(DateTimeZone timezone) {
            return ISODateTimeFormat.dateTimeParser().withZone(timezone)::parseDateTime;
        }
    },
    Unix {
        @Override
        Function<String, DateTime> getFunction(DateTimeZone timezone) {
            return (date) -> new DateTime((long)(Float.parseFloat(date) * 1000), timezone);
        }
    },
    UnixMs {
        @Override
        Function<String, DateTime> getFunction(DateTimeZone timezone) {
            return (date) -> new DateTime(Long.parseLong(date), timezone);
        }

        @Override
        public String toString() {
            return "UNIX_MS";
        }
    },
    Tai64n {
        @Override
        Function<String, DateTime> getFunction(DateTimeZone timezone) {
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
    };

    abstract Function<String, DateTime> getFunction(DateTimeZone timezone);

    static Optional<DateFormat> fromString(String format) {
        switch (format) {
            case "ISO8601":
                return Optional.of(Iso8601);
            case "UNIX":
                return Optional.of(Unix);
            case "UNIX_MS":
                return Optional.of(UnixMs);
            case "TAI64N":
                return Optional.of(Tai64n);
            default:
                return Optional.empty();
        }
    }

    static Function<String, DateTime> getJodaFunction(String matchFormat, DateTimeZone timezone, Locale locale) {
        return DateTimeFormat.forPattern(matchFormat)
                .withDefaultYear((new DateTime(DateTimeZone.UTC)).getYear())
                .withZone(timezone).withLocale(locale)::parseDateTime;
    }

    @Override
    public String toString() {
        return name().toUpperCase(Locale.ROOT);
    }
}

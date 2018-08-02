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
package org.elasticsearch.common.time;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * wrapper class around java.time.DateTimeFormatter that supports multiple formats for easier parsing,
 * and one specific format for printing
 */
public class CompoundDateTimeFormatter {

    final DateTimeFormatter printer;
    final DateTimeFormatter[] parsers;

    CompoundDateTimeFormatter(DateTimeFormatter ... parsers) {
        if (parsers.length == 0) {
            throw new IllegalArgumentException("at least one date time formatter is required");
        }
        this.printer = parsers[0];
        this.parsers = parsers;
    }

    public TemporalAccessor parse(String input) {
        DateTimeParseException failure = null;
        for (int i = 0; i < parsers.length; i++) {
            try {
                return parsers[i].parse(input);
            } catch (DateTimeParseException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        // ensure that all parsers exceptions are returned instead of only the last one
        throw failure;
    }

    public CompoundDateTimeFormatter withZone(ZoneId zoneId) {
        final DateTimeFormatter[] parsersWithZone = new DateTimeFormatter[parsers.length];
        for (int i = 0; i < parsers.length; i++) {
            parsersWithZone[i] = parsers[i].withZone(zoneId);
        }

        return new CompoundDateTimeFormatter(parsersWithZone);
    }

    public String format(TemporalAccessor accessor) {
        return printer.format(accessor);
    }

    public static CompoundDateTimeFormatterBuilder builder() {
        return new CompoundDateTimeFormatterBuilder();
    }

    static final class CompoundDateTimeFormatterBuilder {

        private static final DateTimeFormatter TIME_ZONE_FORMATTER_ZONE_ID = new DateTimeFormatterBuilder()
            .appendZoneId()
            .toFormatter(Locale.ROOT);

        private static final DateTimeFormatter TIME_ZONE_FORMATTER_WITHOUT_COLON = new DateTimeFormatterBuilder()
            .appendOffset("+HHmm", "Z")
            .toFormatter(Locale.ROOT);

        private static final DateTimeFormatter TIME_ZONE_FORMATTER_WITH_COLON = new DateTimeFormatterBuilder()
            .appendOffset("+HH:mm", "Z")
            .toFormatter(Locale.ROOT);


        private DateTimeFormatter printer;
        private List<DateTimeFormatter> parsers = new ArrayList<>();

        CompoundDateTimeFormatterBuilder withPrinter(DateTimeFormatter printer) {
            this.printer = printer;
            return this;
        }

        CompoundDateTimeFormatterBuilder withPrinterAddTimeZone(DateTimeFormatter printer) {
            this.printer = new DateTimeFormatterBuilder().append(printer).append(TIME_ZONE_FORMATTER_ZONE_ID).toFormatter(Locale.ROOT);
            return this;
        }

        CompoundDateTimeFormatterBuilder withFormatterAddTimeZone(DateTimeFormatter formatter) {
            parsers.add(new DateTimeFormatterBuilder().append(formatter).append(TIME_ZONE_FORMATTER_WITH_COLON).toFormatter(Locale.ROOT));
            parsers.add(
                new DateTimeFormatterBuilder().append(formatter).append(TIME_ZONE_FORMATTER_WITHOUT_COLON).toFormatter(Locale.ROOT));
            parsers.add(new DateTimeFormatterBuilder().append(formatter).append(TIME_ZONE_FORMATTER_ZONE_ID).toFormatter(Locale.ROOT));
            return this;
        }

        CompoundDateTimeFormatterBuilder withFormatterAddOptionalTimeZone(DateTimeFormatter formatter) {
            parsers.add(new DateTimeFormatterBuilder().append(formatter)
                .optionalStart().append(TIME_ZONE_FORMATTER_WITH_COLON).optionalEnd()
                .toFormatter(Locale.ROOT));
            parsers.add(new DateTimeFormatterBuilder().append(formatter)
                .optionalStart().append(TIME_ZONE_FORMATTER_WITHOUT_COLON).optionalEnd()
                .toFormatter(Locale.ROOT));
            parsers.add(new DateTimeFormatterBuilder().append(formatter)
                .optionalStart().append(TIME_ZONE_FORMATTER_ZONE_ID).optionalEnd()
                .toFormatter(Locale.ROOT));
            return this;
        }

        CompoundDateTimeFormatter build() {
            if (printer != null) {
                this.parsers.add(0, printer);
            }
            this.parsers = this.parsers.stream()
                .map(parser -> parser.getZone() == null ? parser.withZone(ZoneOffset.UTC) : parser)
                .collect(Collectors.toList());
            return new CompoundDateTimeFormatter(parsers.toArray(new DateTimeFormatter[]{}));
        }
    }
}
